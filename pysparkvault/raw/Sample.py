from .RawVault import *

from pyspark.sql.types import BooleanType, StringType

spark: SparkSession = None # dummy value


#
# Configure
#
source_system_name = 'Allegro'
staging_base_path = f'abfss://raw@devpncdlsweurawdata.dfs.core.windows.net/Allegro/2022/02/08'
staging_prepared_base_path = f'abfss://raw@devpncdlsweurawdata.dfs.core.windows.net/staging/allegro/prepared'
raw_base_path = f'abfss://raw@devpncdlsweurawdata.dfs.core.windows.net/raw/allegro'
staging_load_date_column_name = 'load_date'

config = DataVaultConfiguration(source_system_name, staging_base_path, staging_prepared_base_path, raw_base_path, staging_load_date_column_name)
raw_vault = RawVault(spark, config)

#
# Initialize database
#
raw_vault.initialize_database()

#
# Create hub tables.
#
raw_vault.create_hub('HUB__CLAIM', [ColumnDefinition('ClaimNumber', StringType())])
raw_vault.create_hub('HUB__POLICY', [ColumnDefinition('PublicID', StringType())])

#
# Create link tables.
#
raw_vault.create_link('LNK__CLAIM__POLICY', ['CLAIM_HKEY', 'POLICY_HKEY'])

#
# Create satellite tables
#
raw_vault.create_satellite('SAT__CLAIM', [
    ColumnDefinition('AgencyId', StringType()), 
    ColumnDefinition('Alg_HasTrip', BooleanType()),
    ColumnDefinition('Alg_Recovery', BooleanType()),
    ColumnDefinition('CreateTime', TimestampType())
])

#
# Stage tables.
#
raw_vault.stage_table('cc_claim', 'cc_claim.parquet', ['ClaimNumber'])
raw_vault.stage_table('cc_policy', 'cc_policy.parquet', ['PublicId'])

#
# Load Hubs
#
raw_vault.load_hub_from_prepared_staging_table(
    'cc_claim', 'hub__claim', ['ClaimNumber'],
    [SatelliteDefinition('sat__claim', ['AgencyId', 'Alg_HasTrip', 'Alg_Recovery', 'CreateTime'])])

#
# Load Links
#
raw_vault.load_link_for_linked_source_tables_from_prepared_staging_tables('cc_claim', ForeignKey('PolicyID', ColumnReference('cc_policy', 'ID')), 'LNK__CLAIM__POLICY', 'claim_hkey', 'policy_hkey')

raw_vault.create_point_in_time_table_for_single_satellite('PIT_CLAIM', 'SAT_CLAIM')







#
# Current User
# This table is used for user flattening (Mapping Rule 5). The intermediate data frame is used to merge user information in all other entity tables. We assume there are usually no notable changes in the attributes, thus we just use the latest version of the attributes. 
#

df_user = business_vault.read_data_from_hub('USER', ['ID', 'PublicID'], True)
df_credential = business_vault.read_data_from_hub('CREDENTIAL', ['UserName'], True)
df_user = business_vault.join_linked_dataframes(df_user, df_credential, 'LNK__USER__CREDENTIAL', 'USER_HKEY', 'CREDENTIAL_HKEY', True)

df_user = df_user \
    .groupBy('ID') \
    .agg(F.max('$__LOAD_DATE').alias('LD')) \
    .alias('l') \
    .join(df_user.alias('r'), (F.col('l.ID') == F.col('r.ID')) & (F.col('l.LD') == F.col('r.`$__LOAD_DATE`'))) \
    .select('l.ID', 'r.PublicID', 'r.UserName')
    

display(df_user.limit(3))




def extend_with_user_info(df: DataFrame, column: Optional[str] = None) -> DataFrame:
    if column is None:
        user_id_columns = list(filter(lambda c: c.endswith('UserID'), df.columns))

        result = df
        for c in user_id_columns:
            result = extend_with_user_info(result, c)

        return result
    else:
        columns = df.columns
        column_index = columns.index(column)

        columns = columns[0:column_index] + [f'{column}', f'{column}_UserName'] + columns[column_index + 1:]

        return df \
            .join(
                df_user \
                    .withColumnRenamed('UserName', f'{column}_UserName') \
                    .withColumnRenamed('PublicID', f'{column}') \
                    .alias('user'), 
                df[column] == df_user['ID'],
                how='left') \
            .drop(df[column]) \
            .select(columns)


#
# User
#
user_table_name = f'{currated_database_name}.user'

df_user = business_vault.read_data_from_hub('USER', ['Alg_AcademicTitle', 'Alg_EndDate', 'Alg_FiliationCode', 'Alg_IndividualSignature', 'Alg_LastLogin', 'Alg_MigrationLOB', 'Alg_PrinterId', 'Alg_StartDate', 'CreateTime', 'Department', 'ExternalUser', 'isTechnical', 'JobTitle', 'ObfuscatedInternal', 'PublicID', 'SessionTimeoutSecs', 'SpatialPointDenorm', 'UpdateTime', 'Alg_SelectedLOB', 'Alg_VacationStatus', 'DefaultCountry', 'DefaultPhoneCountry', 'ExperienceLevel', 'Language', 'Locale', 'PolicyType', 'senderDefault', 'SystemUserType', 'VacationStatus', 'ValidationLevel', 'AuthorityProfileID', 'ContactID', 'CreateUserID', 'CredentialID', 'OrganizationID', 'UpdateUserID'], True)
df_credential = business_vault.read_data_from_hub('CREDENTIAL', ['UserName', 'Active'], True)

df_user = business_vault.join_linked_dataframes(df_user, df_credential, 'LNK__USER__CREDENTIAL', 'USER_HKEY', 'CREDENTIAL_HKEY', True)
df_user.write.mode('overwrite').saveAsTable(user_table_name)

spark.sql(f'REFRESH TABLE {user_table_name}')
df_user = spark.table(user_table_name)

display(df_user.limit(3))


#
# Account
#
df_account = business_vault.read_data_from_hub('ACCOUNT', ['AccountNumber', 'Alg_AccountHolder', 'CreateTime', 'PublicID', 'UpdateTime', 'AccountHolderID', 'CreateUserID', 'UpdateUserID'], True)
df_account = extend_with_user_info(df_account)


display(df_account.distinct())


#
# AccountManagement
#
accountmanagement_table_name = f'{currated_database_name}.accountmanagement'
df_accountmanagement = business_vault.read_data_from_hub('ACCOUNTMANAGEMENT', ['Comments', 'CreateTime', 'ECommunication', 'IsConfirmationLetter', 'IsSettlementLetter', 'PaymentAddress', 'PublicID', 'Responsibility', 'UpdateTime', 'AccountID', 'CreateUserID', 'UpdateUserID'], True)
df_accountmanagement = extend_with_user_info(df_accountmanagement)
#df_accountmanagement.write.mode('overwrite').saveAsTable(accountmanagement_table_name)
#spark.sql(f'REFRESH TABLE {accountmanagement_table_name}')
#df_accountmanagement = spark.table(accountmanagement_table_name)

display(df_accountmanagement.limit(3))


#
# AccountPolicy
#

accountpolicy_table_name = f'{currated_database_name}.accountpolicy'
df_accountpolicy = business_vault.read_data_from_hub('ACCOUNTPOLICY', ['Alg_ClaimCorrespondenceAddr', 'CollectiveDamagePolicy', 'CreateTime', 'Description', 'IsBulkInvoiceApplicable', 'PolicyNumber', 'ProductDescription', 'PublicID', 'UpdateTime', 'Alg_LossType', 'AccountID', 'CreateUserID', 'UpdateUserID'], True)
df_accountpolicy = extend_with_user_info(df_accountpolicy)
#df_accountpolicy.write.mode('overwrite').saveAsTable(accountpolicy_table_name)
#spark.sql(f'REFRESH TABLE {accountpolicy_table_name}')
#df_accountpolicy = spark.table(accountpolicy_table_name)

display(df_accountpolicy.limit(3))


#
# biincidentlossitem
#

biincidentlossitem_table_name = f'{currated_database_name}.biincidentlossitem'
df_biincidentlossitem = business_vault.read_data_from_hub('BIINCIDENTLOSSITEM',['BIPercent', 'Comment', 'CreateTime', 'DateFrom', 'DateTo', 'Days', 'FixCosts', 'IRGPAmount', 'PublicID', 'Revenue', 'UpdateTime', 'VariableCosts', 'FixCostsCurrency', 'IRGPCurrency', 'RevenueCurrency', 'Subtype', 'VariableCostsCurrency', 'BusinessInterruptionIncident', 'CreateUserID', 'UpdateUserID'], True)
df_biincidentlossitem = extend_with_user_info(df_biincidentlossitem)
#df_biincidentlossitem.write.mode('overwrite').saveAsTable(biincidentlossitem_table_name)
#spark.sql(f'REFRESH TABLE {biincidentlossitem_table_name}')
#df_biincidentlossitem = spark.table(biincidentlossitem_table_name)

display(df_biincidentlossitem.limit(3))


#
# BodilyInjuryPointPEL
#

bodilyinjurypointpel_table_name = f'{currated_database_name}.bodilyinjurypointpel'
df_bodilyinjurypointpel = business_vault.read_data_from_hub('BODILYINJURYPOINTPEL',['bodilyInjuryComment', 'PublicID', 'sunetFlag', 'bodilyInjuryTypeOfInjury', 'injuryPoint', 'injurySeverity', 'specificBodyParts', 'injuryIncidentID'], True)
df_bodilyinjurypointpel = extend_with_user_info(df_bodilyinjurypointpel)
#df_bodilyinjurypointpel.write.mode('overwrite').saveAsTable(bodilyinjurypointpel_table_name)
#spark.sql(f'REFRESH TABLE {bodilyinjurypointpel_table_name}')
#df_bodilyinjurypointpel = spark.table(bodilyinjurypointpel_table_name)

display(df_bodilyinjurypointpel.limit(3))


#
# Catastrophe
#

catastrophe_table_name = f'{currated_database_name}.catastrophe'
df_catastrophe = business_vault.read_data_from_hub('CATASTROPHE',['Active', 'Alg_IsMultipleCountry', 'CatastropheNumber', 'CatastropheValidFrom', 'CatastropheValidTo', 'CreateTime', 'Description', 'Name', 'PCSCatastropheNumber', 'PolicyEffectiveDate', 'PolicyRetrievalCompletionTime', 'PolicyRetrievalSetTime', 'PublicID', 'UpdateTime', 'Alg_CatastropheZoneType', 'Alg_Country', 'CreateUserID', 'UpdateUserID'], True)
df_catastrophe = extend_with_user_info(df_catastrophe)
#df_catastrophe.write.mode('overwrite').saveAsTable(catastrophe_table_name)
#spark.sql(f'REFRESH TABLE {catastrophe_table_name}')
#df_catastrophe = spark.table(catastrophe_table_name)

display(df_catastrophe.limit(3))


#
# Claim
#

claim_table_name = f'{currated_database_name}.claim'
df_claim_og = business_vault.read_data_from_hub('CLAIM',['Alg_AccidentCode', 'Alg_AccidentProtocol', 'Alg_AcuteInjuriesDescription', 'Alg_AcuteSpecificInjuries', 'Alg_AdditionalLossCause', 'Alg_AddresUnknown', 'Alg_AdjReserves', 'Alg_CauserDetail', 'Alg_CausingVehicleType', 'Alg_CededDate', 'Alg_ClaimReopenReason', 'Alg_ClaimsMadeDate', 'Alg_ClaimSubState', 'Alg_CloseClaim', 'Alg_CnpMode', 'Alg_CollectiveDamage', 'Alg_ConveyanceCode', 'Alg_CourtesyDate', 'Alg_CoverageInQuestion', 'Alg_CreatedBySystem', 'Alg_CurrStatus', 'Alg_DateOfConversation', 'Alg_DeductionReason', 'Alg_DentalCare', 'Alg_Diagnosis', 'Alg_DiagnosisDescription', 'Alg_DriverUnknown', 'Alg_EarlyIntervention', 'Alg_EarlyInterventionDate', 'Alg_Elucidation', 'Alg_EstimatedLossDate', 'Alg_ExpectedDateOfBirth', 'Alg_FirstNotificationConfirm', 'Alg_FirstPhoneCallNotNeeded', 'Alg_FlagClaimForSuva', 'Alg_Fraud', 'Alg_FreeMovementCase', 'Alg_GlassType', 'Alg_GNDeduction', 'Alg_HandlingFees', 'Alg_InsuredCharcteristics', 'Alg_InterclaimsGCode', 'Alg_InterClaimsType', 'Alg_InvolvedObjects', 'Alg_IPSAgeOfDriver', 'Alg_IPSBlameCode', 'Alg_IPSCauseofEvent', 'Alg_IPSClaimHandlingProtocol', 'Alg_IPSContractType', 'Alg_IPSDICDIL', 'Alg_IPSEmergingRisk', 'Alg_IPSEventType', 'Alg_IPSLawsuit', 'Alg_IPSLOB', 'Alg_IPSMovementVehicle', 'Alg_IPSNumber', 'Alg_IPSRoadType', 'Alg_IPSTypeOfLoss', 'Alg_IPSUsageVehicle', 'Alg_IPSVehicleType', 'Alg_IsApplySanctions', 'Alg_IsCatastropheNotApplicable', 'Alg_IsCededCase', 'Alg_IsCededCompany', 'Alg_IsComprehensiveSanctions', 'Alg_IsCourtesyClaim', 'Alg_IsFirstAndFinal', 'Alg_IsInjuredReached', 'Alg_IsMigrated', 'Alg_IsPreventiveClmNotif', 'Alg_IsProfMalPracticeClaim', 'Alg_IsQuickClaim', 'Alg_IsRent87', 'Alg_IsRent87OpenSPAZCase', 'Alg_IsRentCSS', 'Alg_IsScalepoint', 'Alg_IsServiceAffected', 'Alg_LastCloseDate', 'Alg_LastReopenedDate', 'Alg_LCTFleetQuestionaire', 'Alg_LegacyCiriardPolicyNumber', 'Alg_LegacySystem', 'Alg_LiabilityOrigin', 'Alg_LiabilityReasonPH', 'Alg_LossActivity', 'Alg_LossControlTool', 'Alg_LossEventType', 'Alg_LossLocAddressLine1', 'Alg_LosslocCanton', 'Alg_LossLocCity', 'Alg_LosslocCountry', 'Alg_LosslocPostalCode', 'Alg_LossOccurenceDate', 'Alg_LossPlace', 'Alg_ManualStop', 'Alg_MigrationCompleteTime', 'Alg_MigrationStatus', 'Alg_MigrationSubStatus', 'Alg_NextSteps', 'Alg_NGForNVBClaim', 'Alg_NumberOfAttempts', 'Alg_NVBCourtesyDate', 'Alg_OccupationalSafty', 'Alg_OrganisationId', 'Alg_OriginalClaimCreationDate', 'Alg_OriginalClaimNumber', 'Alg_OriginalRent87number', 'Alg_OriginalRenteCSSNumber', 'Alg_PaymentMigrationStatus', 'Alg_PercentDeduction', 'Alg_PercentLiability', 'Alg_PeriodOfValidity', 'Alg_PHKnowsDiagnosis', 'Alg_PHKnowsDiagnosisUpdateTime', 'Alg_ProducingCountry', 'Alg_ProductCNP', 'Alg_ProductCode', 'Alg_ProfMalPracticeClm', 'cc_claim', 'Alg_QualificationNGF', 'Alg_RapportExisting', 'Alg_RegisteredCountry', 'Alg_RelapseCheck', 'Alg_ReturnCallIPEnsued', 'Alg_Sanction', 'Alg_SpecialisationClaim', 'Alg_Statement', 'Alg_StopAutomaticPayment', 'Alg_SubrogationPossible', 'Alg_SubroPossibleCreatedByUser', 'Alg_SuvaFlagRandom', 'Alg_SwissJournalID', 'Alg_TransportFrom', 'Alg_TransportTo', 'Alg_VendorSourceSystem', 'Alg_VerifiedDate', 'AssignedByUserID', 'AssignedGroupID', 'AssignedUserID', 'AssignmentDate', 'AssignmentStatus', 'CatastropheID', 'ClaimNumber', 'ClaimSource', 'CloseDate', 'ClosedOutcome', 'CreateTime', 'CreateUserID', 'DateRptdToAgent', 'Description', 'FaultRating', 'Flagged', 'FlaggedDate', 'FlaggedReason', 'HowReported', 'IncidentReport', 'LitigationStatus', 'LOBCode', 'LossCause', 'LossDate', 'LossLocationID', 'LossType', 'ManifestationDate', 'PermissionRequired', 'PolicyID', 'PublicID', 'ReOpenDate', 'ReopenedReason', 'ReportedByType', 'ReportedDate', 'Segment', 'State', 'Strategy', 'SubrogationStatus', 'UpdateTime', 'UpdateUserID'], True)
df_causerdetail = business_vault.read_data_from_hub('CAUSERDETAIL', ['CauserName', 'CauserInformation', 'ExaminationDate', 'VehicularCategory'], True)
df_lctfleetquestionaire = business_vault.read_data_from_hub('LCTFLEETQUESTIONAIRE', ['LossEventFleet', 'LossCauseFleet', 'DriverLiabilityFleet', 'RoadTypeFleet', 'RoadConditionFleet', 'VehicleUsage', 'EmpRelationshipFleet', 'MovementVehicleFleet', 'VehicleDefectsFleet', 'DriverStateFleet', 'DriverBehaviourFleet', 'LoadLossCauseFleet', 'LoadConditionFleet'], True)
df_productcodes = business_vault.read_data_from_hub('PRODUCTCODES', ['ProductCode'], True)


df_claim = business_vault.join_linked_dataframes(df_claim_og, df_causerdetail, 'LNK__CLAIM__CAUSERDETAIL', 'CLAIM_HKEY', 'CAUSERDETAIL_HKEY', False)
#Manually remove some Hashkeys because of ambiguity in linking function otherwise, ToDo: remove hkey from link 
#df_claim = df_claim.drop("allegro__raw.lnk__claim__causerdetail.$__HKEY",df_causerdetail["allegro__raw.hub__causerdetail.$__HKEY"])
#df_claim = business_vault.join_linked_dataframes(df_claim, df_lctfleetquestionaire, 'LNK__CLAIM__LCTFLEETQUESTIONAIRE', 'CLAIM_HKEY', 'LCTFLEETQUESTIONAIRE_HKEY', False)
#df_claim = business_vault.join_linked_dataframes(df_claim, df_productcodes, 'LNK__CLAIM__PRODUCTCODES', 'CLAIM_HKEY', 'PRODUCTCODES_HKEY', False)

#df_claim = extend_with_user_info(df_claim)
#df_claim.write.mode('overwrite').saveAsTable(claim_table_name)
#spark.sql(f'REFRESH TABLE {claim_table_name}')
#df_claim = spark.table(claim_table_name)


#
# Coverage
#
# Comment: P2000 columns?
#

coverage_table_name = f'{currated_database_name}.coverage'
df_coverage = business_vault.read_data_from_hub('COVERAGE',['Alg_AddtionalInformationDetail', 'Alg_AdditionalInformationTitle', 'Alg_EPTNumberForCnp', 'Alg_GDOProductNumber', 'Alg_GrossPremium', 'Alg_PolicyCover', 'Alg_PolicyCoverOption', 'Alg_SGF', 'Alg_SGS', 'Alg_TechnicalPremium', 'Alg_VA', 'Alg_VG', 'Alg_VOG', 'Alg_VZ4', 'CreateTime', 'CreateUserID', 'Currency', 'PolicyID', 'PolicySystemId', 'PublicID', 'Subtype', 'Type', 'UpdateTime', 'UpdateUserID', 'RiskUnitID', 'Alg_TechnicalCoverCode', 'Alg_TariffPosition'], True)
df_coverage = extend_with_user_info(df_coverage)
#df_coverage.write.mode('overwrite').saveAsTable(coverage_table_name)
#spark.sql(f'REFRESH TABLE {coverage_table_name}')
#df_coverage = spark.table(coverage_table_name)

display(df_coverage.limit(3))


#
# CoverageTerm
#

covterm_table_name = f'{currated_database_name}.covterm'
df_covterm = business_vault.read_data_from_hub('COVERAGETERMS',['Alg_Label', 'Alg_TermType', 'Coverage', 'CovTermPattern', 'CreateTime', 'CreateUser', 'PolicySystemId', 'PublicID', 'Subtype', 'UpdateTime', 'UpdateUser', 'DateFormat', 'DateString', 'Text', 'Code', 'Description', 'Alg_FinancialCurrency', 'FinancialAmount', 'NumericValue', 'Units'], True)
df_covterm = extend_with_user_info(df_covterm)
#df_covterm.write.mode('overwrite').saveAsTable(covterm_table_name)
#spark.sql(f'REFRESH TABLE {covterm_table_name}')
#df_covterm = spark.table(df_covterm)

display(df_covterm.limit(3))


#
# Deductible
#

deductible_table_name = f'{currated_database_name}.deductible'
df_deductible = business_vault.read_data_from_hub('DEDUCTIBLE',['Alg_ApplicableDeductible', 'Alg_DeductibleAmountFixed', 'Alg_DeductibleMax', 'Alg_DeductibleMin', 'Alg_DeductibleNewDriver', 'Alg_Notes', 'Alg_Percentage', 'Alg_SpecialTerm', 'Amount', 'CreateTime', 'Overridden', 'Paid', 'PublicID', 'UpdateTime', 'Waived', 'ClaimID', 'CoverageID', 'CreateUserID', 'UpdateUserID'], True)
df_deductible = extend_with_user_info(df_deductible)
#df_deductible.write.mode('overwrite').saveAsTable(deductible_table_name)
#spark.sql(f'REFRESH TABLE {deductible_table_name}')
#df_deductible = spark.table(df_deductible)

display(df_deductible.limit(3))


#
# Disease
#

disease_table_name = f'{currated_database_name}.disease'
df_disease = business_vault.read_data_from_hub('DISEASE',['Comments', 'CreateTime', 'PublicID', 'UpdateTime', 'DiseasePattern', 'DiseaseSeverity', 'CreateUserID', 'injuryIncidentID', 'UpdateUserID'], True)
df_disease = extend_with_user_info(df_disease)
#df_disease.write.mode('overwrite').saveAsTable(disease_table_name)
#spark.sql(f'REFRESH TABLE {disease_table_name}')
#df_disease = spark.table(df_disease)

display(df_disease.limit(3))


#
# Endorsement
#

endorsement_table_name = f'{currated_database_name}.endorsement'
df_endorsement = business_vault.read_data_from_hub('ENDORSEMENT',['Alg_UserVariables', 'Comments', 'CreateTime', 'DescPerslLine', 'Description', 'FormNumber', 'isNotAsociatedWithClaim', 'PolicySystemId', 'PublicID', 'UpdateTime', 'Coverage', 'CreateUserID', 'PolicyID', 'RiskUnit', 'UpdateUserID'], True)
df_endorsement = extend_with_user_info(df_endorsement)
#df_endorsement.write.mode('overwrite').saveAsTable(endorsement_table_name)
#spark.sql(f'REFRESH TABLE {endorsement_table_name}')
#df_endorsement = spark.table(df_endorsement)

display(df_endorsement.limit(3))