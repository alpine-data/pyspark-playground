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
