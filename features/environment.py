import tempfile
import shutil

from behave import *
from behave.log_capture import capture
from behave.model import Scenario


def before_all(context):
    userdata = context.config.userdata
    continue_after_failed = userdata.getbool("runner.continue_after_failed_step", False)
    Scenario.continue_after_failed_step = continue_after_failed


@capture()
def before_feature(context, feature):
    context.working_directory = tempfile.mkdtemp()
    
    context.cdc_batch_names = []
    context.raw_vault_tables = []
    context.dates = {}
    context.batches = {}
    context.hkeys = {}


@capture()
def after_feature(context, feature):
    shutil.rmtree(context.working_directory)


@capture()
def before_scenario(context, scenario):
    pass


@capture()
def after_scenario(context, scenario):
    pass