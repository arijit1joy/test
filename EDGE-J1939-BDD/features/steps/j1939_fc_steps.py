from datetime import datetime
from time import sleep

from behave import *
from function_definitions import components, definitions, bdd_utility
from function_definitions.system_variables import InternalResponse, FCCSVCase


@given(u'A valid EBU FC file in CSV format')
def step_impl(context):
    context.bu_type = "ebu"
    context.tsp = "Cummins"
    context.j1939_fc_csv = definitions.get_csv_file(FCCSVCase.ValidFCCSV.value)
    context.j1939_fc_json = definitions.get_json_file(FCCSVCase.ValidFCCSV.value)


@given(u'A valid EBU FC file in CSV format containing a device ID that does not exist in the EDGE ecosystem')
def step_impl(context):
    context.bu_type = "invalid"
    context.tsp = "Cummins"
    context.j1939_fc_csv = definitions.get_csv_file(FCCSVCase.ValidFCCSVInvalidDeviceID.value)
    context.j1939_fc_json = definitions.get_json_file(FCCSVCase.ValidFCCSVInvalidDeviceID.value)


@given(u'An invalid EBU FC file in CSV format containing no device ID value')
def step_impl(context):
    context.bu_type = "invalid"
    context.tsp = "Cummins"
    context.j1939_fc_csv = definitions.get_csv_file(FCCSVCase.InvalidFCCSVNoDeviceID.value)
    context.j1939_fc_json = definitions.get_json_file(FCCSVCase.InvalidFCCSVNoDeviceID.value)


@given(u'A valid EBU FC message in CSV format containing a valid deviceID but missing the single sample row')
def step_impl(context):
    context.bu_type = "ebu"
    context.tsp = "Cummins"
    context.j1939_fc_csv = definitions.get_csv_file(FCCSVCase.ValidFCCSVNoSS.value)
    context.j1939_fc_json = definitions.get_json_file(FCCSVCase.ValidFCCSVNoSS.value)


@given(u'An invalid EBU FC message in CSV format containing a valid deviceID but missing the all sample row')
def step_impl(context):
    context.bu_type = "ebu"
    context.tsp = "Cummins"
    context.j1939_fc_csv = definitions.get_csv_file(FCCSVCase.InvalidFCCSVNoAS.value)
    context.j1939_fc_json = definitions.get_json_file(FCCSVCase.InvalidFCCSVNoAS.value)


@given(u'A valid EBU FC message in CSV format containing no device parameters in the all sample rows')
def step_impl(context):
    context.bu_type = "ebu"
    context.tsp = "Cummins"
    context.j1939_fc_csv = definitions.get_csv_file(FCCSVCase.ValidFCCSVNoDeviceParams.value)
    context.j1939_fc_json = definitions.get_json_file(FCCSVCase.ValidFCCSVNoDeviceParams.value)


@when(u'The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket')
def step_impl(context):
    assert components.s3_put_csv_object(context.j1939_csv_bucket,
                                        definitions.get_csv_key(context),
                                        context.j1939_fc_csv) != 500, \
        'An error occurred while handling: {}'.format(context.scenario)


@then(u'CSV Convert success is not recorded')
def step_impl(context):
    sleep(5)  # 10 Second Delay
    assert bdd_utility.check_bdd_parameter(InternalResponse.J1939BDDCSVConvertSuccess.value) is False, \
        'An error occurred while handling: {}'.format(context.scenario)

    context.execute_steps(
        '''
            Then Success Message
        ''')
