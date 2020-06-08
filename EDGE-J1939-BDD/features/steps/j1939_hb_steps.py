from datetime import datetime
from time import sleep

from behave import *
from function_definitions import components, definitions, bdd_utility
from function_definitions.system_variables import InternalResponse, FCCSVCase


@given(u'A valid {bu_type} HB message in JSON format')
def step_impl(context, bu_type):
    context.bu_type = bu_type.lower()
    context.j1939_hb_valid_hb = definitions.get_hb_file(context)
    context.tsp = context.j1939_hb_valid_hb["telematicsPartnerName"] if "telematicsPartnerName" in \
                                                                        context.j1939_hb_valid_hb else ""
    context.cust_ref = context.j1939_hb_valid_hb["customerReference"] if "customerReference" in \
                                                                         context.j1939_hb_valid_hb else ""
    print("{} HB File:".format(bu_type), context.j1939_hb_valid_hb, sep="\n")


@when(u'The HB is posted to the /public topic')
def step_impl(context):
    print("Received BU Type:", context.bu_type)
    publish_topic = context.j1939_topic_template.replace("<device_id>",
                                                         context.device_info[context.bu_type]["device_id"])
    print("Publishing the HB file to the topic:", publish_topic, ". . .")
    hb_file = context.j1939_hb_valid_hb
    publish_response = components.iot_publish_topic(publish_topic, hb_file)
    context.publish_time = datetime.utcnow()
    print("Publish Response:", publish_response)
    assert publish_response["response_status_code"] != 500, \
        'An error occurred while handling: {}'.format(context.scenario)


@then(u'A JSON file is created with the {hb_or_fc_message} as its content and is stored in the edge-j1939-<env> '
      u'bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/{hb_or_fc}_file.json{fc_metadata_step}')
def step_impl(context, fc_metadata_step, hb_or_fc_message, hb_or_fc):
    sleep(5)  # 5 Second Delay
    fc = meta = {"j1939type": "FC"} if hb_or_fc == "fc" else None
    print("Template variables: hb_or_fc --> {}, hb_or_fc_message --> {}, "
          "fc_metadata_step --> {}".format(hb_or_fc, hb_or_fc_message, fc_metadata_step))
    assert definitions.verify_hb_s3_json_exists(context, "ConvertedFiles", required_metadata=meta, fc=fc) is True, \
        'An error occurred while handling: {}'.format(context.scenario)


@then(u'A JSON file is created with the {hb_or_fc_message} as its content and is stored in the edge-j1939-<env> '
      u'bucket under the file path NGDI/esn/device ID/yyyy/MM/dd/{hb_or_fc}_file.json{fc_metadata_step} and {'
      u'further_step}')
def step_impl(context, further_step, fc_metadata_step, hb_or_fc_message, hb_or_fc):
    sleep(5)  # 5 Second Delay
    fc = meta = {"j1939type": "FC"} if hb_or_fc == "fc" else None
    print("Template variables: further_step --> {}, hb_or_fc --> {}, hb_or_fc_message --> {}, "
          "fc_metadata_step --> {}".format(further_step, hb_or_fc, hb_or_fc_message, fc_metadata_step))
    assert definitions.verify_hb_s3_json_exists(context, "NGDI", required_metadata=meta, fc=fc) is True, \
        'An error occurred while handling: {}'.format(context.scenario)
    print("Next Steps as a Sub-step:", further_step)
    context.execute_steps(
        '''
            Then {}
        '''.format(further_step)
    )


@then(u'CP Post Success Message is recorded')
def step_impl(context):
    sleep(5)  # 10 Second Delay
    assert bdd_utility.check_bdd_parameter(InternalResponse.J1939CPPostSuccess.value) is True, \
        'An error occurred while handling: {}'.format(context.scenario)

    context.execute_steps(
        '''
            Then Success Message
        '''
    )


@given(u'An valid EBU HB message in JSON format containing a device ID that does not exist in the EDGE ecosystem')
def step_impl(context):
    context.bu_type = "invalid"
    context.j1939_hb_valid_hb = definitions.get_hb_file(context)
    context.tsp = context.j1939_hb_valid_hb["telematicsPartnerName"] if "telematicsPartnerName" in \
                                                                        context.j1939_hb_valid_hb else ""
    context.cust_ref = context.j1939_hb_valid_hb["customerReference"] if "customerReference" in \
                                                                         context.j1939_hb_valid_hb else ""
    print("EBU HB File:", context.j1939_hb_valid_hb, sep="\n")


@then(u'No JSON file is created with the {hb_or_fc_message} as its content and is stored in the edge-j1939-<env> '
      u'bucket under the file path NGDI/esn/deviceID/yyyy/MM/dd/{hb_or_fc}_file.json and {further_step}')
def step_impl(context, further_step, hb_or_fc_message, hb_or_fc):
    sleep(5)  # 5 Second Delay
    fc = True if hb_or_fc == "fc" else False
    print("Template variables: further_step --> {}, hb_or_fc --> {}, "
          "hb_or_fc_message --> {}".format(further_step, hb_or_fc, hb_or_fc_message))
    assert definitions.verify_hb_s3_json_does_not_exist(context, "NGDI", fc=fc) is True, \
        'An error occurred while handling: {}'.format(context.scenario)
    print("Next Steps as a Sub-step:", further_step)
    context.execute_steps(
        '''
            Then {}
        '''.format(further_step)
    )


@then(u'There is a DeviceID error recorded')
def step_impl(context):
    sleep(5)  # 5 Second Delay
    assert bdd_utility.check_bdd_parameter(InternalResponse.J1939BDDDeviceInfoError.value) is True, \
        'An error occurred while handling: {}'.format(context.scenario)
    context.execute_steps(
        '''
            Then Success Message
        '''
    )


@given(u'An invalid EBU HB message in JSON format containing a valid deviceID but missing the telematicsPartnerName '
       u'and customerReference')
def step_impl(context):
    context.bu_type = "ebu"
    hb_file = definitions.get_hb_file(context)
    hb_file.pop("telematicsPartnerName")
    hb_file.pop("customerReference")
    context.tsp = context.j1939_hb_valid_hb["telematicsPartnerName"] if "telematicsPartnerName" in \
                                                                        context.j1939_hb_valid_hb else ""
    context.cust_ref = context.j1939_hb_valid_hb["customerReference"] if "customerReference" in \
                                                                         context.j1939_hb_valid_hb else ""
    context.j1939_hb_valid_hb = hb_file
    print("EBU HB File:", context.j1939_hb_valid_hb, sep="\n")


@then(u'There is no CP post success recorded')
def step_impl(context):
    sleep(5)  # 5 Second Delay
    assert bdd_utility.check_bdd_parameter(InternalResponse.J1939CPPostSuccess.value) is False, \
        'An error occurred while handling: {}'.format(context.scenario)
    context.execute_steps(
        '''
            Then Success Message
        '''
    )


@given(u'An invalid EBU HB message in JSON format containing a valid deviceID but having incorrect values for the '
       u'telematicsPartnerName and customerReference')
def step_impl(context):
    context.bu_type = "ebu"
    hb_file = definitions.get_hb_file(context)
    hb_file["telematicsPartnerName"] = "InvalidTSP"
    hb_file["customerReference"] = "InvalidCustRef"
    context.tsp = context.j1939_hb_valid_hb["telematicsPartnerName"] if "telematicsPartnerName" in \
                                                                        context.j1939_hb_valid_hb else ""
    context.cust_ref = context.j1939_hb_valid_hb["customerReference"] if "customerReference" in \
                                                                         context.j1939_hb_valid_hb else ""
    context.j1939_hb_valid_hb = hb_file
    print("EBU HB File:", context.j1939_hb_valid_hb, sep="\n")


@then(u'Success Message')
def step_impl(context):
    print("Successfully Verified the Scenario!")
