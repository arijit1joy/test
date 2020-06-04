import traceback
from datetime import datetime

from behave import *
from function_definitions import components, definitions


@given(u'A valid {bu_type} HB message in JSON format')
def step_impl(context, bu_type):
    try:
        valid_ebu_json = context.j1939_hb_valid_hb
        print("EBU HB File:", valid_ebu_json, sep="\n")
        context.bu_type = bu_type.lower()
    except Exception as e:
        traceback.print_exc()
        raise Exception("An Exception occurred! Error: ", e)


@when(u'The HB is posted to the /public topic')
def step_impl(context):
    try:
        print("Received BU Type:", context.bu_type)
        publish_topic = context.j1939_topic_template.replace("<device_id>", "device_id")
        print("Publishing the HB file to the topic:", publish_topic, ". . .")
        hb_file = definitions.get_hb_file(context)
        publish_response = components.iot_publish_topic(publish_topic, hb_file)
        context.publish_time = datetime.now()
        print("Publish Response:", publish_response)
        assert publish_response["response_status_code"] != 500
    except Exception as e:
        traceback.print_exc()
        raise Exception("An Exception occurred! Error: ", e)


@then(u'A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under '
      u'the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/hb_file.json with a metadata called "j1939-type" whose '
      u'value is "HB"')
def step_impl(context):
    assert definitions.verify_hb_s3_json_exists(context, "ConvertedFiles", required_metadata={"j1939-type": "HB"}) is \
           True


@then(u'A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under '
      u'the file path NGDI/esn/device ID/yyyy/MM/dd/hb_file.json with a metadata called "j1939-type" whose value is '
      u'"HB"')
def step_impl(context):
    assert definitions.verify_hb_s3_json_exists(context, "NGDI",  required_metadata={"j1939-type": "HB"}) is True
    context.execute_steps(
        '''
            Then Success Message
        '''
    )


@then(u'Success Message')
def step_impl(context):
    print("Successfully Verified the Feature")
