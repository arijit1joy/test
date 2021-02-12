import os
import shutil
from datetime import datetime, timedelta
from behave import given, when, then
from pypika import Table, Query
from utilities import rest_api_utility as rest_api
from utilities.db_utility import get_edge_db_payload
from utilities.common_utility import exception_handler, set_delay
from utilities.aws_utilities.iot_utility import publish_to_mqtt_topic
from utilities.file_utility.file_handler import same_file_contents, get_json_file
from utilities.aws_utilities.s3_utility import get_key_from_list_of_s3_objects, download_object_from_s3, \
    delete_object_from_s3

j1939_hb_payload = {}
j1939_hb_payload_path = "data/j1939_hb/upload/valid_j1939_hb_payload.json"
download_folder_path = "data/j1939_hb/download"
download_converted_file_name = "data/j1939_hb/download/received_j1939_hb_ebu_converted_file.json"
compare_converted_file_name = "data/j1939_hb/compare/j1939_hb_ebu_converted_file.json"
download_ngdi_file_name = "data/j1939_hb/download/received_j1939_hb_ebu_ngdi_file.json"
compare_ngdi_file_name = "data/j1939_hb/compare/j1939_hb_ebu_ngdi_file.json"


@exception_handler
@given(u'A valid EBU HB message in JSON format containing a valid data')
def valid_ebu_hb_message(context):
    context.j1939_hb_stages = ["FILE_RECEIVED", "CD_PT_POSTED", "FILE_SENT"]
    context.device_id = context.ebu_device_id_1
    context.esn = context.ebu_esn_1
    global j1939_hb_payload
    j1939_hb_payload = get_json_file(j1939_hb_payload_path)
    j1939_hb_payload["componentSerialNumber"] = context.esn
    j1939_hb_payload["equipmentId"] = "EDGE_{esn}".format(esn=context.esn)
    j1939_hb_payload["vin"] = context.ebu_vin_1
    j1939_hb_payload["telematicsDeviceId"] = context.device_id


@exception_handler
@when(u'The HB is posted to the /public topic')
def hb_message_published_to_iot(context):
    topic = context.j1939_public_topic.replace("{device_id}", context.device_id)
    publish_to_mqtt_topic(topic, j1939_hb_payload)
