import os
import shutil
from datetime import datetime
from behave import given, when, then
from pypika import Table, Query
from utilities import rest_api_utility as rest_api
from utilities.db_utility import get_edge_db_payload
from utilities.common_utility import exception_handler
from utilities.file_utility.file_handler import same_file_contents
from utilities.aws_utilities.s3_utility import get_key_from_list_of_s3_objects, download_object_from_s3, \
    delete_object_from_s3

DOWNLOAD_FOLDER_PATH = "data/j1939_fc/download"
DATE_TIME_STAMP = "%Y-%m-%dT%H"


@exception_handler
@given(u'A valid EBU FC message in CSV format containing a valid data')
def valid_ebu_fc_message(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED", "CD_PT_POSTED", "FILE_SENT"]
    context.file_name = context.file_name_for_ebu_scenario_1
    context.download_converted_file_name = "data/j1939_fc/download/received_j1939_fc_ebu_converted_file.json"
    context.compare_converted_file_name = "data/j1939_fc/compare/j1939_fc_ebu_converted_file.json"
    context.download_ngdi_file_name = "data/j1939_fc/download/received_j1939_fc_ebu_ngdi_file.json"
    context.compare_ngdi_file_name = "data/j1939_fc/compare/j1939_fc_ebu_ngdi_file.json"
    context.device_id = context.ebu_device_id_1
    context.esn = context.ebu_esn_1
    context.date_path = datetime.strptime(context.file_name.split("_")[4], DATE_TIME_STAMP).strftime("%Y/%m/%d")


@exception_handler
@given(u'A valid EBU FC file in CSV format containing a device_id that does not exist in the EDGE ecosystem')
def valid_ebu_fc_message_with_not_exist_device(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED"]
    context.file_name = context.file_name_for_ebu_scenario_2
    context.device_id = context.not_whitelisted_device_id


@exception_handler
@given(u'An invalid EBU FC file in CSV format containing no device_id value')
def invalid_ebu_fc_message_without_device_id(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED"]
    context.file_name = context.file_name_for_ebu_scenario_3
    context.device_id = context.ebu_esn_1


@exception_handler
@given(u'A valid PSBU FC message in CSV format containing a valid data')
def valid_psbu_fc_message(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED", "FILE_SENT"]
    context.file_name = context.file_name_for_psbu_scenario_1
    context.download_converted_file_name = "data/j1939_fc/download/received_j1939_fc_psbu_converted_file.json"
    context.compare_converted_file_name = "data/j1939_fc/compare/j1939_fc_psbu_converted_file.json"
    context.device_id = context.psbu_device_id_1
    context.esn = context.psbu_esn_1
    context.date_path = datetime.strptime(context.file_name.split("_")[4], DATE_TIME_STAMP).strftime("%Y/%m/%d")


@exception_handler
@given(u'A valid PSBU FC message in CSV format containing a valid data and filename without ESN')
def valid_psbu_fc_message_without_esn_in_filename(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED", "FILE_SENT"]
    context.file_name = context.file_name_for_psbu_scenario_2
    context.download_converted_file_name = \
        "data/j1939_fc/download/received_j1939_fc_psbu_converted_file_improper_esn.json"
    context.compare_converted_file_name = "data/j1939_fc/compare/j1939_fc_psbu_converted_file_improper_esn.json"
    context.device_id = context.psbu_device_id_2
    context.esn = context.psbu_esn_2
    context.date_path = datetime.strptime(context.file_name.split("_")[3], DATE_TIME_STAMP).strftime("%Y/%m/%d")


@exception_handler
@when(u'The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket')
def j1939_fc_file_uploaded_to_s3(context):
    file_key = "bosch-device/" + context.file_name
    get_key = get_key_from_list_of_s3_objects(context.device_upload_bucket, file_key)
    assert get_key is not None
    assert delete_object_from_s3(context.device_upload_bucket, get_key) is True


@exception_handler
@then(u'Stored J1939 FC metadata stages in EDGE DB')
def assert_j1939_fc_stages_in_edge_db(context):
    da_edge_metadata = Table(context.edge_metadata_table)
    query = Query.from_(da_edge_metadata).select(da_edge_metadata.data_pipeline_stage).where(
        da_edge_metadata.device_id == context.device_id).where(da_edge_metadata.data_protocol == "J1939_FC")  # noqa
    edge_db_payload = get_edge_db_payload('get', query)
    edge_db_response = rest_api.post(context.edge_common_db_url, edge_db_payload)
    received_stages = [stage["data_pipeline_stage"] for stage in edge_db_response["body"]]
    assert set(context.j1939_fc_stages) == set(received_stages)


@exception_handler
@then(u'A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the '
      u'edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/fc_file.json with a '
      u'metadata called j1939type whose value is FC')
def assert_j1939_fc_message_in_converted_files(context):
    file_key = "ConvertedFiles/{0}/{1}/{2}/".format(context.esn, context.device_id, context.date_path)
    get_key = get_key_from_list_of_s3_objects(context.final_bucket, file_key)
    assert get_key is not None
    if get_key:
        os.mkdir(DOWNLOAD_FOLDER_PATH)
        download_object_from_s3(context.final_bucket, get_key, context.download_converted_file_name)
        assert same_file_contents(context.compare_converted_file_name, context.download_converted_file_name) is True
        shutil.rmtree(DOWNLOAD_FOLDER_PATH)


@exception_handler
@then(u'A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the '
      u'edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/fc_file.json with a metadata '
      u'called j1939type whose value is FC and CP Post Success Message is recorded')
def assert_j1939_fc_message_in_ngdi(context):
    file_key = "NGDI/{0}/{1}/{2}/".format(context.esn, context.device_id, context.date_path)
    get_key = get_key_from_list_of_s3_objects(context.final_bucket, file_key)
    assert get_key is not None
    if get_key:
        os.mkdir(DOWNLOAD_FOLDER_PATH)
        download_object_from_s3(context.final_bucket, get_key, context.download_ngdi_file_name)
        assert same_file_contents(context.compare_ngdi_file_name, context.download_ngdi_file_name) is True
        shutil.rmtree(DOWNLOAD_FOLDER_PATH)


@exception_handler
@then(u'No JSON file is created with the FC message in NGDI JSON format as its content and is stored in the '
      u'edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/fc_file.json')
def assert_j1939_fc_message_not_in_ngdi(context):
    file_key = "NGDI/{0}/{1}/".format(context.esn, context.device_id)
    get_key = get_key_from_list_of_s3_objects(context.final_bucket, file_key)
    assert get_key is None
