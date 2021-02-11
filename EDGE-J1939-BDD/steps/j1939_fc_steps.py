import os
import shutil
from datetime import datetime, timedelta
from behave import given, when, then
from pypika import Table, Query
from utilities import rest_api_utility as rest_api
from utilities.db_utility import get_edge_db_payload
from utilities.common_utility import exception_handler, set_delay
from utilities.file_utility.file_handler import same_file_contents
from utilities.aws_utilities.s3_utility import upload_object_to_s3, get_key_from_list_of_s3_objects, \
    download_object_from_s3, delete_object_from_s3


@exception_handler
@given(u'A valid EBU FC message in CSV format containing a valid data')
def valid_ebu_fc_message(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED", "CD_PT_POSTED", "FILE_SENT"]
    context.file_name = "edge_192999999999951_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
    context.download_folder_path = "data/j1939_fc/download"
    context.download_converted_file_name = "data/j1939_fc/download/received_j1939_fc_ebu_converted_file.json"
    context.compare_converted_file_name = "data/j1939_fc/compare/j1939_fc_ebu_converted_file.json"
    context.download_ngdi_file_name = "data/j1939_fc/download/received_j1939_fc_ebu_ngdi_file.json"
    context.compare_ngdi_file_name = "data/j1939_fc/compare/j1939_fc_ebu_ngdi_file.json"
    context.device_id = context.ebu_device_id_1
    context.esn = context.ebu_esn_1


@exception_handler
@given(u'A valid EBU FC file in CSV format containing a device_id that does not exist in the EDGE ecosystem')
def valid_ebu_fc_message_with_not_exist_device(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED"]
    context.file_name = "edge_192999999999953_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
    context.device_id = context.not_whitelisted_device_id


@exception_handler
@given(u'An invalid EBU FC file in CSV format containing no device_id value')
def invalid_ebu_fc_message_without_device_id(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED"]
    context.file_name = "edge_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
    context.device_id = context.ebu_esn_1


@exception_handler
@given(u'A valid PSBU FC message in CSV format containing a valid data')
def valid_psbu_fc_message(context):
    context.j1939_fc_stages = ["FILE_RECEIVED", "UNCOMPRESSED", "CSV_JSON_CONVERTED"]
    context.file_name = "edge_192999999999952_20210209123000_19299952_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
    context.download_folder_path = "data/j1939_fc/download"
    context.download_converted_file_name = "data/j1939_fc/download/received_j1939_fc_psbu_converted_file.json"
    context.compare_converted_file_name = "data/j1939_fc/compare/j1939_fc_psbu_converted_file.json"
    context.device_id = context.psbu_device_id_1
    context.esn = context.psbu_esn_1


@exception_handler
@when(u'The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket')
def j1939_fc_file_uploaded_to_s3(context):
    file_key = "bosch-device/" + context.file_name
    file_path = "data/j1939_fc/upload/" + context.file_name
    upload_object_to_s3(context.device_upload_bucket, file_key, file_path)


@then(u'Stored J1939 FC metadata stages in EDGE DB')
@exception_handler
@set_delay(20, wait_before=True)
def assert_j1939_fc_stages_in_edge_db(context):
    current_date_time = (datetime.utcnow() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    da_edge_metadata = Table(context.edge_metadata_table)
    query = Query.from_(da_edge_metadata).select(da_edge_metadata.data_pipeline_stage).where(
        da_edge_metadata.device_id == context.device_id).where(da_edge_metadata.data_protocol == "J1939_FC") \
        .where(da_edge_metadata.file_received_date >= current_date_time)
    edge_db_payload = get_edge_db_payload('get', query)
    edge_db_response = rest_api.post(context.edge_common_db_url, edge_db_payload)
    received_stages = [stage["data_pipeline_stage"] for stage in edge_db_response["body"]]
    assert set(context.j1939_fc_stages) == set(received_stages)


@exception_handler
@then(u'A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the '
      u'edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/fc_file.json with a '
      u'metadata called j1939type whose value is FC')
def assert_j1939_fc_message_in_converted_files(context):
    current_dt = datetime.utcnow()
    file_key = "ConvertedFiles/{0}/{1}/{2}/{3}/{4}/".format(
        context.esn, context.device_id, current_dt.strftime("%Y"), current_dt.strftime("%m"), current_dt.strftime("%d"))
    get_key = get_key_from_list_of_s3_objects(context.final_bucket, file_key)
    assert get_key is not None
    if get_key:
        os.mkdir(context.download_folder_path)
        download_object_from_s3(context.final_bucket, get_key, context.download_converted_file_name)
        assert same_file_contents(context.compare_converted_file_name, context.download_converted_file_name) is True
        shutil.rmtree(context.download_folder_path)
        assert delete_object_from_s3(context.final_bucket, get_key) is True


@exception_handler
@then(u'A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the '
      u'edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/fc_file.json with a metadata '
      u'called j1939type whose value is FC and CP Post Success Message is recorded')
def assert_j1939_fc_message_in_ngdi(context):
    current_dt = datetime.utcnow()
    file_key = "NGDI/{0}/{1}/{2}/{3}/{4}/".format(
        context.esn, context.device_id, current_dt.strftime("%Y"), current_dt.strftime("%m"), current_dt.strftime("%d"))
    get_key = get_key_from_list_of_s3_objects(context.final_bucket, file_key)
    assert get_key is not None
    if get_key:
        os.mkdir(context.download_folder_path)
        download_object_from_s3(context.final_bucket, get_key, context.download_ngdi_file_name)
        assert same_file_contents(context.compare_ngdi_file_name, context.download_ngdi_file_name) is True
        shutil.rmtree(context.download_folder_path)
        assert delete_object_from_s3(context.final_bucket, get_key) is True


@exception_handler
@then(u'No JSON file is created with the FC message in NGDI JSON format as its content and is stored in the '
      u'edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/fc_file.json')
def assert_j1939_fc_message_not_in_ngdi(context):
    file_key = "NGDI/{0}/".format(context.esn)
    get_key = get_key_from_list_of_s3_objects(context.final_bucket, file_key)
    assert get_key is None
