from time import sleep
from pypika import Table, Query
from utilities import rest_api_utility as rest_api
from utilities.db_utility import get_edge_db_payload
from utilities.common_utility import exception_handler
from utilities.file_utility.file_handler import get_json_file
from utilities.aws_utilities.s3_utility import upload_object_to_s3
from utilities.aws_utilities.iot_utility import publish_to_mqtt_topic

J1939_HB_PAYLOAD_PATH = "data/j1939_hb/upload/valid_j1939_hb_payload.json"


def delete_metadata(context):
    device_ids = [context.ebu_device_id_1, context.ebu_device_id_2, context.ebu_device_id_3, context.psbu_device_id_1,
                  context.psbu_device_id_2, context.not_whitelisted_device_id, context.ebu_esn_1]
    da_edge_metadata = Table(context.edge_metadata_table)
    query = Query.from_(da_edge_metadata).delete().where(da_edge_metadata.device_id.isin(device_ids))
    edge_db_payload = get_edge_db_payload('post', query)
    edge_db_response = rest_api.post(context.edge_common_db_url, edge_db_payload)
    print(f"Delete Metadata Response: {edge_db_response}")


def get_j1939_fc_data_set(context):
    context.file_name_for_ebu_scenario_1 = "edge_192999999999951_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
    context.file_name_for_ebu_scenario_2 = "edge_192999999999953_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
    context.file_name_for_ebu_scenario_3 = "edge_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
    context.file_name_for_psbu_scenario_1 = "edge_192999999999952_19299952_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
    context.file_name_for_psbu_scenario_2 = "edge_192999999999954_BDD001_2021-02-09T12_30_00.015Z.csv.gz"

    j1939_fc_data_set = [context.file_name_for_ebu_scenario_1, context.file_name_for_ebu_scenario_2,
                         context.file_name_for_ebu_scenario_3, context.file_name_for_psbu_scenario_1,
                         context.file_name_for_psbu_scenario_2]

    return j1939_fc_data_set


def create_j1939_hb_payload(device_id, esn, vin):
    j1939_hb_payload = get_json_file(J1939_HB_PAYLOAD_PATH)
    j1939_hb_payload["componentSerialNumber"] = esn
    j1939_hb_payload["equipmentId"] = "EDGE_{esn}".format(esn=esn)
    j1939_hb_payload["vin"] = vin
    j1939_hb_payload["telematicsDeviceId"] = device_id

    return j1939_hb_payload


def get_j1939_hb_data_set(context):
    hb_payload_for_ebu_scenario_1 = create_j1939_hb_payload(context.ebu_device_id_1, context.ebu_esn_1,
                                                            context.ebu_vin_1)
    hb_payload_for_ebu_scenario_2 = create_j1939_hb_payload(context.not_whitelisted_device_id, context.ebu_esn_1,
                                                            context.ebu_vin_1)
    hb_payload_for_ebu_scenario_3 = create_j1939_hb_payload(context.ebu_device_id_2, context.ebu_esn_2,
                                                            context.ebu_vin_2)
    del hb_payload_for_ebu_scenario_3["telematicsPartnerName"]
    del hb_payload_for_ebu_scenario_3["customerReference"]

    hb_payload_for_ebu_scenario_4 = create_j1939_hb_payload(context.ebu_device_id_3, context.ebu_esn_3,
                                                            context.ebu_vin_3)
    hb_payload_for_ebu_scenario_4["telematicsPartnerName"] = "Invalid_TPN"
    hb_payload_for_ebu_scenario_4["customerReference"] = "Invalid_CR"

    hb_payload_for_psbu_scenario_1 = create_j1939_hb_payload(context.psbu_device_id_1, context.psbu_esn_1,
                                                             context.psbu_vin_1)

    j1939_hb_data_set = {context.ebu_device_id_1: hb_payload_for_ebu_scenario_1,
                         context.not_whitelisted_device_id: hb_payload_for_ebu_scenario_2,
                         context.ebu_device_id_2: hb_payload_for_ebu_scenario_3,
                         context.ebu_device_id_3: hb_payload_for_ebu_scenario_4,
                         context.psbu_device_id_1: hb_payload_for_psbu_scenario_1}

    return j1939_hb_data_set


@exception_handler
def handle_j1939_process(context):
    # Delete metadata stages stored during last BDD execution
    delete_metadata(context)

    # Set up and upload files for J1939 FC
    j1939_fc_data_set = get_j1939_fc_data_set(context)

    for file_name in j1939_fc_data_set:
        file_key = "bosch-device/" + file_name
        file_path = "data/j1939_fc/upload/" + file_name
        upload_object_to_s3(context.device_upload_bucket, file_key, file_path)

    print("<---Finished setting up data for J1939 FC--->")

    # Set up and publish payload for J1939 HB
    j1939_hb_data_set = get_j1939_hb_data_set(context)

    for device_id, j1939_hb_payload in j1939_hb_data_set.items():
        topic = context.j1939_public_topic.replace("{device_id}", device_id)
        publish_to_mqtt_topic(topic, j1939_hb_payload, context.region)

        # Using j1939_hb_payload in "then" steps
        context.j1939_hb_payload = j1939_hb_payload

    print("<---Finished setting up data for J1939 HB--->")

    # Wait for 6.5 minutes to allow all J1939 lambdas to process
    time_in_secs = 390

    print(f"Delaying {time_in_secs} Seconds for J1939 Features..!")
    sleep(time_in_secs)
