from time import sleep
from utilities.common_utility import exception_handler
from utilities.j1939_utility import delete_metadata, get_j1939_fc_data_set, get_j1939_hb_data_set
from utilities.aws_utilities.s3_utility import upload_object_to_s3
from utilities.aws_utilities.iot_utility import publish_to_mqtt_topic


def before_all(context):
    environment = context.config.userdata["environment"]
    context.region = context.config.userdata["region"]
    edge_base_url = "https://api.edge-{environment}.aws.cummins.com".format(environment=environment)
    context.edge_common_db_url = edge_base_url + "/v3/EdgeDBLambda"
    context.device_health_data_table = "da_edge_olympus.da_edge_device_health_data"
    context.edge_metadata_table = "da_edge_olympus.da_edge_metadata"
    context.j1939_public_topic = "$aws/things/{device_id}/public"
    context.device_upload_bucket = "device-data-log-{environment}".format(environment=environment)
    context.final_bucket = "edge-j1939-{environment}".format(environment=environment)
    context.ebu_device_id_1 = "192999999999951"
    context.ebu_esn_1 = "19299951"
    context.ebu_vin_1 = "TESTVIN19299951"
    context.ebu_device_id_2 = "192999999999955"
    context.ebu_esn_2 = "19299955"
    context.ebu_vin_2 = "TESTVIN19299955"
    context.ebu_device_id_3 = "192999999999956"
    context.ebu_esn_3 = "19299956"
    context.ebu_vin_3 = "TESTVIN19299956"
    context.psbu_device_id_1 = "192999999999952"
    context.psbu_esn_1 = "19299952"
    context.psbu_vin_1 = "TESTVIN19299952"
    context.psbu_device_id_2 = "192999999999954"
    context.psbu_esn_2 = "19299954"
    context.psbu_vin_2 = "TESTVIN19299954"
    context.not_whitelisted_device_id = "192999999999953"

    # Delete metadata stages stored during last BDD execution
    delete_metadata(context)


@exception_handler
def before_feature(context, feature):
    if "J1939 Fault Code" in feature.name:
        j1939_fc_data_set = get_j1939_fc_data_set(context)

        for file_name in j1939_fc_data_set:
            file_key = "bosch-device/" + file_name
            file_path = "data/j1939_fc/upload/" + file_name
            upload_object_to_s3(context.device_upload_bucket, file_key, file_path)

        # Wait for 4.5 minutes to allow all J1939 FC lambdas to process
        time_in_secs = 270

    elif "J1939 Heart Beat" in feature.name:
        j1939_hb_data_set = get_j1939_hb_data_set(context)

        for device_id, j1939_hb_payload in j1939_hb_data_set.items():
            topic = context.j1939_public_topic.replace("{device_id}", device_id)
            publish_to_mqtt_topic(topic, j1939_hb_payload, context.region)

            # Using j1939_hb_payload in "then" behavior
            context.j1939_hb_payload = j1939_hb_payload

        # Wait for 2.5 minutes to allow all J1939 HB lambdas to process
        time_in_secs = 150

    else:
        time_in_secs = 0

    print(f"Delaying {time_in_secs} Seconds for Feature: {feature.name}")
    sleep(time_in_secs)
