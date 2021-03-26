from time import sleep
from pypika import Table, Query
from utilities import rest_api_utility as rest_api
from utilities.db_utility import get_edge_db_payload
from utilities.common_utility import exception_handler
from utilities.aws_utilities.s3_utility import upload_object_to_s3


def delete_metadata(context):
    da_edge_metadata = Table(context.edge_metadata_table)
    query = Query.from_(da_edge_metadata).delete().where(da_edge_metadata.device_id.isin(
        [context.ebu_device_id_1, context.psbu_device_id_1, context.psbu_device_id_2,
         context.not_whitelisted_device_id, context.ebu_esn_1]))
    edge_db_payload = get_edge_db_payload('post', query)
    edge_db_response = rest_api.post(context.edge_common_db_url, edge_db_payload)
    print(f"Delete Metadata Response: {edge_db_response}")


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
        context.file_name_for_ebu_scenario_1 = "edge_192999999999951_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
        context.file_name_for_ebu_scenario_2 = "edge_192999999999953_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
        context.file_name_for_ebu_scenario_3 = "edge_19299951_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
        context.file_name_for_psbu_scenario_1 = "edge_192999999999952_19299952_BDD001_2021-02-09T12_30_00.015Z.csv.gz"
        context.file_name_for_psbu_scenario_2 = "edge_192999999999954_BDD001_2021-02-09T12_30_00.015Z.csv.gz"

        files = [context.file_name_for_ebu_scenario_1, context.file_name_for_ebu_scenario_2,
                 context.file_name_for_ebu_scenario_3, context.file_name_for_psbu_scenario_1,
                 context.file_name_for_psbu_scenario_2]

        for file_name in files:
            file_key = "bosch-device/" + file_name
            file_path = "data/j1939_fc/upload/" + file_name
            upload_object_to_s3(context.device_upload_bucket, file_key, file_path)

        # Wait for 5 minutes to allow all J1939 FC lambdas to process
        sleep(300)
