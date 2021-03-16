from pypika import Table, Query
from utilities import rest_api_utility as rest_api
from utilities.db_utility import get_edge_db_payload


def delete_metadata(context):
    da_edge_metadata = Table(context.edge_metadata_table)
    query = Query.from_(da_edge_metadata).delete().where(da_edge_metadata.device_id.isin(
        [context.ebu_device_id_1, context.psbu_device_id_1, context.not_whitelisted_device_id, context.ebu_esn_1]))
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
    context.psbu_vin_1 = "TESTVIN19299951"
    context.not_whitelisted_device_id = "192999999999953"

    # Delete metadata stages stored during last BDD execution
    delete_metadata(context)
