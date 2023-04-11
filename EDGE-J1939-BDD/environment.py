from utilities.j1939_utility import handle_j1939_process

# skipping the feature due to failure in BDD stage

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
    
    # Upload files for J1939 FC and publish payload for J1939 HB to reduce waiting time
    handle_j1939_process(context)
