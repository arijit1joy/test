def before_all(context):
    environment = context.config.userdata["environment"]
    edge_base_url = "https://api.edge-{environment}.aws.cummins.com".format(environment=environment)
    context.edge_common_db_url = edge_base_url + "/v3/EdgeDBLambda"
    context.edge_metadata_table = "da_edge_olympus.da_edge_metadata"
    context.j1939_public_topic = "$aws/things/{device_id}/public"
    context.device_upload_bucket = "device-data-log-{environment}".format(environment=environment)
    context.uncompress_bucket = "device-data-log-uncompressed-files-{environment}".format(environment=environment)
    context.csv_bucket = "da-edge-j1939-datalog-files-{environment}".format(environment=environment)
    context.final_bucket = "edge-j1939-{environment}".format(environment=environment)
    context.ebu_device_id_1 = "192999999999951"
    context.ebu_esn_1 = "19299951"
    context.ebu_vin_1 = "TESTVIN19299951"
    context.psbu_device_id_1 = "192999999999952"
    context.psbu_esn_1 = "19299952"
    context.psbu_vin_1 = "TESTVIN19299951"
    context.not_whitelisted_device_id = "192999999999953"
