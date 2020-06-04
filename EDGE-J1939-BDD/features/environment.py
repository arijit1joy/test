import json

import function_definitions.definitions as definitions


def before_all(context):

    print("X-------This is Before All function. This happens before the BDD executes for any feature.-------X")
    context.j1939_hb_valid_hb = definitions.get_json_file("j1939_hb_valid_hb")
    context.device_info = json.loads(context.config.userdata["device_info"])
    context.j1939_topic_template = context.config.userdata["public_topic_template"]
    context.j1939_final_bucket = context.config.userdata["final_bucket"]
    context.j1939_uncompress_bucket = context.config.userdata["uncompress_bucket"]
    context.j1939_device_upload_bucket = context.config.userdata["device_upload_bucket"]
    context.j1939_csv_bucket = context.config.userdata["csv_bucket"]


def after_all(context):

    print("X-------This is After All function. This happens after the BDD for every feature has been executed.-------X")
