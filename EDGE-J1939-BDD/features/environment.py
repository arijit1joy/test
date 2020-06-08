import json
import subprocess

from function_definitions import components, definitions, bdd_utility


def before_all(context):

    print("X-------This is Before All function. This happens before the BDD executes for any feature.-------X")
    context.j1939_hb_valid_hb = definitions.get_json_file("j1939_hb_valid_hb")
    context.device_info = json.loads(context.config.userdata["device_info"])
    context.j1939_topic_template = context.config.userdata["public_topic_template"]
    context.j1939_final_bucket = context.config.userdata["final_bucket"]
    context.j1939_uncompress_bucket = context.config.userdata["uncompress_bucket"]
    context.j1939_device_upload_bucket = context.config.userdata["device_upload_bucket"]
    context.j1939_csv_bucket = context.config.userdata["csv_bucket"]


def before_scenario(context, scenario):
    context.scenario = scenario


def after_scenario(context, scenario):

    print("X-------This is After the {} Scenario. "
          "This happens after every scenario has been executed.-------X".format(scenario))
    pt_device_info = context.device_info["pt"]
    ebu_device_info = context.device_info["ebu"]
    invalid_device_info = context.device_info["invalid"]
    try:
        definitions.clean_up_bucket(context.j1939_final_bucket,
                                    "ConvertedFiles/{}/".format(pt_device_info["esn"]), recursive=True)
        definitions.clean_up_bucket(context.j1939_final_bucket,
                                    "ConvertedFiles/{}/".format(ebu_device_info["esn"]), recursive=True)
        definitions.clean_up_bucket(context.j1939_final_bucket,
                                    "ConvertedFiles/{}/".format(invalid_device_info["esn"]), recursive=True)
    except Exception as e:
        print("An Exception occurred while cleaning up ConvertedFiles folder:", e)
    try:
        definitions.clean_up_bucket(context.j1939_final_bucket,
                                    "NGDI/{}/".format(pt_device_info["esn"]), recursive=True)
        definitions.clean_up_bucket(context.j1939_final_bucket,
                                    "NGDI/{}/".format(ebu_device_info["esn"]), recursive=True)
    except Exception as e:
        print("An Exception occurred while cleaning up NGDI folder:", e)
    try:
        definitions.clean_up_bucket(context.j1939_csv_bucket, context.csv_file_name, recursive=True)
    except Exception as e:
        print("An Exception occurred while cleaning up CSV bucket:", e)
    try:
        bdd_utility.update_bdd_parameter("Null")
    except Exception as e:
        print("An Exception occurred while cleaning up the BDD parameter:", e)
