import traceback

# import boto3
import file_handler.handler as handler


from function_definitions import components


def get_json_file(file_name):
    return handler.get_json_file("{}.json".format(file_name))


def get_hb_file(context):
    hb_file = context.j1939_hb_valid_hb
    hb_file["telematicsDeviceId"] = context.device_info["device_id"]
    hb_file["componentSerialNumber"] = context.device_info["esn"]
    hb_file["vin"] = context.device_info["vin"]
    hb_file["equipmentId"] = hb_file["equipmentId"].format(context.device_info["esn"])
    return hb_file


def verify_hb_s3_json_exists(context, converted_or_ngdi, required_metadata=None):
    try:
        bu_info_json = context.device_info[context.bu_type]
        print("Current BU Information for BU -", context.bu_type, ":", bu_info_json)
        esn = bu_info_json["esn"]
        device_id = bu_info_json["device_id"]
        year = "%02d" % context.publish_time.year
        month = "%02d" % context.publish_time.month
        day = "%02d" % context.publish_time.day
        key_exists_ressponse = components.s3_check_if_key_exists(
            context.j1939_final_bucket,
            "{}/{}/{}/{}/{}/{}/{}_".format(converted_or_ngdi, esn, device_id, year, month, day, device_id,),
            required_metadata=required_metadata
        )
        print("Key Exists Response:", key_exists_ressponse)
        assert key_exists_ressponse["response_status_code"] != 500
    except Exception as e:
        print("An Exception occurred! Error: ", e)
        traceback.print_exc()
        return False
