import subprocess
import traceback
from datetime import datetime

import file_handler.handler as handler

from function_definitions import components


def get_json_file(file_name):
    return handler.get_json_file("{}.json".format(file_name))


def get_csv_file(file_name):
    return handler.get_csv_binary_file("{}.csv".format(file_name))


def get_hb_file(context):
    hb_file = context.j1939_hb_valid_hb
    bu_info_json = context.device_info[context.bu_type]
    hb_file["telematicsDeviceId"] = bu_info_json["device_id"]
    hb_file["componentSerialNumber"] = bu_info_json["esn"]
    hb_file["vin"] = bu_info_json["vin"]
    hb_file["equipmentId"] = "EDGE_{}".format(bu_info_json["esn"])
    return hb_file


def verify_s3_json_exists(context, converted_or_ngdi, required_metadata=None, fc=False):
    try:
        bu_info_json = context.device_info[context.bu_type]
        print("Current BU Information for BU -", context.bu_type, ":", bu_info_json)
        esn = bu_info_json["esn"]
        device_id = bu_info_json["device_id"]
        year = "%02d" % context.publish_time.year
        month = "%02d" % context.publish_time.month
        day = "%02d" % context.publish_time.day
        file_name = context.fc_json_file_name if fc else context.hb_json_file_name
        match = context.j1939_fc_json if fc else None

        if converted_or_ngdi.lower == "ngdi" and fc:
            match["vin"] = "BDDTEST00000000"
            match["equipmentId"] = "EDGE_15240000"

        key_exists_response = components.s3_check_if_key_exists(
            context.j1939_final_bucket,
            "{}/{}/{}/{}/{}/{}/{}"
            .format(converted_or_ngdi, esn, device_id, year, month, day, file_name),
            required_metadata=required_metadata, matches_json=match)

        print("Key Exists Response:", key_exists_response)
        assert key_exists_response["response_status_code"] != 500, "An error occurred while verifying that the file" \
                                                                   " exists!"
        return True
    except Exception as e:
        print("An Exception occurred! Error: ", e)
        traceback.print_exc()
        return False


def verify_s3_json_does_not_exist(context, converted_or_ngdi, fc=False):
    try:
        bu_info_json = context.device_info[context.bu_type]
        print("Current BU Information for BU -", context.bu_type, ":", bu_info_json)
        esn = bu_info_json["esn"]
        device_id = bu_info_json["device_id"]
        year = "%02d" % context.publish_time.year
        month = "%02d" % context.publish_time.month
        day = "%02d" % context.publish_time.day
        file_name = context.hb_json_file_name if not fc else context.fc_json_file_name
        key_exists_response = components.s3_check_if_key_exists(
            context.j1939_final_bucket,
            "{}/{}/{}/{}/{}/{}/{}".format(converted_or_ngdi, esn, device_id, year, month, day, file_name))
        print("Key Exists Response:", key_exists_response)
        assert key_exists_response["response_status_code"] == 500, "An error occurred while verifying that the HB " \
                                                                   "Json does not exist!"
        return True
    except Exception as e:
        print("An Exception occurred! Error: ", e)
        traceback.print_exc()
        return False


def clean_up_bucket(bucket, path, recursive=False):
    print("Cleaning up the path: '{}' from the bucket: '{}'".format(path, bucket))
    subprocess.call("aws s3 rm s3://{}/{} --recursive".format(bucket, path), shell=True) if recursive else \
        subprocess.call("aws s3 rm s3://{}/{}".format(bucket, path), shell=True)


def set_s3_file_name(context, has_json=None, is_hb=False):
    bu_info_json = context.device_info[context.bu_type]
    print("Current BU Information for BU -", context.bu_type, ":", bu_info_json)
    esn = bu_info_json["esn"]
    device_id = bu_info_json["device_id"]
    context.publish_time = datetime.utcnow()
    if not is_hb:
        context.fc_csv_file_name = "EDGE_{}_{}_BDD0000_{}_{}_BDD0.csv" \
            .format(device_id, esn, context.publish_time.strftime("%Y%m%d%H%M%S"),
                    context.publish_time.strftime("%Y-%m-%dT%H:%M:%S.%f"))
        context.fc_json_file_name = "EDGE_{}_{}_BDD0000_{}_{}_BDD0.json" \
            .format(device_id, esn, context.publish_time.strftime("%Y%m%d%H%M%S"),
                    context.publish_time.strftime("%Y-%m-%dT%H:%M:%S.%f")) if has_json else None
    else:
        context.hb_json_file_name = "EDGE_{}_{}_BDD0000_{}.json" \
            .format(device_id, esn, int(datetime.strptime(context.publish_time.strftime('%Y-%m-%d %H:%M'),
                                                          '%Y-%m-%d %H:%M').timestamp()))
