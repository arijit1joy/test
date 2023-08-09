import os
import json
import boto3
import utility as util
from datetime import datetime
from obfuscate_gps_utility import handle_gps_coordinates

LOGGER = util.get_logger(__name__)


def obfuscate_gps(body):
    if "samples" in body:
        for sample in body["samples"]:
            if "convertedDeviceParameters" in sample:
                converted_device_params = sample["convertedDeviceParameters"]
                if "Latitude" in converted_device_params and "Longitude" in converted_device_params:
                    latitude = converted_device_params["Latitude"]
                    longitude = converted_device_params["Longitude"]
                    LOGGER.info(f"Latitude: {latitude}, Longitude: {longitude}, before obfuscated gps coordinates")
                    converted_device_params["Latitude"], converted_device_params["Longitude"] = \
                        handle_gps_coordinates(latitude, longitude)
                    LOGGER.info(f"Latitude: {converted_device_params['Latitude']}, "
                                f"Longitude: {converted_device_params['Longitude']}, after obfuscated gps coordinates")
    send_file_to_s3(body)


def send_file_to_s3(body):
    try:
        s3_client = boto3.client("s3")
        bucket_name = os.environ["j1939_end_bucket"]
        device_id = body["telematicsDeviceId"]

        esn = body["componentSerialNumber"]
        tsp_name = body["telematicsPartnerName"]
        if tsp_name=="COSPA":
            LOGGER.info("This is a COSMOS HB file")

        # Please note that the order is expected to be <Make>*<Model>***<ESN>**** for Improper PSBU ESN
        if esn and "*" in esn:
            esn = [esn_component for esn_component in esn.split("*") if esn_component][-1]

        config_id = body["dataSamplingConfigId"]
        current_dt = datetime.now()
        file_name = "EDGE_{0}_{1}_{2}_{3}.json".format(device_id, esn, config_id, int(current_dt.timestamp()))
        file_key = "ConvertedFiles/{0}/{1}/{2}/{3}/{4}/{5}".format(
            esn, device_id, current_dt.strftime("%Y"), current_dt.strftime("%m"), current_dt.strftime("%d"), file_name)
        LOGGER.info(f"File Name: {file_name}, File Key:  {file_key}")
        send_to_s3_response = s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(body).encode())
        LOGGER.debug(f"Send to S3 Response: {send_to_s3_response}")
    except Exception as e:
        LOGGER.error(f"An error occurred while sending file to s3:  {e}")
        util.write_to_audit_table(e)
