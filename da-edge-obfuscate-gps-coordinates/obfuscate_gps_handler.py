import os
import json
import boto3
import utility as util
from datetime import datetime
from uuid import uuid4
from edge_gps_utility_layer import handle_gps_coordinates
from db_util import get_certification_family
from db_util import insert_into_metadata_Table

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
        emission_bucket_name = os.environ["j1939_emission_end_bucket"]
        device_id = body["telematicsDeviceId"]

        esn = body["componentSerialNumber"]
        tsp_name = body["telematicsPartnerName"]

        # Please note that the order is expected to be <Make>*<Model>***<ESN>**** for Improper PSBU ESN
        if esn and "*" in esn:
            esn = [esn_component for esn_component in esn.split("*") if esn_component][-1]

        config_id = body["dataSamplingConfigId"]
        current_dt = datetime.now()
        if tsp_name == "COSPA":
            LOGGER.debug("This is a COSMOS HB file")
            file_name = "COSPA_{0}_{1}_{2}_{3}.json".format(device_id, esn, config_id, int(current_dt.timestamp()))
        else:
            file_name = "EDGE_{0}_{1}_{2}_{3}.json".format(device_id, esn, config_id, int(current_dt.timestamp()))
        file_key = "ConvertedFiles/{0}/{1}/{2}/{3}/{4}/{5}".format(
            esn, device_id, current_dt.strftime("%Y"), current_dt.strftime("%m"), current_dt.strftime("%d"), file_name)
        LOGGER.info(f"File Name: {file_name}, File Key:  {file_key}")
        LOGGER.info(f"config_id: {config_id}")
        # Depending on config_id insert to emission bucket else insert to j1939 bucket
        if config_id.startswith('SC9'):
            LOGGER.info(f"Starting additional processing as this is Emission data")
            uuid = str(uuid4())
            #certificationFamily = get_certification_family(body["telematicsDeviceId"], body["componentSerialNumber"])
            #body["certificationFamily"] = certificationFamily
            insert_into_metadata_Table(body["telematicsDeviceId"], uuid, body["componentSerialNumber"], config_id,
                                       file_name, len(str(body)))
            send_to_s3_response = s3_client.put_object(Bucket=emission_bucket_name, Key=file_key,
                                                       Body=json.dumps(body).encode(),
                                                       Metadata={'message_id': uuid})
        else:
            send_to_s3_response = s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(body).encode())
        LOGGER.debug(f"Send to S3 Response: {send_to_s3_response}")
    except Exception as e:
        LOGGER.error(f"An error occurred while sending file to s3:  {e}")
        util.write_to_audit_table(e)
