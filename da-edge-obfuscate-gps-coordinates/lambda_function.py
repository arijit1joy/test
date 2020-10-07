import os
import json
import boto3
from datetime import datetime
from obfuscate_gps_utility import obfuscate_gps_coordinates


def lambda_handler(event, context):
    print("Event: ", event)
    body = event
    if "samples" in body:
        for sample in body["samples"]:
            if "convertedDeviceParameters" in sample:
                converted_device_params = sample["convertedDeviceParameters"]
                if "Latitude" in converted_device_params and "Longitude" in converted_device_params:
                    latitude = converted_device_params["Latitude"]
                    longitude = converted_device_params["Longitude"]
                    latitude, longitude = obfuscate_gps_coordinates(latitude, longitude)

    s3_client = boto3.client("s3")
    bucket_name = os.environ["j1939_end_bucket"]
    device_id = body["telematicsDeviceId"]
    esn = body["componentSerialNumber"]
    config_id = body["dataSamplingConfigId"]
    current_dt = datetime.now()
    file_name = "EDGE_{0}_{1}_{2}_{3}.json".format(device_id, esn, config_id, int(current_dt.timestamp()))
    file_key = "ConvertedFiles/{0}/{1}/{2}/{3}/{4}/{5}".format(esn, device_id, current_dt.year, current_dt.month, current_dt.day, file_name)
    send_to_s3_response = s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(body).encode())
    print("Send to S3 Response: ", send_to_s3_response)
