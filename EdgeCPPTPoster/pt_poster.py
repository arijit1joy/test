import json
import boto3
import os
import requests
import traceback

import bdd_utility
from system_variables import InternalResponse
from obfuscate_gps_utility import deobfuscate_gps_coordinates

secret_name = os.environ['PTxAPIKey']
region_name = os.environ['Region']

# Create a Secrets Manager client
session = boto3.session.Session()
sec_client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)


def send_to_pt(post_url, headers, json_body):

    try:
        headers_json = json.loads(headers)
        get_secret_value_response = sec_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            secret = json.loads(secret)
            api_key = secret['x-api-key']
        else:
            print("PT x-api-key not exist in secret manager")
        headers_json['x-api-key'] = api_key

        # de-obfuscate GPS co-ordinates
        if "samples" in json_body:
            for sample in json_body["samples"]:
                if "convertedDeviceParameters" in sample:
                    converted_device_params = sample["convertedDeviceParameters"]
                    if "Latitude" in converted_device_params and "Longitude" in converted_device_params:
                        latitude = converted_device_params["Latitude"]
                        longitude = converted_device_params["Longitude"]
                        converted_device_params["Latitude"], converted_device_params["Longitude"] = \
                            deobfuscate_gps_coordinates(latitude, longitude)

        final_json_body = [json_body]
        print("Posting the JSON body:", final_json_body, "to the PT Cloud through URL:",
              post_url, "with headers:", headers_json)

        pt_response = requests.post(url=post_url, data=json.dumps(final_json_body), headers=headers_json)

        print("Post to PT response as text:", pt_response.text)

        pt_response_body = pt_response.json()
        pt_response_code = pt_response.status_code
        print("Post to PT response code:", pt_response_code)
        print("Post to PT response body:", pt_response_body)

        if pt_response_code != 200:
            bdd_utility.update_bdd_parameter(InternalResponse.J1939BDDPTPostSuccess.value)

    except Exception as e:
        traceback.print_exc()
        print("ERROR! An exception occurred while posting to PT endpoint:", e)