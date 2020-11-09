import json
import boto3
import os
import requests
import traceback
import datetime
import bdd_utility
from system_variables import InternalResponse
from obfuscate_gps_utility import deobfuscate_gps_coordinates
from metadata_utility import write_health_parameter_to_database

secret_name = os.environ['PTxAPIKey']
region_name = os.environ['Region']

# Create a Secrets Manager client
session = boto3.session.Session()
sec_client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)


def handle_fc_params(converted_fc_params):
    for fc_param in converted_fc_params:
        if "activeFaultCodes" in fc_param:
            for afc in fc_param["activeFaultCodes"]:
                if "count" in afc:
                    afc["occurenceCount"] = str(afc["count"])
                    afc.pop("count")
        if "inactiveFaultCodes" in fc_param:
            fc_param.pop("inactiveFaultCodes")
        if "pendingFaultCodes" in fc_param:
            fc_param.pop("pendingFaultCodes")
    print("Converted FC Params: ", converted_fc_params)
    return converted_fc_params


def handle_hb_params(converted_device_params):
    # De-obfuscate GPS co-ordinates
    if "Latitude" in converted_device_params and "Longitude" in converted_device_params:
        latitude = converted_device_params["Latitude"]
        longitude = converted_device_params["Longitude"]
        converted_device_params["Latitude"], converted_device_params["Longitude"] = \
            deobfuscate_gps_coordinates(latitude, longitude)

    # Remove unnecessary params from device parameters for PT payload
    converted_device_params = {key.lower(): value for key, value in converted_device_params.items() if
                               key in ["Latitude", "Longitude", "Altitude"]}
    print("Converted Device Params: ", converted_device_params)
    return converted_device_params


def store_device_health_params(converted_device_params, sample_time_stamp, device_id, esn):
    if 'messageID' in converted_device_params:
        message_id = converted_device_params['messageID']
        cpu_temperature = converted_device_params['CPU_temperature'] \
            if 'CPU_temperature' in converted_device_params else None
        pmic_temperature = converted_device_params['PMIC_temperature'] \
            if 'PMIC_temperature' in converted_device_params else None
        latitude = converted_device_params['Latitude'] if 'Latitude' in converted_device_params else None
        longitude = converted_device_params['Longitude'] if 'Longitude' in converted_device_params else None
        altitude = converted_device_params['Altitude'] if 'Altitude' in converted_device_params else None
        pdop = converted_device_params['PDOP'] if 'PDOP' in converted_device_params else None
        satellites_used = converted_device_params['Satellites_Used'] \
            if 'Satellites_Used' in converted_device_params else None
        lte_rssi = converted_device_params['LTE_RSSI'] if 'LTE_RSSI' in converted_device_params else None
        lte_rscp = converted_device_params['LTE_RSCP'] if 'LTE_RSCP' in converted_device_params else None
        lte_rsrq = converted_device_params['LTE_RSRQ'] if 'LTE_RSRQ' in converted_device_params else None
        lte_rsrp = converted_device_params['LTE_RSRP'] if 'LTE_RSRP' in converted_device_params else None
        cpu_usage_level = converted_device_params['CPU_Usage_Level'] \
            if 'CPU_Usage_Level' in converted_device_params else None
        ram_usage_level = converted_device_params['RAM_Usage_Level'] \
            if 'RAM_Usage_Level' in converted_device_params else None
        snr_per_satellite = converted_device_params['SNR_per_Satellite'] \
            if 'SNR_per_Satellite' in converted_device_params else None
        convert_timestamp = datetime.datetime.strptime(sample_time_stamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        new_timestamp = datetime.datetime.strftime(convert_timestamp, '%Y-%m-%d %H:%M:%S')
        write_health_parameter_to_database(message_id, cpu_temperature, pmic_temperature, latitude, longitude,
                                           altitude, pdop, satellites_used, lte_rssi, lte_rscp, lte_rsrq, lte_rsrp,
                                           cpu_usage_level, ram_usage_level, snr_per_satellite, new_timestamp,
                                           device_id, esn, os.environ["edgeCommonAPIURL"])
    else:
        print("There is no messageId in Converted Device Parameter.")


def send_to_pt(post_url, headers, json_body):
    try:
        headers_json = json.loads(headers)
        get_secret_value_response = sec_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            secret = json.loads(secret)
            api_key = secret['x-api-key']
            headers_json['x-api-key'] = api_key
        else:
            print("PT x-api-key not exist in secret manager")

        if "samples" in json_body:
            for sample in json_body["samples"]:
                if "convertedEquipmentFaultCodes" in sample:
                    converted_fc_params = sample["convertedEquipmentFaultCodes"]
                    fault_codes_params = handle_fc_params(converted_fc_params)
                    if fault_codes_params:
                        sample["convertedEquipmentFaultCodes"] = fault_codes_params
                    else:
                        sample.pop("convertedEquipmentFaultCodes")
                if "convertedDeviceParameters" in sample:
                    converted_device_params = sample["convertedDeviceParameters"]
                    store_device_health_params(converted_device_params, sample["dateTimestamp"],
                                               json_body["telematicsDeviceId"], json_body["componentSerialNumber"])
                    device_health_params = handle_hb_params(converted_device_params)
                    if device_health_params:
                        sample["convertedDeviceParameters"] = device_health_params
                    else:
                        sample.pop("convertedDeviceParameters")

        # We are not sending payload to PT for Digital Cockpit Device
        if not json_body["telematicsDeviceId"] == '357186082267036':
            final_json_body = [json_body]
            pt_response = requests.post(url=post_url, data=json.dumps(final_json_body), headers=headers_json)
            pt_response_body = pt_response.json()
            pt_response_code = pt_response.status_code
            print("Post to PT response code:", pt_response_code)
            print("Post to PT response body:", pt_response_body)

            if pt_response_code != 200:
                bdd_utility.update_bdd_parameter(InternalResponse.J1939BDDPTPostSuccess.value)

    except Exception as e:
        traceback.print_exc()
        print("ERROR! An exception occurred while posting to PT endpoint:", e)
