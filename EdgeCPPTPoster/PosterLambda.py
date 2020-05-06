import json
import requests
import os
import boto3
import environment_params as env
import post
import pt_poster
import uuid
from kinesis_utility import build_metadata_and_write


# Retrieve the environment variables

edgeCommonAPIURL = os.environ['edgeCommonAPIURL']
currentProductAPIKey = os.environ['CurrentProdutCommonAPIKey']
endpointFile = os.environ["EndpointFile"]
CPPostBucket = os.environ["CPPostBucket"]
EndpointBucket = os.environ["EndpointBucket"]
JSONFormat = os.environ["JSONFormat"]
PSBUSpecifier = os.environ["PSBUSpecifier"]
EBUSpecifier = os.environ["EBUSpecifier"]
UseEndpointBucket = os.environ["UseEndpointBucket"]
PTJ1939PostURL = os.environ["PTJ1939PostURL"]
PTJ1939Header = os.environ["PTJ1939Header"]
PowerGenValue = os.environ["PowerGenValue"]

s3_client = boto3.client('s3')


def get_device_info(device_id):
    headers = {'Content-Type': 'application/json', 'x-api-key': currentProductAPIKey}

    payload = env.get_dev_info_payload

    print("Get device info payload:", payload)

    payload["input"]["Params"][0]["devId"] = device_id

    print("Retrieving the device details from the EDGE DB...")

    try:

        response = requests.post(url=edgeCommonAPIURL, json=payload, headers=headers)

        print("Get device info response as text:", response.text)

        get_device_info_body = response.json()
        get_device_info_code = response.status_code

        print("Get device info response code:", get_device_info_code)
        print("Get device info response body:", get_device_info_body)

        if get_device_info_code == 200 and get_device_info_body:

            get_device_info_body = get_device_info_body[0]

            print("Get device info response body JSON:", get_device_info_body)

            return get_device_info_body

        else:

            print("An error occurred while trying to retrieve the device's details. Check EDGE common DB logs.")
            return False

    except Exception as e:

        print("An exception occurred while retrieving the device details:", e)

        return False


def get_business_partner(device_type):

    if device_type.lower() == EBUSpecifier:

        return "EBU"

    elif device_type.lower() == PSBUSpecifier:

        return "PSBU"

    else:

        return False


def lambda_handler(event, context):
    # json_file = open("EDGE_352953080329158_64000002_SC123_20190820045303_F2BA (3).json", "r")

    print("Lambda Event:", json.dumps(event))
    hb_uuid = str(uuid.uuid4())
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    file_size = event['Records'][0]['s3']['object']['size']

    print("Bucket Name:", bucket_name)
    print("File Key:", file_key)

    file_key = file_key.replace("%3A", ":")

    print("New FileKey:", file_key)

    file_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)

    print("Get File Object Response:", file_object)

    file_date_time = str(file_object['LastModified'])[:19]

    file_object_stream = file_object['Body'].read()

    json_body = json.loads(file_object_stream)

    print("File as JSON:", json_body)

    print("Checking if this is HB or FC...")

    file_metadata = file_object["Metadata"]

    print("File Metadata:", file_metadata)

    j1939_type = file_metadata["j1939type"] if "j1939type" in file_metadata else 'HB'

    fc_uuid = file_metadata["uuid"] if "uuid" in file_metadata else None

    print("FC or HB:", j1939_type)

    device_id = json_body["telematicsDeviceId"] if "telematicsDeviceId" in json_body else None

    esn = json_body['componentSerialNumber'] if 'componentSerialNumber' in json_body else None

    if j1939_type == 'HB':
        config_spec_name, req_id = post.get_cspec_req_id(json_body['dataSamplingConfigId'])

        build_metadata_and_write(hb_uuid, device_id, file_key, file_size, file_date_time, 'J1939-HB',
                                     'FILE_RECEIVED', esn, config_spec_name, req_id)

    print("Device ID sending the file:", device_id)

    device_info = get_device_info(device_id)

    if device_info:
        hb_esn = json_body['componentSerialNumber']
        device_type = device_info["device_type"] if "device_type" in device_info else None
        device_owner = device_info["device_owner"] if "device_owner" in device_info else None
        dom = device_info["dom"] if "dom" in device_info else None

        print("device_type:", device_type)
        print("device_owner:", device_owner)
        print("dom:", dom)

        if device_type:

            business_partner = get_business_partner(device_type)

            if business_partner:

                print("This is a(n)", business_partner, "device!")

                if business_partner == "EBU":

                    config_spec_name, req_id = post.get_cspec_req_id(json_body['dataSamplingConfigId'])

                    post.send_to_cd(bucket_name, file_key, file_size, file_date_time, JSONFormat, s3_client,
                                    j1939_type, fc_uuid, EndpointBucket, endpointFile, UseEndpointBucket, json_body,
                                    config_spec_name, req_id, device_id, esn, hb_uuid)

                else:

                    if dom:

                        if dom.lower() is not PowerGenValue.lower():

                            pt_poster.send_to_pt(PTJ1939PostURL, PTJ1939Header, json_body)

                        else:

                            print("This is a PSBU device, but it is PCC, cannot send to PT")

                            return

                    else:

                        print("Error! The boxApplication value is not recorded in the EDGE DB!")

                        return

            else:

                print("Error! This is not an EBU or PSBU device")

                return

        else:

            print("ERROR! The device_type value is missing for the device:", device_id)

            return

    else:

        print("ERROR! The device_info value is missing for the device:", device_info)

        return


# Local Test Main

if __name__ == '__main__':
    lambda_handler("", "")
