import json
import requests
import os
import boto3
import environment_params as env
import post
import pt_poster
import uuid
from sqs_utility import sqs_send_message
from multiprocessing import Process
import bdd_utility
from system_variables import InternalResponse
from update_scheduler import update_scheduler_table, get_request_id_from_consumption_view
from pypika import Query, Table
import edge_logger as logging

logger = logging.logging_framework("EdgeCPPTPoster.PosterLambda")

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
mapTspFromOwner = os.environ["mapTspFromOwner"]
data_quality_lambda = os.environ["DataQualityLambda"]
s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')


def delete_message_from_sqs_queue(receipt_handle):
    queue_url = os.environ["QueueUrl"]
    sqs_client = boto3.client('sqs')  # noqa
    sqs_message_deletion_response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    logger.info(f"SQS message deletion response: '{sqs_message_deletion_response}'. . .")
    return sqs_message_deletion_response


def get_device_info(device_id):
    headers = {'Content-Type': 'application/json', 'x-api-key': currentProductAPIKey}

    payload = env.get_dev_info_payload

    payload["input"]["Params"][0]["devId"] = device_id

    logger.info(f"Retrieving the device details from the EDGE DB...")

    try:

        response = requests.post(url=edgeCommonAPIURL, json=payload, headers=headers)

        logger.info(f"Get device info response as text:{response.text}")

        get_device_info_body = response.json()
        get_device_info_code = response.status_code
        logger.info(
            f"Get device info response code: {get_device_info_code} Get device info response body: {get_device_info_body}")

        if get_device_info_code == 200 and get_device_info_body:

            get_device_info_body = get_device_info_body[0]

            logger.info(f"Get device info response body JSON: {get_device_info_body}")

            return get_device_info_body

        else:

            logger.error(f"An error occurred while trying to retrieve the device's details. Check EDGE common DB logs.")
            return False

    except Exception as e:

        logger.error(f"An exception occurred while retrieving the device details: {e}")

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
    records = event.get("Records", [])
    processes = []
    logger.debug(f"Received SQS Records: {records}.")
    for record in records:
        s3_event_body = json.loads(record["body"])
        receipt_handle = record["receiptHandle"]
        # Retrieve the uploaded file from the s3 bucket and process the uploaded file
        process = Process(target=retrieve_and_process_file, args=(s3_event_body, receipt_handle))

        # Make a list of all process to wait and terminate at the end
        processes.append(process)

        # Start process
        process.start()

    # Make sure that all processes have finished
    for process in processes:
        process.join()


def retrieve_and_process_file(s3_event_body, receipt_handle):
    event_json = json.dumps(s3_event_body)

    # Invoke data quality lambda - start
    try:
        data_quality(event_json)
    except Exception as e:
        logger.info(f"ERROR Invoking data quality - {e}")
    # Invoke data quality lambda - end

    hb_uuid = str(uuid.uuid4())
    bucket_name = s3_event_body['Records'][0]['s3']['bucket']['name']
    file_key = s3_event_body['Records'][0]['s3']['object']['key']
    file_size = s3_event_body['Records'][0]['s3']['object']['size']
    logger.info(f"Bucket Name: {bucket_name} File Key: {file_key}")
    file_key = file_key.replace("%3A", ":")

    logger.info(f"New FileKey: {file_key}")

    file_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)

    logger.info(f"Get File Object Response: {file_object}")

    file_date_time = str(file_object['LastModified'])[:19]

    file_object_stream = file_object['Body'].read()

    json_body = json.loads(file_object_stream)

    logger.info(f"File as JSON: {json_body}")

    logger.info(f"Checking if this is HB or FC...")

    file_metadata = file_object["Metadata"]

    logger.info(f"File Metadata: {file_metadata}")

    j1939_type = file_metadata["j1939type"] if "j1939type" in file_metadata else 'HB'

    fc_uuid = file_metadata["uuid"] if "uuid" in file_metadata else None

    logger.info(f"FC or HB: {j1939_type}")

    device_id = json_body["telematicsDeviceId"] if "telematicsDeviceId" in json_body else None

    esn = json_body['componentSerialNumber'] if 'componentSerialNumber' in json_body else None
    file_name = file_key.split('/')[-1]

    if j1939_type.lower() == 'hb':
        config_spec_name, req_id = post.get_cspec_req_id(json_body['dataSamplingConfigId'])
        data_config_filename = '_'.join(['EDGE', device_id, esn, config_spec_name])
        request_id = get_request_id_from_consumption_view('J1939_HB', data_config_filename)
        sqs_message = str(hb_uuid) + "," + str(device_id) + "," + str(file_name) + "," + str(file_size) + "," + str(
            file_date_time) + "," + str('J1939_HB') + "," + str('FILE_RECEIVED') + "," + str(esn) + "," + str(
            config_spec_name) + "," + str(request_id) + "," + str(None) + "," + " " + "," + " "
        sqs_send_message(os.environ["metaWriteQueueUrl"], sqs_message)
        # Updating scheduler lambda based on the request_id
        if request_id:
            update_scheduler_table(request_id, device_id)

    logger.info(f"Device ID sending the file: {device_id}")

    device_info = get_device_info(device_id)

    if device_info:
        device_owner = device_info["device_owner"] if "device_owner" in device_info else None
        dom = device_info["dom"] if "dom" in device_info else None

        logger.info(f"device_owner: {device_owner} dom: {dom}")

        # Get Cust Ref, VIN, EquipmentID from EDGEDB and update in the json before posting to CD and PT
        if "cust_ref" in device_info:
            json_body['customerReference'] = device_info["cust_ref"]
        if "equip_id" in device_info:
            json_body['equipmentId'] = device_info["equip_id"]
        if "vin" in device_info:
            json_body['vin'] = device_info["vin"]
        if "telematicsPartnerName" not in json_body or not json_body["telematicsPartnerName"]:
            logger.info(f"TSP is missing in the payload, retrieving it . . .")
            tsp_owners = json.loads(mapTspFromOwner)
            tsp_name = tsp_owners[device_owner] if device_owner in tsp_owners else None
            if tsp_name:
                json_body["telematicsPartnerName"] = tsp_name
            else:
                logger.error(f"Error! Could not retrieve TSP. This is mandatory field!")
                return

        if device_owner in json.loads(os.environ["cd_device_owners"]):

            config_spec_name, req_id = post.get_cspec_req_id(json_body['dataSamplingConfigId'])

            logger.info(f"After Update json : {json_body}")
            post.send_to_cd(bucket_name, file_key, file_size, file_date_time, JSONFormat, s3_client,
                            j1939_type, fc_uuid, EndpointBucket, endpointFile, UseEndpointBucket, json_body,
                            config_spec_name, req_id, device_id, esn, hb_uuid)

        elif device_owner in json.loads(os.environ["psbu_device_owner"]):

            parameter = ssm_client.get_parameter(Name='da-edge-j1939-content-spec-value', WithDecryption=False)
            logger.info(f"{parameter}")
            config_spec_value = json.loads(parameter['Parameter']['Value'])
            if j1939_type.lower() == 'fc':
                json_body['dataSamplingConfigId'] = config_spec_value['FC']
            else:
                json_body['dataSamplingConfigId'] = config_spec_value['Periodic']

            json_body['telematicsPartnerName'] = config_spec_value['PT_TSP']

            logger.info(f"Json_body before calling SEND_TO_PT function: {json_body}")

            pt_poster.send_to_pt(PTJ1939PostURL, PTJ1939Header, json_body)
        else:

            logger.error(f"Error! The boxApplication value is not recorded in the EDGE DB!")
            bdd_utility.update_bdd_parameter(InternalResponse.J1939BDDPSBUDeviceInfoError.value)
            return

    else:
        logger.error(f"ERROR! The device_info value is missing for the device: {device_info}")
        bdd_utility.update_bdd_parameter(InternalResponse.J1939BDDDeviceInfoError.value)
        return
    delete_message_from_sqs_queue(receipt_handle)


'''
invoke content spec association API
'''


def data_quality(event):
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName=data_quality_lambda,
        InvocationType='Event',
        Payload=event
    )
    if response['StatusCode'] != 202:
        raise Exception
    print("Data Quality invoked")


# Local Test Main


if __name__ == '__main__':
    lambda_handler("", "")
