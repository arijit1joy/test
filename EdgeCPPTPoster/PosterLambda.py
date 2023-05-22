import os
import json
import time
import uuid
import boto3
import requests
import post
import pt_poster
import utility as util
import environment_params as env
from multiprocessing import Process
from sqs_utility import sqs_send_message
import sys
from update_scheduler import update_scheduler_table, get_request_id_from_consumption_view
sys.path.insert(1, './lib')
from edge_db_lambda_client import EdgeDbLambdaClient

LOGGER = util.get_logger(__name__)

# Retrieve the environment variables
edgeCommonAPIURL = os.environ['edgeCommonAPIURL']
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
process_data_quality = os.environ["ProcessDataQuality"]
data_quality_lambda = os.environ["DataQualityLambda"]
MAX_ATTEMPTS = int(os.environ["MaxAttempts"])
s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')
EDGE_DB_CLIENT = EdgeDbLambdaClient()


def delete_message_from_sqs_queue(receipt_handle):
    queue_url = os.environ["QueueUrl"]
    sqs_client = boto3.client('sqs')  # noqa
    sqs_message_deletion_response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    LOGGER.debug(f"SQS message deletion response: '{sqs_message_deletion_response}'. . .")
    return sqs_message_deletion_response


def get_device_info(device_id):
    payload = env.get_dev_info_payload["query"]
    payload = payload.replace("%(devId)s", f"'{device_id}'")  # We format directly because we need a query string

    attempts = 0

    LOGGER.debug(f"Retrieving the device details from the EDGE DB for Device ID: {device_id}")

    try:
        while attempts < MAX_ATTEMPTS:
            time.sleep(2 * attempts / 10)  # Sleep for 200 ms exponentially
            get_device_info_body = EDGE_DB_CLIENT.execute(payload)  # This will return an object
            attempts += 1
            LOGGER.debug(f"Returned device info body: {get_device_info_body}")
            if get_device_info_body:
                get_device_info_body = get_device_info_body[0]
                return get_device_info_body
        LOGGER.error(f"An error occurred while trying to retrieve the device's details. Check EDGEDBReader logs.")
        return False
    except Exception as e:
        LOGGER.error(f"An exception occurred while retrieving the device details: {e}")
        return False


def get_business_partner(device_type):
    if device_type.lower() == EBUSpecifier:
        return "EBU"
    elif device_type.lower() == PSBUSpecifier:
        return "PSBU"
    else:
        return False


def retrieve_and_process_file(s3_event_body, receipt_handle):
    event_json = json.dumps(s3_event_body)

    if process_data_quality.lower() == 'yes':
        LOGGER.debug("Initiating data quality...")
        # Invoke data quality lambda - start
        try:
            data_quality(event_json)
        except Exception as e:
            LOGGER.error(f"ERROR Invoking data quality - {e}")
        # Invoke data quality lambda - end
    else:
        LOGGER.debug("data quality skipped...")

    bucket_name = s3_event_body['Records'][0]['s3']['bucket']['name']
    file_key = s3_event_body['Records'][0]['s3']['object']['key']
    file_size = s3_event_body['Records'][0]['s3']['object']['size']
    LOGGER.debug(f"Bucket Name: {bucket_name} File Key: {file_key}")
    file_key = file_key.replace("%3A", ":")
    LOGGER.info(f"New FileKey: {file_key}")

    file_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    LOGGER.debug(f"Get File Object Response: {file_object}")

    file_date_time = str(file_object['LastModified'])[:19]
    file_object_stream = file_object['Body'].read()
    json_body = json.loads(file_object_stream)
    LOGGER.debug(f"File as JSON: {json_body}")

    file_metadata = file_object["Metadata"]
    LOGGER.debug(f"File Metadata: {file_metadata}")

    j1939_type = file_metadata["j1939type"] if "j1939type" in file_metadata else 'HB'

    # If the file contains a UUID, then use it moving forward else:
    # Set the UUID to None for FC and get a new UUID for HB
    fc_uuid = file_metadata["uuid"] if "uuid" in file_metadata else None
    hb_uuid = file_metadata["uuid"] if "uuid" in file_metadata else str(uuid.uuid4())

    LOGGER.info(f"FC or HB: {j1939_type}")

    device_id = json_body["telematicsDeviceId"] if "telematicsDeviceId" in json_body else None
    esn = json_body['componentSerialNumber'] if 'componentSerialNumber' in json_body else None
    # Please note that the order is expected to be <Make>*<Model>***<ESN>**** for Improper PSBU ESN
    if esn and "*" in esn:
        esn = [esn_component for esn_component in esn.split("*") if esn_component][-1]

    file_name = file_key.split('/')[-1]

    if j1939_type.lower() == "fc":
        config_spec_name_fc, req_id_fc = post.get_cspec_req_id(file_key.split('_')[3])
        config_spec_and_req_id = str(config_spec_name_fc) + "," + str(req_id_fc)
        j1939_data_type = 'J1939_FC'
        file_uuid = fc_uuid
    elif j1939_type.lower() == 'hb':
        j1939_data_type = 'J1939_HB'
        config_spec_name, req_id = post.get_cspec_req_id(json_body['dataSamplingConfigId'])
        data_config_filename = '_'.join(['EDGE', device_id, esn, config_spec_name])
        request_id = get_request_id_from_consumption_view('J1939_HB', data_config_filename)
        config_spec_and_req_id = str(config_spec_name) + "," + str(request_id)
        file_uuid = hb_uuid

        # Updating scheduler lambda based on the request_id
        if request_id:
            update_scheduler_table(request_id, device_id)
    else:
        raise RuntimeError(f"Invalid 'j1939type': '{j1939_type}' received! "
                           "The 'j1939type' S3 object metadata for FC files should be 'FC'!")

    sqs_message = file_uuid + "," + str(device_id) + "," + str(file_name) + "," + str(file_size) + "," + str(
       file_date_time) + "," + str(j1939_data_type) + "," + str('FILE_RECEIVED') + "," + str(
        esn) + "," + config_spec_and_req_id + "," + str(None) + "," + " " + "," + " "
    
    sqs_message_template = \
                            f"{file_uuid},{device_id},{file_name},{str(file_size)}," \
                            f"{'{FILE_METADATA_CURRENT_DATE_TIME}'},{j1939_data_type}," \
                            f"{'{FILE_METADATA_FILE_STAGE}'},{esn},{config_spec_and_req_id},,,,"

    

    
    if j1939_type.lower() == 'hb':
        #current_dt = datetime.now()

        file_received_sqs_message = sqs_message_template \
            .replace("{FILE_METADATA_CURRENT_DATE_TIME}",str(file_date_time)) \
            .replace("{FILE_METADATA_FILE_STAGE}", "FILE_RECEIVED")
        # fielsent and fildatetime
        LOGGER.debug(f"Sending Metadata message for HB with: {file_received_sqs_message}")
        sqs_send_message(os.environ["metaWriteQueueUrl"], file_received_sqs_message, edgeCommonAPIURL)

    device_info = get_device_info(device_id)  # type: dict

    if device_info:
        device_owner = device_info["device_owner"] if "device_owner" in device_info else None

        # Get Cust Ref, VIN, EquipmentID from EDGEDB and update in the json before posting to CD and PT
        if "cust_ref" in device_info:
            json_body['customerReference'] = device_info["cust_ref"]
        if "equip_id" in device_info:
            json_body['equipmentId'] = device_info["equip_id"]
        if "vin" in device_info:
            json_body['vin'] = device_info["vin"]
        if "telematicsPartnerName" not in json_body or not json_body["telematicsPartnerName"]:
            LOGGER.info(f"TSP is missing in the payload, retrieving it . . .")
            tsp_owners = json.loads(mapTspFromOwner)
            tsp_name = tsp_owners[device_owner] if device_owner in tsp_owners else None
            if tsp_name:
                json_body["telematicsPartnerName"] = tsp_name
            else:
                LOGGER.error(f"Error! Could not retrieve TSP. This is mandatory field!")
                return

        if device_owner in json.loads(os.environ["cd_device_owners"]):
            sqs_message = sqs_message.replace("FILE_RECEIVED", "CD_PT_POSTED")
            LOGGER.debug(f"Metadata Message sent to CD: {sqs_message}")
            post.send_to_cd(bucket_name, file_key, JSONFormat, s3_client, j1939_type, EndpointBucket, endpointFile,
                            UseEndpointBucket, json_body, file_uuid, sqs_message, j1939_data_type)

        elif device_owner in json.loads(os.environ["psbu_device_owner"]):
            parameter = ssm_client.get_parameter(Name='da-edge-j1939-content-spec-value', WithDecryption=False)
            config_spec_value = json.loads(parameter['Parameter']['Value'])
            if j1939_type.lower() == 'fc':
                json_body['dataSamplingConfigId'] = config_spec_value['FC']
            else:
                json_body['dataSamplingConfigId'] = config_spec_value['Periodic']

            json_body['telematicsPartnerName'] = config_spec_value['PT_TSP']

            LOGGER.debug(f"Json_body before calling SEND_TO_PT function: {json_body}")
            sqs_message = sqs_message.replace("FILE_RECEIVED", "FILE_SENT")
            LOGGER.debug(f"Metadata Message sent to PT: {sqs_message}")
            pt_poster.send_to_pt(PTJ1939PostURL, PTJ1939Header, json_body, sqs_message, j1939_data_type, j1939_type.lower(),file_uuid,device_id,esn)
        else:
            error_message = f"The boxApplication value is not recorded in the EDGE DB for the device: {device_id}"
            LOGGER.error(error_message)
            util.write_to_audit_table(j1939_data_type, error_message, device_id)
            return

    else:
        error_message = f"The device_info value is missing for the device: {device_id}"
        LOGGER.error(error_message)
        util.write_to_audit_table(j1939_data_type, error_message, device_id)
        return
    delete_message_from_sqs_queue(receipt_handle)


# Invoke the Data Quality Lambda
def data_quality(event):
    lambda_client = boto3.client('lambda')  # noqa
    response = lambda_client.invoke(
        FunctionName=data_quality_lambda,
        InvocationType='Event',
        Payload=event
    )

    if response['StatusCode'] != 202:
        raise RuntimeError("An error occurred while invoking the data quality lambda")

    LOGGER.debug("Successfully invoked the Data Quality lambda!")


def lambda_handler(event, context):  # noqa
    records = event.get("Records", [])
    processes = []
    LOGGER.debug(f"Received SQS Records: {records}.")
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
