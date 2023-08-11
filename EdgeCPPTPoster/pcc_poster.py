import os
import boto3
import json
import utility as util
from pt_poster import handle_hb_params, store_device_health_params
from edge_sqs_utility_layer.sqs_utility import sqs_send_message
import datetime

LOGGER = util.get_logger(__name__)

edgeCommonAPIURL = os.environ['edgeCommonAPIURL']

PCC_ROLE_ARN = os.environ["pcc_role_arn"]
J19139_STREAM_ARN = os.environ["j1939_stream_arn"]
PCC_REGION = os.environ["pcc_region"]


def send_to_pcc(json_body, device_id, j1939_data_type, sqs_message_template):
    partition_key = str(device_id) + '-' + j1939_data_type
    LOGGER.info(f"Partition key for device_id {device_id} with data type {j1939_data_type} is {partition_key}")
    try:
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
                    device_health_params = handle_hb_params(converted_device_params, False)
                    if device_health_params:
                        sample["convertedDeviceParameters"] = device_health_params
                    else:
                        sample.pop("convertedDeviceParameters")
        sts_connection = boto3.client('sts')
        LOGGER.info('Getting STS credentials')
        sts_credentials = sts_connection.assume_role(
            RoleArn=PCC_ROLE_ARN,
            RoleSessionName='PCC_J1939_KinesisSession'
        )
        access_key = sts_credentials['Credentials']['AccessKeyId']
        secret_key = sts_credentials['Credentials']['SecretAccessKey']
        session_token = sts_credentials['Credentials']['SessionToken']
        LOGGER.debug('Successfully retrieved STS credentials')
        kinesis = boto3.client('kinesis', aws_access_key_id=access_key,
                               aws_secret_access_key=secret_key,
                               aws_session_token=session_token,
                               region_name=PCC_REGION)
        payload = json.dumps(json_body, indent=2).encode('utf-8')
        LOGGER.info(f"Kinesis Request payload: {payload}")
        kinesis_response = kinesis.put_record(
            StreamARN=J19139_STREAM_ARN,
            Data=payload,
            PartitionKey=partition_key)
        LOGGER.info(f"Kinesis Response: {kinesis_response}")
        current_dt = datetime.datetime.now()

        file_sent_sqs_message = sqs_message_template.replace("{FILE_METADATA_FILE_STAGE}", "FILE_SENT")
        file_sent_sqs_message = sqs_message_template.replace("{FILE_METADATA_CURRENT_DATE_TIME}", current_dt.strftime('%Y-%m-%d %H:%M:%S'))

        sqs_send_message(os.environ["metaWriteQueueUrl"], file_sent_sqs_message, edgeCommonAPIURL)
        return kinesis_response
    except Exception as kinesis_streaming_exception:
        error_message = f"An Error Occurred while Streaming Data to Kinesis: {kinesis_streaming_exception}"
        LOGGER.error(error_message)
        util.write_to_audit_table(j1939_data_type, error_message, json_body['telematicsDeviceId'])


def handle_fc_params(converted_fc_params):
    for fc_param in converted_fc_params:
        if "activeFaultCodes" in fc_param:
            for afc in fc_param["activeFaultCodes"]:
                if "count" in afc:
                    afc["occurenceCount"] = str(afc["count"])
                    afc.pop("count")
        if "inactiveFaultCodes" in fc_param:
            for ifc in fc_param["inactiveFaultCodes"]:
                if "count" in ifc:
                    ifc["occurenceCount"] = str(ifc["count"])
                    ifc.pop("count")
        if "pendingFaultCodes" in fc_param:
            for pfc in fc_param["pendingFaultCodes"]:
                if "count" in pfc:
                    pfc["occurenceCount"] = str(pfc["count"])
                    pfc.pop("count")
    LOGGER.debug(f"Converted FC Params: {converted_fc_params}")
    return converted_fc_params
