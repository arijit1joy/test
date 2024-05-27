from uuid import uuid4

import boto3
import os
import sys
import requests

sys.path.insert(1, './lib')
import utility as util

LOGGER = util.get_logger(__name__)
tsb_url = os.environ["tsb_url"]
region = os.environ["region"]
secret_name = os.environ["secret_name"]


def push_to_tsb(message):
    secret_client = boto3.session.Session().client(service_name='secretsmanager', region_name=region)
    secret_value = secret_client.get_secret_value(SecretId=secret_name)
    api_key = secret_value['SecretString']
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json', 'x-api-key': api_key}
    uuid = str(uuid4())
    device_id = message["telematicsDeviceId"]
    esn = message["componentSerialNumber"]
    file_name = 'NULL'
    file_size = 'NULL'
    SQS_TEMPLATE = "[message_id],[device_id],[file_name],[file_size],[file_datetime],AAI-EMISSION,[file_status],[esn],,,,,"
    file_sent_sqs_message = formatter(SQS_TEMPLATE,
                                          message_id=uuid,
                                          device_id=device_id,
                                          esn=esn,
                                          file_name=file_name,
                                          file_size=file_size,
                                          file_status="Posted",
                                          file_datetime='{FILE_METADATA_CURRENT_DATE_TIME}'
                                        )
    payload = {"records": [{"value": {
        "metadata": {"messageID": uuid, "deviceID": device_id, "esn": esn,
                     "bu": "EBU",
                     "topic": "aai-emission", "fileType": 'JSON',
                     "fileSentSQSMessage": file_sent_sqs_message},
        "data": message}}]}
    LOGGER.info(f"Payload: {payload}")
    LOGGER.info(f"Headers: {headers}")
    # Send request to AAI Cloud
    response_datahub_topic = requests.post(url=tsb_url, json=payload, headers=headers)
    if response_datahub_topic.status_code == 200:
        LOGGER.info("Message successfully send to TSB")
    else:
        LOGGER.error(
            f"Error while posting message to TSB : {response_datahub_topic.status_code} & {response_datahub_topic.text}")
        raise Exception("Error while posting message to TSB")


def formatter(base_str, **kwargs):
    for k, v in kwargs.items():
        base_str = base_str.replace(f"[{k}]", str(v))
    return base_str