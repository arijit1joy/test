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


def push_to_tsb(payload):
    secret_client = boto3.session.Session().client(service_name='secretsmanager', region_name=region)
    secret_value = secret_client.get_secret_value(SecretId=secret_name)
    api_key = secret_value['SecretString']
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json', 'x-api-key': api_key}
    LOGGER.info(f"Payload: {payload}")
    LOGGER.info(f"Headers: {headers}")
    # Send request to AAI Cloud
    response_datahub_topic = requests.post(url=tsb_url, json=payload, headers=headers)
    if response_datahub_topic.status_code == 200:
        LOGGER.info("Message successfully send to TSB")
    else:
        LOGGER.error(f"Error while posting replay message to TSB: {response_datahub_topic.status_code}")

