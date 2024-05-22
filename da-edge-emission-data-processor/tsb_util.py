import json
import os
import sys
import requests

sys.path.insert(1, './lib')
import utility as util

LOGGER = util.get_logger(__name__)
tsb_url = os.environ["tsb_url"]


def push_to_tsb(payload):
    try:
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        LOGGER.info(f"Payload: {payload}")
        LOGGER.info(f"Headers: {headers}")
        # Send request to AAI Cloud
        response_datahub_topic = requests.post(url=tsb_url, json=payload, headers=headers)
        if response_datahub_topic.status_code == 200:
            LOGGER.info("Message successfully send to TSB")
        else:
            LOGGER.error(f"Error while posting replay message to TSB: {response_datahub_topic.status_code}")
    except Exception as e:
        LOGGER.error(f"An error occurred while posting to TSB: {e}")