import json
import os
import traceback

import boto3
import edge_logger as logging
from sqs_utility import sqs_send_message

logger = logging.logging_framework("EdgeCPPTPoster.Post")

s3_resource = boto3.resource('s3')  # noqa
CDPTJ1939PostURL = os.environ["CDPTJ1939PostURL"]
CDPTJ1939Header = os.environ["CDPTJ1939Header"]


def get_config_spec_req_id(sc_number):
    if '-' in sc_number:
        config_spec_name = ''.join(sc_number.split('-')[:-1])
        req_id = sc_number.split('-')[-1]
    else:
        config_spec_name = sc_number
        req_id = None
    return config_spec_name, req_id


def send_to_cd(bucket_name, key, client, j1939_type, fc_uuid, json_body, hb_uuid, sqs_message):
    logger.info(f"Received CD file for posting!")

    logger.info(f"Posting to the NGDI folder for posting to CD Pipeline...")

    try:

        if j1939_type.lower() == "fc":

            post_to_ngdi_response = client.put_object(Bucket=bucket_name,
                                                      Key=key.replace("ConvertedFiles", "NGDI"),
                                                      Body=json.dumps(json_body).encode(),
                                                      Metadata={'j1939type': j1939_type, 'uuid': fc_uuid})

            sqs_send_message(os.environ["metaWriteQueueUrl"], sqs_message)

        else:

            post_to_ngdi_response = client.put_object(Bucket=bucket_name, Key=key.replace("ConvertedFiles", "NGDI"),
                                                      Body=json.dumps(json_body).encode(),
                                                      Metadata={'j1939type': j1939_type,
                                                                'uuid': hb_uuid})

            sqs_send_message(os.environ["metaWriteQueueUrl"], sqs_message)

        logger.info(f"Post CD File to NGDI Folder Response:{post_to_ngdi_response}")

    except Exception as e:

        logger.error(f"ERROR! An Exception occurred while posting the file to the NGDI folder: {e} --> Traceback:")
        traceback.print_exc()  # Printing the Stack Trace)
