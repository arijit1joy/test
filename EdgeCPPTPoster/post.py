import json
import os
import traceback

import boto3
import edge_logger as logging
from sqs_utility import sqs_send_message

import pt_poster

logger = logging.logging_framework("EdgeCPPTPoster.Post")

s3_resource = boto3.resource('s3')  # noqa
CDPTJ1939PostURL = os.environ["CDPTJ1939PostURL"]
CDPTJ1939Header = os.environ["CDPTJ1939Header"]
edgeCommonAPIURL = os.environ['edgeCommonAPIURL']


def check_endpoint_file_exists(endpoint_bucket, endpoint_file):
    logger.info(f"Checking if endpoint file: '{endpoint_file}' exists in the bucket: '{endpoint_bucket}'...")
    return False


def get_cspec_req_id(sc_number):
    if '-' in sc_number:
        config_spec_name = ''.join(sc_number.split('-')[:-1])
        req_id = sc_number.split('-')[-1]
    else:
        config_spec_name = sc_number
        req_id = None
    return config_spec_name, req_id


def send_to_cd(bucket_name, key, json_format, client, j1939_type, endpoint_bucket, endpoint_file, use_endpoint_bucket,
               json_body, uuid, sqs_message):
    logger.info(f"Received CD file for posting!")

    ngdi_key = key.replace("ConvertedFiles", "NGDI")

    if json_format.lower() == "sdk":
        logger.info(f"Posting to the bucket: '{bucket_name}' with key: '{ngdi_key}', "
                    f"for further processing to the CD Pipeline...")

        try:
            post_to_ngdi_response = client.put_object(Bucket=bucket_name, Key=ngdi_key,
                                                      Body=json.dumps(json_body).encode(),
                                                      Metadata={'j1939type': j1939_type, 'uuid': uuid})

            sqs_send_message(os.environ["metaWriteQueueUrl"], sqs_message, edgeCommonAPIURL)

            logger.info(f"Post CD File to NGDI Folder Response:{post_to_ngdi_response}")
        except Exception as e:

            logger.error(f"ERROR! An Exception occurred while posting the file to the NGDI folder: {e} --> Traceback:")
            traceback.print_exc()  # Printing the Stack Trace

    elif json_format.lower() == "ngdi":

        if use_endpoint_bucket.lower() == "y":

            # TODO - This functionality is not in use now, but may be used in the future
            endpoint_file_exists = check_endpoint_file_exists(endpoint_bucket, endpoint_file)
            logger.info(endpoint_file_exists)

        else:
            sqs_message = sqs_message.replace("CD_PT_POSTED", "FILE_SENT")
            pt_poster.send_to_pt(CDPTJ1939PostURL, CDPTJ1939Header, json_body, sqs_message)
