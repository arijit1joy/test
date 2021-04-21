import json
import pt_poster
import os
import boto3
import traceback
import edge_logger as logging
from sqs_utility import sqs_send_message

logger = logging.logging_framework("EdgeCPPTPoster.Post")

s3_resource = boto3.resource('s3')
CDPTJ1939PostURL = os.environ["CDPTJ1939PostURL"]
CDPTJ1939Header = os.environ["CDPTJ1939Header"]


def check_endpoint_file_exists(endpoint_bucket, endpoint_file):
    logger.info(f"Checking if endpoint file exists...")


def get_cspec_req_id(sc_number):
    if '-' in sc_number:
        config_spec_name = ''.join(sc_number.split('-')[:-1])
        req_id = sc_number.split('-')[-1]
    else:
        config_spec_name = sc_number
        req_id = None
    return config_spec_name, req_id


def send_to_cd(bucket_name, key, file_size, file_date_time, json_format, client, j1939_type, fc_uuid, endpoint_bucket,
               endpoint_file, use_endpoint_bucket, json_body, config_spec_name, req_id, device_id, esn, hb_uuid,
               sqs_message):
    logger.info(f"Received CD file for posting!")

    file_key = key.split('/')[-1]
    if json_format.lower() == "sdk":
        logger.info(f"Posting to the NGDI folder for posting to CD Pipeline...")

        try:

            if j1939_type.lower() == "fc":

                config_spec_name_fc, req_id_fc = get_cspec_req_id(key.split('_')[3])

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

    elif json_format.lower() == "ngdi":

        if use_endpoint_bucket.lower() == "y":

            endpoint_file_exists = check_endpoint_file_exists(endpoint_bucket, endpoint_file)

        else:
            sqs_message = sqs_message.replace("CD_PT_POSTED", "FILE_SENT")
            pt_poster.send_to_pt(CDPTJ1939PostURL, CDPTJ1939Header, json_body, sqs_message)
