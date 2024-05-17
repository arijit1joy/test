import sys
import json
import os
import boto3

sys.path.insert(1, './lib')
import utility as util
from db_util import update_metadata_Table

LOGGER = util.get_logger(__name__)
emission_bucket_name = os.environ["j1939_emission_end_bucket"]

def lambda_handler(event, context):  # noqa
    try:
        LOGGER.info(f"Event posted to Emission Data Processor lambda function is: {event}")
        for record in event['Records']:
            s3_body = json.loads(record['body'])
            LOGGER.info(f"s3_body: {s3_body}")
            for s3_record in s3_body['Records']:
                key = s3_record['s3']['object']['key']
                LOGGER.info(f"fileId: {key}")
                # download file in s3 bucket
                content = getContent(key)
                content_json = json.loads(content)
                # Send to AAI Cloud

                # Update metadata table to FILE_PROCESSED
                update_metadata_Table(content_json['telematicsDeviceId'], content_json['componentSerialNumber'], content_json['dataSamplingConfigId'])
    except Exception as e:
        LOGGER.error(f"An error occurred while processing Emission data: {e}")


def getContent(fileId):
    try:
        s3 = boto3.client('s3')
        # Read the CSV file from S3
        response = s3.get_object(Bucket=emission_bucket_name, Key=fileId)
        content = response['Body'].read().decode('utf-8')
        LOGGER.info(f"file content retrieved from S3 bucket")
        return content
    except Exception as e:
        LOGGER.error(e)