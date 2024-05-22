import sys
import os
import boto3

sys.path.insert(1, './lib')
import utility as util

LOGGER = util.get_logger(__name__)
emission_bucket_name = os.environ["j1939_emission_end_bucket"]


def get_content(fileId):
    try:
        s3 = boto3.client('s3')
        # Read the CSV file from S3
        response = s3.get_object(Bucket=emission_bucket_name, Key=fileId)
        content = response['Body'].read().decode('utf-8')
        LOGGER.info("file content retrieved from S3 bucket")
        return content
    except Exception as e:
        LOGGER.error(f"An error occurred while getting content from S3 bucket: {e}")