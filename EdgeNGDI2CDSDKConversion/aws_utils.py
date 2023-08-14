import boto3
import json
import os

spn_bucket = os.getenv('spn_parameter_json_object')
spn_bucket_key = os.getenv('spn_parameter_json_object_key')


def _fetch_spn_file():
    s3_client = boto3.client('s3')

    spn_file_stream = s3_client.get_object(Bucket=spn_bucket, Key=spn_bucket_key)
    spn_file = spn_file_stream['Body'].read()
    _spn_file_json = json.loads(spn_file)
    return _spn_file_json


spn_file_json = _fetch_spn_file()
