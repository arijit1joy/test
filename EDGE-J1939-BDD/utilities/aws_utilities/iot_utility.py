"""
    This file contains all of the functions that directly interact with AWS IOT.
"""

import json
import boto3

from utilities.common_utility import exception_handler

# TODO take region_name from os environ (Need to add "region" environment variable in buildspec/code build)
IOT_DATA_CLIENT = boto3.client('iot-data', region_name='us-east-1')  # noqa
IOT_CLIENT = boto3.client('iot', region_name='us-east-1')  # noqa


@exception_handler
def publish_to_mqtt_topic(topic, payload):
    publish_response = IOT_DATA_CLIENT.publish(topic=topic, qos=1, payload=json.dumps(payload))
    return publish_response['ResponseMetadata']['HTTPStatusCode']


@exception_handler
def get_thing_shadow(thing_name, shadow_name):
    get_shadow_response = IOT_DATA_CLIENT.get_thing_shadow(thingName=thing_name, shadowName=shadow_name)
    return get_shadow_response['ResponseMetadata']['HTTPStatusCode']


@exception_handler
def iot_delete_job(job_id):
    iot_response = IOT_CLIENT.delete_job(jobId=job_id, force=True)
    if 200 in iot_response:  # If deletion succeeded, return AWS Response message
        return iot_response
    else:
        return False


@exception_handler
def iot_describe_job(job_id):
    return IOT_CLIENT.describe_job(jobId=job_id)
