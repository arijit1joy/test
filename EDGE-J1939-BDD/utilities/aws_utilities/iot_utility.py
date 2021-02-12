"""
    This file contains all of the functions that directly interact with AWS IOT.
"""

import json
import boto3

from utilities.common_utility import exception_handler


@exception_handler
def publish_to_mqtt_topic(topic, payload, region):
    IOT_DATA_CLIENT = boto3.client('iot-data', region_name=region)  # noqa
    publish_response = IOT_DATA_CLIENT.publish(topic=topic, qos=1, payload=json.dumps(payload))
    return publish_response['ResponseMetadata']['HTTPStatusCode']


@exception_handler
def get_thing_shadow(thing_name, shadow_name, region):
    IOT_DATA_CLIENT = boto3.client('iot-data', region_name=region)  # noqa
    get_shadow_response = IOT_DATA_CLIENT.get_thing_shadow(thingName=thing_name, shadowName=shadow_name)
    return get_shadow_response['ResponseMetadata']['HTTPStatusCode']


@exception_handler
def iot_delete_job(job_id, region):
    IOT_CLIENT = boto3.client('iot', region_name=region)  # noqa
    iot_response = IOT_CLIENT.delete_job(jobId=job_id, force=True)
    if 200 in iot_response:  # If deletion succeeded, return AWS Response message
        return iot_response
    else:
        return False


@exception_handler
def iot_describe_job(job_id, region):
    IOT_CLIENT = boto3.client('iot', region_name=region)  # noqa
    return IOT_CLIENT.describe_job(jobId=job_id)
