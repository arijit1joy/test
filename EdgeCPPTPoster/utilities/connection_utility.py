import json
import os
import utility as util
from functools import wraps

import boto3

LOGGER = util.get_logger(__name__)


def invoke_db_reader(read_query):
    edge_db_reader_name = os.environ["EDGEDBReader"]
    lambda_client = boto3.client("lambda")

    LOGGER.debug("Invoking the EDGE DB Reader . . .")

    # Invoke the EDGE DB reader lambda to execute the query and to get the response
    db_read_lambda_response = lambda_client.invoke(
        FunctionName=edge_db_reader_name, InvocationType='RequestResponse', LogType='None',
        Payload=bytes(json.dumps(read_query).encode("utf-8"))
    )

    # Decode and return the lambda's response
    return json.loads(db_read_lambda_response["Payload"].read().decode("utf-8"))


def invoke_db_common_api(write_query):
    edge_common_api_name = os.environ["EDGECommonDBAPI"]
    lambda_client = boto3.client("lambda")

    LOGGER.debug("Invoking the EDGE DB Common API . . .")

    # Invoke the EDGE DB reader lambda to execute the query and to get the response
    common_api_lambda_response = lambda_client.invoke(
        FunctionName=edge_common_api_name, InvocationType='RequestResponse', LogType='None',
        Payload=bytes(json.dumps(write_query).encode("utf-8"))
    )

    # Decode and return the lambda's response
    return json.loads(common_api_lambda_response["Payload"].read().decode("utf-8"))