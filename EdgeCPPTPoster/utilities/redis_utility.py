import json
import os
import sys
import boto3

import utility as util
LOGGER = util.get_logger(__name__)

sys.path.insert(1, './lib')
from edge_db_lambda_client import invoke_db_reader
from rediscluster import RedisCluster

REDIS_CLIENT = None
SECRET_NAME = os.environ['RedisSecretName']
REGION = os.environ['region']


def get_redis_connection():
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=REGION)
        get_secret_value_response = client.get_secret_value(SecretId=SECRET_NAME)
        secret_string = get_secret_value_response['SecretString']
        secret_params = json.loads(secret_string)

        redis_client = RedisCluster(startup_nodes=[{"host": secret_params['redis_host'],
                                                    "port": secret_params['redis_port']}],
                                    decode_responses=True, skip_full_coverage_check=True)
        LOGGER.info("Connected to redis.!")
        return redis_client
    except Exception as RedisException:
        LOGGER.error(f"Connecting to redis failed with error: {RedisException}")
        return None


def get_set_redis_value(redis_key, sql_query, redis_expiry):
    try:
        global REDIS_CLIENT
        if REDIS_CLIENT is None:
            REDIS_CLIENT = get_redis_connection()

        redis_response = REDIS_CLIENT.get(redis_key)
        response = json.loads(redis_response) if redis_response else None
        LOGGER.debug(f"Value from Redis: {response}")

        if response is None:
            LOGGER.info(
                f"Could not find the Request ID for the key: '{redis_key}' in the Redis Cache. "
                f"Retrieving it from the Data Base with the query: '{sql_query}'."
            )
            response = invoke_db_reader(sql_query)
            REDIS_CLIENT.set(redis_key, json.dumps(response), ex=redis_expiry)

        LOGGER.info(f"Attempting to force DB connection with query {sql_query}")
        LOGGER.info("DB connection returned")

        return response
    except Exception as error:
        LOGGER.error(f"An error occurred while getting and setting value from Redis: {error}")
