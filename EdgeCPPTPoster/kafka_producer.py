import os
import json
import boto3
import utility as util
from kafka import KafkaProducer
from botocore.exceptions import ClientError

LOGGER = util.get_logger(__name__)
cluster_response = {}
secret_response = {}


def get_secret_value():
    """Gets the value of a secret.
    Version (if defined) is used to retrieve a particular version of
    the secret.
    """
    global secret_response

    try:
        secrets_client = boto3.client("secretsmanager")
        secret_arn = os.environ['mskSecretArn']
        if not secret_response:
            LOGGER.debug("Inside get_secret_value() method")
            kwargs = {'SecretId': secret_arn}
            secret_response = secrets_client.get_secret_value(**kwargs)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            LOGGER.error("The requested secret was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            LOGGER.error("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            LOGGER.error("The request had invalid params:", e)
    return secret_response


def get_brokers(mskcluster_arn):
    """Gets the broker list to publish the message 
    using msk cluster arn.
    """
    LOGGER.debug("Inside get_brokers() method")
    global cluster_response
    try:
        cluster_client = boto3.client('kafka')
        if not cluster_response:
            cluster_response = cluster_client.get_bootstrap_brokers(ClusterArn=mskcluster_arn)
    except ClientError as e:
        LOGGER.error("Error while getting broker list", e)
    return cluster_response['BootstrapBrokerStringSaslScram'].split(',')


def publish_message(message_object, j1939_data_type):
    # Get credentials
    LOGGER.debug("Inside publish_message()")
    secret_object = get_secret_value()
    sec = json.loads(secret_object['SecretString'])

    # Get msk brokers
    mskcluster_arn = os.environ['mskClusterArn']
    list_brokers = get_brokers(mskcluster_arn)
    try:
        producer = KafkaProducer(
            security_protocol='SASL_SSL',
            sasl_mechanism='SCRAM-SHA-512',
            bootstrap_servers=list_brokers,
            sasl_plain_username=sec['username'],
            sasl_plain_password=sec['password']
        )

        topic_name = os.environ['topicName']
        producer.send(topic_name, str(message_object).encode('utf-8'))
        producer.flush()
        producer.close()
        json_obj = {
            "response": "200"
        }
        return json_obj
    except ClientError as e:
        error_message = f"Error while publishing the message to cluster: {e}"
        LOGGER.error(error_message)
        util.write_to_audit_table(j1939_data_type, error_message, message_object["telematicsDeviceId"])
        json_obj = {
            "response": "500"
        }
        return json_obj
