import json
from kafka import KafkaProducer
import boto3
from botocore.exceptions import ClientError
import os
import edge_logger as logging
logger = logging.logging_framework("EdgeCPPTPoster.Post")

secret_arn = os.environ['mskSecretArn']
mskcluster_arn = os.environ['mskClusterArn']
topic_name = os.environ['topicName']

def get_secret_value():
    """Gets the value of a secret.
    Version (if defined) is used to retrieve a particular version of
    the secret.
    """
    try:
        logger.info("Inside get_secret_value() method")
        secrets_client = boto3.client("secretsmanager")
        kwargs = {'SecretId': secret_arn}
        response = secrets_client.get_secret_value(**kwargs)
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("The request had invalid params:", e)

def get_brokers(mskcluster_arn):
    """Gets the broker list to publish the message 
    using msk cluster arn.
    """
    logger.info("Inside get_brokers() method")
    try:
        client = boto3.client('kafka')
        response = client.get_bootstrap_brokers(
        ClusterArn=mskcluster_arn
        )
        return response['BootstrapBrokerStringSaslScram'].split(',')
    except ClientError as e:
        logger.error("Error while getting broker list", e)

def publish_message(message_object):

    #Get credentials
    logger.info("Inside publish_message()")
    secret_object = get_secret_value()
    sec=json.loads(secret_object['SecretString'])
    #print(sec['username'])
    
    #Get msk brokers
    list_brokers = get_brokers(mskcluster_arn)    
    try:
        producer = KafkaProducer(
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-512',
        bootstrap_servers=list_brokers,
        sasl_plain_username=sec['username'],
        sasl_plain_password=sec['password']
        )

        producer.send(topic_name,str(message_object).encode('utf-8'))
        producer.flush()
        producer.close()
        json_obj = {
        "response" : "200"
        }
        return json_obj
    except ClientError as e:
        logger.error("Error while publishing the message to cluster", e)
        json_obj = {
        "response" : "500"
        }
        return json_obj



   