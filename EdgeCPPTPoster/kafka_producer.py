import json
from kafka import KafkaProducer
import boto3
from botocore.exceptions import ClientError

secret_arn = os.environ['mskSecretArn']
mskcluster_arn = os.environ['mskClusterArn']
topic_name = os.environ['topic_name']

def get_secret_value(arn):
    """Gets the value of a secret.
    Version (if defined) is used to retrieve a particular version of
    the secret.
    """
    try:
        secrets_client = boto3.client("secretsmanager")
        kwargs = {'SecretId': arn}
        response = secrets_client.get_secret_value(**kwargs)
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)

def get_brokers(mskcluster_arn):
    """Gets the broker list to publish the message 
    using msk cluster arn.
    """
    try:
        client = boto3.client('kafka')
        response = client.get_bootstrap_brokers(
        ClusterArn=mskcluster_arn
        )
        return response['BootstrapBrokerStringSaslScram'].split(',')
    except ClientError as e:
        print(e)

def publish_message(message_object):

    #Get credentials
    secret_object = get_secret_value(secret_arn)
    sec=json.loads(secret_object['SecretString'])
    print(sec['username'])
    
    #Get msk brokers
    list_brokers = get_brokers(mskcluster_arn)
    print(list_brokers)
    
    try:
        producer = KafkaProducer(
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-512',
        bootstrap_servers=list_brokers,
        sasl_plain_username=sec['username'],
        sasl_plain_password=sec['password']
        )

        producer.send(topic_name,message_object.encode('utf-8'))
        producer.flush()
        producer.close()
        json_obj = {
        "username" : sec['username'],
        "brokers" : str(list_brokers),
        "response" : "200"
        }
        return json_obj
    except ClientError as e:
        print(e)
        json_obj = {
        "username" : sec['username'],
        "brokers" : str(list_brokers),
        "response" : e
        }
        return json_obj



   