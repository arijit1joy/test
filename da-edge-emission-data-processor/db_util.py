import utility as util
from pypika import Query, Table
import os
import json
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

time_format = os.getenv('TimeFormat')

logger = util.get_logger(__name__)

region = os.getenv('region')
secret_name = os.environ['EdgeRDSSecretName']


def update_metadata_Table(device_id, esn, config_id):
    query = update_metadata_table_query(device_id, esn, config_id)
    try:
        db_request("post", query)
    except Exception as e:
        logger.info("Unable to update status in metadata table")
        return server_error(str(e))


def update_metadata_table_query(device_id, esn, config_id):
    da_edge_metadata = Table('da_edge_olympus.da_edge_metadata')
    query = Query.update(da_edge_metadata).set(da_edge_metadata.data_pipeline_stage, 'FILE_SENT').where(da_edge_metadata.device_id == device_id).where(da_edge_metadata.esn == esn).where(da_edge_metadata.config_spec_name == config_id)
    logger.info(query.get_sql(quote_char=None))
    return query.get_sql(quote_char=None)


def db_request(method, query):

    conn = psycopg2.connect(get_db_connection_string())

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    if method == 'get':
        response = json.loads(json.dumps(cur.fetchall(), indent=2))
        logger.info(response)
        return response

    conn.commit()
    body = {
        "message": "operation successful"
    }

    return body


def get_db_connection_string():
    secret_params = get_secret_params()
    db_conn_string = "host=" + secret_params['host'] + " port=" + str(secret_params['port']) + " dbname=" + secret_params['dbname'] + " user=" + \
                     secret_params['username'] + " password=" + secret_params['password']
    return db_conn_string


def get_secret_params():
    client = boto3.client(service_name='secretsmanager', region_name=region)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret_string = get_secret_value_response['SecretString']
    return json.loads(secret_string)


def server_error(message):
    body = {
        "errorMessage": message
    }
    return __http_response(500, body)


def __http_response(status_code, body):
    response = {
            "isBase64Encoded": False,
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(body)
        }
    return response