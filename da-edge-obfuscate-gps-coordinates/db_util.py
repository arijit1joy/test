import utility as util
from pypika import Query, Table
import time
import os
import json
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

time_format = os.getenv('TimeFormat')

logger = util.get_logger(__name__)

region = os.getenv('region')
secret_name = os.environ['EdgeRDSSecretName']


def get_certification_family(device_id, esn):
    query = get_certification_family_query(device_id, esn)
    try:
        response = db_request("get", query)
        if len(response) == 0:
            return ""
        return response[0]
    except Exception as e:
        logger.info("Unable to get certification family information from device_information table")
        return server_error(str(e))


def get_certification_family_query(device_id, esn):
    device_information = Table('device_information', schema='da_edge_olympus')
    query = Query.from_(device_information)\
                 .select(device_information.certification_family)\
                 .where(device_information.engine_serial_number == esn)\
                 .where(device_information.device_id == device_id)

    logger.debug(query.get_sql(quote_char=None))
    return query.get_sql(quote_char=None)


def insert_into_metadata_Table(device_id, message_id, esn, config_id):
    query = insert_to_metadata_table_query(device_id, message_id, esn, config_id)
    try:
        db_request("post", query)
    except Exception as e:
        logger.info("Unable to insert into metadata table")
        return server_error(str(e))


def insert_to_metadata_table_query(device_id, message_id, esn, config_id):
    time_default_format = time.localtime()
    current_date_time = time.strftime(time_format, time_default_format)
    da_edge_metadata = Table('da_edge_olympus.da_edge_metadata')
    query = Query.into(da_edge_metadata).columns(da_edge_metadata.device_id,
                                                 da_edge_metadata.uuid,
                                                 da_edge_metadata.data_protocol,
                                                 da_edge_metadata.data_pipeline_stage,
                                                 da_edge_metadata.esn,
                                                 da_edge_metadata.config_spec_name,
                                                 da_edge_metadata.created_datetime)
    query = query.insert(device_id, message_id, 'J1939_FC', 'FILE_RECEIVED', esn, config_id, current_date_time)
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