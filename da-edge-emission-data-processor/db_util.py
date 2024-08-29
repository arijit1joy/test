import time

import utility as util
from pypika import Query, Table,functions as fn
import os

from edge_db_simple_layer import send_payload_to_edge, server_error, form_query_to_db_payload

time_format = os.getenv('TimeFormat')

logger = util.get_logger(__name__)

region = os.getenv('region')

def get_certification_family(device_id, esn):
    query = get_certification_family_query(device_id, esn)
    try:

        response = send_payload_to_edge(form_query_to_db_payload(query, method='get'))
        response_json = response.json()
        logger.debug(f"Response json is: {response_json}")
        if len(response_json) == 0:
            return ""
        certification_family = response_json[0]["certification_family"]
        logger.info(f"Certification family retrieved from database is: {certification_family}")
        return certification_family
    except Exception as e:
        logger.info("Error getting certificate family information from device_information table")
        return server_error(str(e))


def get_certification_family_query(device_id, esn):
    device_information = Table('da_edge_olympus.device_information')
    query = Query.from_(device_information)\
                 .select(device_information.certification_family)\
                 .where(device_information.engine_serial_number == esn)\
                 .where(device_information.device_id == device_id)
    logger.info(query.get_sql(quote_char=None))
    return query.get_sql(quote_char=None)

def insert_into_metadata_Table(device_id, message_id, esn, config_id, file_name, file_size):
    query = insert_to_metadata_table_query(device_id, message_id, esn, config_id, file_name, file_size)
    try:
        response = send_payload_to_edge(form_query_to_db_payload(query, method='post'))
        logger.info(f"Record inserted into Metadata table successfully")
    except Exception as e:
        logger.info("Error inserting into metadata table")
        return server_error(str(e))


def insert_to_metadata_table_query(device_id, message_id, esn, config_id, file_name, file_size):
    time_default_format = time.gmtime()
    current_date_time = time.strftime(time_format, time_default_format)
    da_edge_metadata = Table('da_edge_olympus.da_edge_metadata')
    query = Query.into(da_edge_metadata).columns(da_edge_metadata.device_id,
                                                 da_edge_metadata.uuid,
                                                 da_edge_metadata.data_protocol,
                                                 da_edge_metadata.data_pipeline_stage,
                                                 da_edge_metadata.esn,
                                                 da_edge_metadata.config_spec_name,
                                                 da_edge_metadata.file_name,
                                                 da_edge_metadata.file_size,
                                                 da_edge_metadata.file_received_date,
                                                 da_edge_metadata.created_datetime)
    query = query.insert(device_id, message_id, 'J1939_Emissions', 'FILE_SENT', esn, config_id, file_name, file_size, current_date_time, current_date_time)
    logger.info(query.get_sql(quote_char=None))
    return query.get_sql(quote_char=None)


def get_request_id_from_consumption_view(data_protocol, data_config_filename):
    query = _get_request_id_query(data_protocol, data_config_filename)
    try:
        response = send_payload_to_edge(form_query_to_db_payload(query, method='get')).json()

        if response and response[0]:
            logger.info(f" getting requestID - {response[0]['request_id']}")
            return response[0]['request_id'], response[0]['status']
        else:
            logger.info("query response is blank")
            return None, None
    except Exception as e:
        logger.info("Error in getting request ID")
        logger.info(str(e))
        return None, None


def _get_request_id_query(data_type, data_config_filename):
    data_requester_information = Table('da_edge_olympus.data_requester_information')
    scheduler = Table('da_edge_olympus.scheduler')
    split_config_filename = data_config_filename.split("_")
    query = Query.from_(data_requester_information) \
        .join(scheduler) \
        .on(data_requester_information.request_id == scheduler.request_id) \
        .select(scheduler.request_id, scheduler.status) \
        .where(data_requester_information.data_type == data_type) \
        .where(scheduler.device_id == split_config_filename[1]) \
        .where(scheduler.engine_serial_number == split_config_filename[2]) \
        .where(fn.Substring(scheduler.config_spec_file_name, 1, 6) == split_config_filename[3]) \
        .where(scheduler.status.isin(['Config Accepted', 'Data Rx In Progress']))
    logger.info(f"Query for getting request ID- {query}")
    return query.get_sql(quote_char=None)

def update_scheduler_table(req_id, device_id, config_status):
    if req_id and config_status != "Data Rx In Progress":
        try:
            current_scedhuler_status =get_current_scheduler_data_requester_status(req_id,device_id)
            query = get_update_scheduler_query(req_id, device_id)
            logger.debug(f"Update Scheduler Table Query: {query}")
            response = send_payload_to_edge(form_query_to_db_payload(query, method='post'))
            logger.info('Successfully updated scheduler table with Data Rx In Progress ')
            # inserting to audit_data_request_status table
            insert_into_audit_data_request_status_table(req_id, device_id, current_scedhuler_status, 'Data Rx In Progress')

        except Exception as e:
            logger.error(f"Failed to update scheduler table with Data Rx In Progress: '{e}'")
    else:
        logger.info(f"Could not get the Request ID for this file or the config_status was Data Rx In Progress!")


def get_update_scheduler_query(req_id, device_id):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_default_format = time.localtime()
    current_date_time = time.strftime(time_format, time_default_format)
    scheduler = Table('da_edge_olympus.scheduler')
    query = Query.update(scheduler).set(scheduler.status, 'Data Rx In Progress').set(scheduler.updated_date_time,
                                                                                     current_date_time).where(
        scheduler.request_id == req_id
        ).where(scheduler.device_id == device_id
                ).where(scheduler.status == 'Config Accepted')
    logger.info(f"Query for updating scheduler table with Data Rx In Progress - {query}")
    return query.get_sql(quote_char=None)


def get_cspec_req_id(sc_number):
    if '-' in sc_number:
        config_spec_name = ''.join(sc_number.split('-')[:-1])
        req_id = sc_number.split('-')[-1]
    else:
        config_spec_name = sc_number
        req_id = None
    return config_spec_name, req_id


def get_current_scheduler_data_requester_status(request_id, device_id):
    scheduler = Table('da_edge_olympus.SCHEDULER')
    data_requester_information = Table('da_edge_olympus.data_requester_information')
    query = Query.from_(scheduler
                        ).inner_join(data_requester_information
                                     ).on(scheduler.request_id == data_requester_information.request_id
                                          )
    query = query.select(scheduler.request_id, scheduler.status).where(scheduler.request_id == request_id
                                 ).where(scheduler.device_id == device_id).get_sql(quote_char=None)
    try:
        response = send_payload_to_edge(form_query_to_db_payload(query, method='get')).json()
        if response:
            logger.info(f" getting current status of request - {response[0]['status']}")
            return response[0]['status']
    except Exception as exception:
        logger.error(f"Error couldn't get current scheduler status: {exception}")
        return ""

def insert_into_audit_data_request_status_table(request_id, device_id, old_status, new_status):
    audit_dnr_table = Table('da_edge_olympus.audit_data_request_status')
    query = Query.into(audit_dnr_table).columns(
        audit_dnr_table.request_id,
        audit_dnr_table.device_id,
        audit_dnr_table.old_status,
        audit_dnr_table.new_status
    ).insert(request_id, device_id, old_status, new_status).get_sql(quote_char=None)
    logger.info(f"Insert audit_data_request_status Table Query: {query}")
    try:
        response = response = send_payload_to_edge(form_query_to_db_payload(query, method='post'))
        logger.info(f"successfully updated audit_data_request_status table : {response}")
    except Exception as exception:
        logger.error(f"Error updating audit_data_request_status table: {str(exception)}")

