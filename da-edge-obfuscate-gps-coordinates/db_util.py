import utility as util
from pypika import Query, Table
import time
import os
import edge_core as edge

logger = util.get_logger(__name__)

region = os.getenv('region')
time_format = os.getenv('TimeFormat')
edgeCommonAPIURL = os.getenv("edgeCommonAPIURL")


def get_certification_family(device_id, esn):
    query = get_certification_family_query(device_id, esn)
    try:
        response = edge.api_request(edgeCommonAPIURL, "get", query)
        if len(response) == 0:
            return ""
        return response[0]
    except Exception as e:
        logger.info("Error getting certificate family information from device_information table")
        return edge.server_error(str(e))


def get_certification_family_query(device_id, esn):
    device_information = Table('da_edge_olympus.device_information')
    query = Query.from_(device_information)\
                 .select(device_information.certification_family)\
                 .where(device_information.engine_serial_number == esn)\
                 .where(device_information.device_id == device_id)

    logger.debug(query.get_sql(quote_char=None))
    return query.get_sql(quote_char=None)


def insert_into_metadata_Table(device_id, message_id, esn, config_id):
    query = insert_to_metadata_table_query(device_id, message_id, esn, config_id)
    try:
        response = edge.api_request(edgeCommonAPIURL, "post", query)
        logger.info(f"Record inserted into Metadata table successfully")
    except Exception as e:
        logger.info("Error inserting into metadata table")
        return edge.server_error(str(e))


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
    query = query.insert(device_id, message_id, 'J1939_Emissions', 'FILE_RECEIVED', esn, config_id, current_date_time)
    logger.info(query.get_sql(quote_char=None))
    return query.get_sql(quote_char=None)

