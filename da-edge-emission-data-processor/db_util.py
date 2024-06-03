import time

import utility as util
from pypika import Query, Table
import os

from edge_core_layer.edge_core import api_request
from edge_core_layer.edge_core import server_error

time_format = os.getenv('TimeFormat')

logger = util.get_logger(__name__)

region = os.getenv('region')
edgeCommonAPIURL = os.getenv("edgeCommonAPIURL")


def insert_into_metadata_Table(device_id, message_id, esn, config_id, file_name, file_size):
    query = insert_to_metadata_table_query(device_id, message_id, esn, config_id, file_name, file_size)
    try:
        response = api_request(edgeCommonAPIURL, "post", query)
        logger.info(f"Record inserted into Metadata table successfully")
    except Exception as e:
        logger.info("Error inserting into metadata table")
        return server_error(str(e))


def insert_to_metadata_table_query(device_id, message_id, esn, config_id, file_name, file_size):
    time_default_format = time.localtime()
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