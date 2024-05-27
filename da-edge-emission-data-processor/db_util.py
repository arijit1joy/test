import utility as util
from pypika import Query, Table
import os

from edge_core_layer.edge_core import api_request
from edge_core_layer.edge_core import server_error

time_format = os.getenv('TimeFormat')

logger = util.get_logger(__name__)

region = os.getenv('region')
edgeCommonAPIURL = os.getenv("edgeCommonAPIURL")


def update_metadata_Table(device_id, esn, config_id):
    query = update_metadata_table_query(device_id, esn, config_id)
    logger.info(f"update metadata table query: {query}")
    try:
        response = api_request(edgeCommonAPIURL, "post", query)
        logger.info(f"Record updated into Metadata table successfully")
    except Exception as e:
        logger.error(f"Error updating into metadata table: {e}")
        return server_error(str(e))


def update_metadata_table_query(device_id, esn, config_id):
    da_edge_metadata = Table('da_edge_olympus.da_edge_metadata')
    query = Query.update(da_edge_metadata).set(da_edge_metadata.data_pipeline_stage, 'FILE_SENT').where(da_edge_metadata.device_id == device_id).where(da_edge_metadata.esn == esn).where(da_edge_metadata.config_spec_name == config_id)
    logger.info(query.get_sql(quote_char=None))
    return query.get_sql(quote_char=None)
