import os
import utility as util
import edge_core as edge
import scheduler_query as scheduler
from pypika import Query, Table

LOGGER, FILE_NAME = util.logger_and_file_name(__name__)
db_api_url = os.environ["edgeCommonAPIURL"]


def get_request_id_from_consumption_view(data_protocol, data_config_filename):
    query = get_request_id_from_consumption_view_query(data_protocol, data_config_filename)
    LOGGER.info(f"Get Request ID From Consumption View Query: {query}")
    try:
        response = edge.api_request(db_api_url, "get", query)
        LOGGER.debug(f"Get Request ID From Consumption View Response: {response}")
        if response:
            request_id = response[0]['request_id']
            return request_id
        LOGGER.info(f'Successfully fetch request id from view')
    except Exception as exception:
        LOGGER.error(f'Failed to fetch request id from consumption view: {exception}')
        raise exception
    return None


# noinspection PyTypeChecker
def get_request_id_from_consumption_view_query(data_protocol, data_config_filename):
    data_consumption_vw = Table('da_edge_olympus.edge_data_consumption_vw')
    query = Query.from_(data_consumption_vw).select(data_consumption_vw.request_id) \
        .where(data_consumption_vw.data_type == data_protocol) \
        .where(data_consumption_vw.data_config_filename == data_config_filename) \
        .where(data_consumption_vw.config_status == 'Config Accepted')
    return query.get_sql(quote_char=None)


def update_scheduler_table(req_id, device_id):
    LOGGER.info(f'updating scheduler table')
    query = scheduler.get_update_scheduler_query(req_id, device_id)
    LOGGER.info(f"Updating Scheduler Table Query: {query}")
    try:
        edge.api_request(db_api_url, "post", query)
        LOGGER.info(f'Successfully updated scheduler table')
    except Exception as exception:
        LOGGER.error(f'Failed to update scheduler table: {exception}')
        raise exception
