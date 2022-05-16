import os

import edge_core as edge
import scheduler_query as scheduler
from pypika import Query, Table

import utility as util
from utilities.redis_utility import get_set_redis_value

LOGGER = util.get_logger(__name__)
REDIS_EXPIRY = 5 * 24 * 60 * 60  # expire after 5 days
DB_API_URL = os.environ["edgeCommonAPIURL"]


# noinspection PyTypeChecker
def _get_request_id_from_consumption_view_query(data_protocol, data_config_filename):
    data_consumption_vw = Table('da_edge_olympus.edge_data_consumption_vw')
    query = Query.from_(data_consumption_vw).select(data_consumption_vw.request_id) \
        .where(data_consumption_vw.data_type == data_protocol) \
        .where(data_consumption_vw.data_config_filename == data_config_filename) \
        .where(data_consumption_vw.config_status == 'Config Accepted')
    return query.get_sql(quote_char=None)


def get_request_id_from_consumption_view(data_protocol, data_config_filename):
    query = _get_request_id_from_consumption_view_query(data_protocol, data_config_filename)

    redis_key = "req_id@@" + data_protocol.lower() + "@@" + data_config_filename.lower()
    LOGGER.debug(f"Redis Key for request_id and consumption_view: '{redis_key}'")

    try:

        response = get_set_redis_value(redis_key, query, REDIS_EXPIRY)
        LOGGER.debug(f"Get Req ID Response: '{response}'")

        if response:
            request_id = response[0]['request_id']
            return request_id

        LOGGER.info(f'Successfully fetched the request id from view')
    except Exception as exception:
        # Using logging level 'info' in case exception occurred due to invalid query
        LOGGER.info(f"Get Request ID From Consumption View Query: {query}")
        LOGGER.error(f'Failed to fetch request id from consumption view: {exception}')
        raise exception
    return None


def update_scheduler_table(req_id, device_id):
    LOGGER.debug(f'updating scheduler table')
    query = scheduler.get_update_scheduler_query(req_id, device_id)

    try:
        edge.api_request(DB_API_URL, "post", query)
        LOGGER.info(f'Successfully updated scheduler table')
    except Exception as exception:
        # Using logging level 'info' in case exception occurred due to invalid query
        LOGGER.info(f"Updating Scheduler Table Query: {query}")
        LOGGER.error(f'Failed to update scheduler table: {exception}')
        raise exception
