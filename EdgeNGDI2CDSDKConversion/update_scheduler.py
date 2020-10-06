import boto3
import edge_core as edge
from pypika import Query, Table, Order, functions as fn


def update_scheduler_table(req_id):
    print('updating scheduler table')
    query = get_update_scheduler_query(req_id)
    print('query',query)
    try:
        edge.api_request(os.environ["EdgeCommonDBAPI"], "post", query)
        print('Successfully updated scheduler table')
    except Exception as exception:
        print('Failed to update scheduler table')
        return edge.server_error(str(exception)) 

def get_update_scheduler_query(req_id):
    scheduler = Table('da_edge_olympus.scheduler')
    query = Query.update(scheduler).set(scheduler.status, 'Data Rx in progress').where(scheduler.request_id == req_id)
    return query.get_sql(quote_char=None)