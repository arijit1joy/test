"""
    This file contains all of the functions that deal with DB functions (query creation, etc.).
"""

import json

from utilities.common_utility import exception_handler


@exception_handler
def get_edge_db_payload(method, pypika_query):
    payload_template = json.load(open("utilities/file_utility/common files/db_payload.json"))
    payload_template["method"] = method
    payload_template["query"] = pypika_query.get_sql(quote_char=None)
    print(pypika_query.get_sql(quote_char=None))
    return payload_template
