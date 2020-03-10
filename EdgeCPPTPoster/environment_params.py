
get_dev_info_payload = {
    "method": "get",
    "query": "SELECT DEVICE_TYPE, DEVICE_OWNER, DOM FROM da_edge_olympus.DEVICE_INFORMATION WHERE DEVICE_ID = %("
             "devId)s;",
    "input": {"Params": [{"devId": "devId"}]}
}
