import os
from edge_simple_logging_layer import get_logger as get_log


def get_logger(file_name):
    return get_log(file_name)


