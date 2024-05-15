import os
from edge_core_layer.edge_logger import logging_framework


def get_logger(file_name):
    converted_file_name = ''.join([word.capitalize() for word in file_name.split('_')])
    logger = logging_framework(f"EmissionDataProcessor.{converted_file_name}", os.environ["LoggingLevel"])

    return logger


