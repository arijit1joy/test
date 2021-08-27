import os
from edge_logger import logging_framework


def logger_and_file_name(file_name):
    converted_file_name = ''.join([word.capitalize() for word in file_name.split('_')])
    logger = logging_framework(f"ObfuscateGPSCoordinates.{converted_file_name}", os.environ["LoggingLevel"])

    return logger, converted_file_name
