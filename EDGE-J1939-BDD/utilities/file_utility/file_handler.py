"""
    This file contains all of the functions that process Files.
"""

import json

from utilities.common_utility import exception_handler


@exception_handler
def get_json_file(file_name):
    json_file_stream = open(file_name, 'r')
    json_file = json_file_stream.read()
    return json.loads(json_file)


@exception_handler
def get_csv_file(file_name):
    # TODO implement CSV file processing
    pass


@exception_handler
def get_zip_file(file_name):
    # TODO implement ZIP file processing
    pass


@exception_handler
def get_file(file_name):
    # TODO implement generic (e.g. text) file processing
    pass


@exception_handler
def same_file_contents(first_file, second_file):
    import filecmp
    # Returns "True" if both files have the same content & "False" otherwise
    return filecmp.cmp(first_file, second_file, shallow=False)
