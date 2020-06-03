import json


def get_json_file(file_name):

    json_file_stream = open('file_handler/json_files/' + file_name, 'r')
    json_file = json_file_stream.read()
    return json.loads(json_file)
