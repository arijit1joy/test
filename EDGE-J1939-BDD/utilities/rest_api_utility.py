"""
    This file contains all of the functions that directly make REST API calls.
"""

import requests

from utilities.common_utility import exception_handler


@exception_handler
def get_url_with_query_string_params(url, query_string_params):
    parameter_string = '?'  # This indicates the start of the query string parameters part of the URL

    # Construct the final URL with the query string params
    for key, value in query_string_params.items():
        parameter_string += key + "=" + value + "&"

    return url + parameter_string[:-1]  # To remove '&' from the last index from the URL


@exception_handler
def get(url, query_string_params=None, headers=None):
    if query_string_params:
        url = get_url_with_query_string_params(url, query_string_params)

    print(f"Making a GET request to the URL: {url} . . .")
    get_response = requests.get(url, headers=headers)
    get_final_response = set_final_response(get_response)
    print("Requests GET Method Response: ", get_final_response)
    return get_final_response


@exception_handler
def post(url, payload, query_string_params=None, headers=None):
    if query_string_params:
        url = get_url_with_query_string_params(url, query_string_params)

    print(f"Making a POST request to the URL: {url} . . .")
    post_response = requests.post(url, json=payload, headers=headers)  # TODO: What is the response is not JSON
    post_final_response = set_final_response(post_response)
    print("Requests POST Method Response: ", post_final_response)
    return post_final_response


@exception_handler
def set_final_response(request_response):
    response = {"status_code": request_response.status_code, "body": request_response.json()}
    return response
