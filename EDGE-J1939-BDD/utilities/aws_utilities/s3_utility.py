"""
    This file contains all of the functions that directly interact with AWS S3.
"""

import boto3
import botocore

from utilities.common_utility import exception_handler

S3_CLIENT = boto3.client('s3')  # noqa
S3_RESOURCE = boto3.resource('s3')  # noqa


@exception_handler
def upload_object_to_s3(bucket_name, key, path_to_file, metadata=None, extra_arguments=None):
    extra_args = extra_arguments if extra_arguments else {}  # If the user provides the extra arguments, use it

    if metadata:  # Add metadata (which is a json object) to the object, if it is provided
        extra_args["Metadata"] = metadata

    print(f"Uploading the file: '{key}' to the bucket: '{bucket_name}' . . .")
    S3_CLIENT.upload_file(path_to_file, bucket_name, key, ExtraArgs=extra_args)

    return True


@exception_handler
def get_key_from_list_of_s3_objects(bucket_name, prefix):
    s3_object_list = S3_CLIENT.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    s3_objects = s3_object_list.get("Contents") if s3_object_list.get("Contents") else None

    if not s3_objects:
        print(f"No object(s) with key prefix : '{prefix}' in the bucket: '{bucket_name}'!")
        return None

    s3_objects.sort(reverse=True, key=lambda s3_object: s3_object["LastModified"])
    return s3_objects[0]["Key"]  # Get the actual object key of the latest key


@exception_handler
def download_object_from_s3(bucket_name, key, path_to_file):
    print(f"Downloading the file: '{key}' from the bucket: '{bucket_name}' . . .")
    S3_CLIENT.download_file(bucket_name, key, path_to_file)

    return True


@exception_handler
def delete_object_from_s3(bucket_name, key):
    S3_RESOURCE.Object(bucket_name, key).delete()
    return True


# Check if the object is in the bucket and return the object data if the get_object_info is "True"
@exception_handler
def object_is_in_s3(bucket_name, key, get_object_info=False):
    try:
        object_info = S3_CLIENT.head_object(Bucket=bucket_name, Key=key)
        return object_info if get_object_info else True
    except botocore.exceptions.ClientError as object_in_s3_error:  # noqa
        if object_in_s3_error.response['Error']['Code'] == "404":
            print(f"The object with key: '{key}' does not exist in the bucket: '{bucket_name}'!")
            return False
        raise object_in_s3_error
