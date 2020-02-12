import logging
import boto3
from botocore.exceptions import ClientError


def get_object(bucket_name, object_name):
    # Retrieve the object
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return None
    return response["Body"].read().decode()


def main(bucket, key):
    stream = get_object(bucket, key)
    return stream


# if __name__ == '__main__':
#     main()