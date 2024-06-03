import sys
import json

sys.path.insert(1, './lib')
import utility as util
from s3_util import get_content
from db_util import insert_into_metadata_Table
from tsb_util import push_to_tsb

LOGGER = util.get_logger(__name__)


def lambda_handler(event, context):
    try:
        LOGGER.info(f"Event posted to Emission Data Processor lambda function is: {event}")
        for record in event['Records']:
            s3_body = json.loads(record['body'])
            LOGGER.info(f"s3_body: {s3_body}")
            for s3_record in s3_body['Records']:
                key = s3_record['s3']['object']['key']
                LOGGER.info(f"fileId: {key}")
                # download file in s3 bucket
                content, uuid = get_content(key)
                content_json = json.loads(content)
                # Send to AAI Cloud
                push_to_tsb(content_json)
                # insert a row in metadata table to FILE_SENT
                insert_into_metadata_Table(content_json["telematicsDeviceId"], uuid, content_json["componentSerialNumber"], content_json['dataSamplingConfigId'],
                                           key.rsplit('/', 1).pop(), len(content))
    except Exception as e:
        LOGGER.error(f"An error occurred while processing Emission data: {e}")
