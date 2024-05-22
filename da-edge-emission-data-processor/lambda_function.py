import sys
import json

sys.path.insert(1, './lib')
import utility as util
from s3_util import get_content
from db_util import update_metadata_Table
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
                content = get_content(key)
                content_json = json.loads(content)
                # Send to AAI Cloud
                push_to_tsb(content_json)
                # Update metadata table to FILE_PROCESSED
                update_metadata_Table(content_json['telematicsDeviceId'], content_json['componentSerialNumber'], content_json['dataSamplingConfigId'])
    except Exception as e:
        LOGGER.error(f"An error occurred while processing Emission data: {e}")
