import sys

sys.path.insert(1, './lib')
import utility as util



LOGGER = util.get_logger(__name__)


def lambda_handler(event, context):  # noqa
    try:
        LOGGER.info(f"Event posted to Emission Data Processor lambda function is: {event}")
    except Exception as e:
        LOGGER.error(f"An error occurred while obfuscating gps coordinates: {e}")

