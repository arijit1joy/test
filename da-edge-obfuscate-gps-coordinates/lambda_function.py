import utility as util
from obfuscate_gps_handler import obfuscate_gps


LOGGER = util.logger_and_file_name(__name__)


def lambda_handler(event, context):  # noqa
    try:
        body = event
        obfuscate_gps(body)
    except Exception as e:
        LOGGER.error(f"An error occurred while obfuscating gps coordinates: {e}")
        util.write_to_audit_table(e)
