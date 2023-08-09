import utility as util
from obfuscate_gps_handler import obfuscate_gps


LOGGER = util.get_logger(__name__)


def lambda_handler(event, context):  # noqa
    try:
        body = event
        tsp_name = body["telematicsPartnerName"]
        if tsp_name=="COSPA":
            LOGGER.info("This is a COSMOS HB file")
        obfuscate_gps(body)
    except Exception as e:
        LOGGER.error(f"An error occurred while obfuscating gps coordinates: {e}")
        util.write_to_audit_table(e)
