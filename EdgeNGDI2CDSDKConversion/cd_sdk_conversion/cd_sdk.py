import json
import utility as util

LOGGER, FILE_NAME = util.logger_and_file_name(__name__)


def map_ngdi_sample_to_cd_payload(parameters, fc=False):

    # Get the appropriate template file based on if the "fc" flag is set to True
    cd_json_template = json.load(open(f"cd_sdk_conversion/cd_{'fc' if fc else 'hb'}_sdk_payload.json", "r"))

    LOGGER.info(f"CD {'FC' if fc else 'HB'} Json Template: {cd_json_template}")

    final_cd_payload = {}

    # Populate the CD Payload Template
    for cd_fc_parameter in cd_json_template:
        if cd_fc_parameter.lower() in parameters:  # If the parameter is provided in the current file populate it
            final_cd_payload[cd_fc_parameter] = parameters[cd_fc_parameter.lower()]
        else:  # Send the template's default empty value (e.g. {}, "", [], etc.)
            final_cd_payload[cd_fc_parameter] = cd_json_template[cd_fc_parameter]

    return final_cd_payload
