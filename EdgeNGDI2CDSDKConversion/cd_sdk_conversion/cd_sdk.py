import json

import edge_logger as logging

logger = logging.logging_framework(f"EdgeNGDI2CDSDKConversion.{__name__}")


def map_ngdi_sample_to_cd_payload(parameters, fc=False, hb=False):

    assert fc or hb, \
        "Unable to determine if this is a hb or fc call! Please, set either the 'fc' or 'hb' parameters to True."

    assert bool(fc) != bool(hb), \
        "The 'fc' and the 'hb' parameters cannot both be set to true! " \
        "Please, set either the 'fc' or 'hb' parameters to True."

    # Get the appropriate template file based on if the "fc" or the "hb" flag is set to True

    cd_json_template = json.load(open(f"cd_sdk_conversion/cd_{'fc' if fc else 'hb'}_sdk_payload.json", "r"))
    logger.info(f"CD {'FC' if fc else 'HB'} Json Template: {cd_json_template}")

    final_cd_payload = {}

    for cd_fc_parameter in cd_json_template:
        if cd_fc_parameter.lower() in parameters:
            final_cd_payload[cd_fc_parameter] = parameters[cd_fc_parameter.lower()]
        else:
            final_cd_payload[cd_fc_parameter] = cd_json_template[cd_fc_parameter]

    return final_cd_payload
