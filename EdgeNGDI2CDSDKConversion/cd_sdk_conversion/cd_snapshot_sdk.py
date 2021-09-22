import utility as util

LOGGER = util.logger_and_file_name(__name__)


def get_snapshot_data(params, time_stamp, address, spn_file_json):
    snapshot_data = {"Snapshot_DateTimestamp": time_stamp}
    parameters = []

    LOGGER.debug(f"SPN File as JSON: {spn_file_json}")
    try:
        # For each parameter, add an object to the Snapshot object
        for param in params:
            if param in spn_file_json:
                parameters.append({
                    "Name": spn_file_json[param],
                    "Value": params[param],
                    "Parameter_Source_Address": address
                })
            else:  # If the parameter is not part of the EDGE SPN list, print a warning & move on to the next parameter
                LOGGER.warn(f"The parameter: '{param}' is not in the EDGE SPN list!")

        snapshot_data["Parameter"] = parameters
        snapshot_data_list = [snapshot_data]  # Make the snapshot object a list (per the CD specifications)

        LOGGER.debug(f"Snapshot Data: {snapshot_data_list}")
        return snapshot_data_list

    except Exception as get_snapshot_error:
        # Catch the error and just print the error to the console
        LOGGER.error(f"Error! An exception occurred when getting the snapshot data: {get_snapshot_error}")
        raise get_snapshot_error
