import edge_logger as logging

logger = logging.logging_framework(f"EdgeNGDI2CDSDKConversion.{__name__}")


def get_snapshot_data(params, time_stamp, address, spn_file_json):
    logger.info(f"Getting snapshot data for the parameter list: {params}")
    snapshot_data = {"Snapshot_DateTimestamp": time_stamp}
    parameters = []

    logger.info(f"SPN File as JSON: {spn_file_json}")
    try:
        for param in params:
            parameters.append({
                "Name": spn_file_json[param],
                "Value": params[param],
                "Parameter_Source_Address": address
            })

        snapshot_data["Parameter"] = parameters
        snapshot_data_list = [snapshot_data]

        logger.info(f"Snapshot Data: {snapshot_data_list}")
        return snapshot_data_list

    except Exception as e:
        logger.error(f"Error! An Exception {e} occurred when getting the snapshot data!")
        raise e
