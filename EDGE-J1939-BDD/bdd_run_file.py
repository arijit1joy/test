import os
import shutil
import subprocess
import sys
from datetime import datetime

if __name__ == '__main__':
    print(f"Running behave command. Received arguments: {str(sys.argv)}")

    # Get the current execution environment (one of: "dev", "stage" or "test")
    execution_environment = sys.argv[1].lower()
    region = sys.argv[2].lower()
    print(f"Current application environment: '{execution_environment}' and region: '{region}'")

    # Delete the "reports" folder if it is already there
    if os.path.exists("reports"):
        shutil.rmtree("reports")

    # Get the current date for reporting
    current_date_time = datetime.utcnow()
    time_path = f"{current_date_time.year}/{current_date_time.month:02d}/{current_date_time.day:02d}"

    if execution_environment in ["dev", "stage", "test"]:
        behave_execution_exit_code = subprocess.call("behave "
                                                     f"-D environment={execution_environment} "
                                                     f"-D region={region}",
                                                     shell=True)

        # Regardless of whether the behave execution passed or failed, copy the reports to the EDGE reporting bucket
        s3_copy_exit_code = subprocess.call("aws s3 cp reports/ "
                                            f"s3://da-edge-bdd-reports-${execution_environment}/J1939/"
                                            f"{time_path}/ "
                                            "--recursive",
                                            shell=True)

        if s3_copy_exit_code != 0:
            print(f"<<<<<<<<AN ERROR: '{s3_copy_exit_code}' OCCURRED WHILE UPLOADING THE BDD REPORTS TO THE EDGE "
                  "REPORTING BUCKET!>>>>>>>")

        if behave_execution_exit_code != 0:
            print("BDD Execution Failed! Aborting Deployment!")
            exit(behave_execution_exit_code)

        print("BDD Execution Succeeded!")
    else:
        print(f"An invalid application environment: '{execution_environment}' was provided. "
              f"Valid values are: ['dev', 'test', 'stage']")
