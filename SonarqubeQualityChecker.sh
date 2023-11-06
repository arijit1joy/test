#!/bin/bash

function getSonarSecrets() {
  read -r sonarToken sonarHost <<<$(python - <<-EOF
import sys
import boto3
import traceback
import json

secrets_client = boto3.client('secretsmanager')

try:
  # Get the sonar secret from the AWS Secrets Manager
  sonar_secrets = secrets_client.get_secret_value(
      SecretId="$sonarSecretID"
  )

  sonar_secret_string = json.loads(sonar_secrets["SecretString"])
  sonar_token, sonar_host = sonar_secret_string["sonar_common_token"], sonar_secret_string["sonar_host_https_url"]

  print(f"{sonar_token} {sonar_host}")  # This returns the result to the bash script
except Exception as secret_retrieval_error:
  print(f"An exception ('{secret_retrieval_error}') occurred while trying to retrieve the Sonar secrets.")
  traceback.print_exc()
  exit(1)
EOF
)
  if [ $? -ne 0 ]; then
    exit 1;  # If some error occurred while trying to get the sonar secret, do not proceed with the rest of the process
  fi;
}

function getQualityGateAnalysis() {
  projectKey=$(grep "${1}" "${2}/sonar-project.properties" | cut -d'=' -f2) # Get the project key from the sonar properties file

  # setMainBranchCommand=$(echo curl -X POST -d "project=$projectKey&branch=${3}" -u "${sonarToken}": "${sonarHost}"/api/project_branches/set_main)
  # echo Setting branch "${3}" as the main branch in project "$projectKey" with the command: "$setMainBranchCommand"
  
  getReportCommand=$(echo curl -u "${sonarToken}": "${sonarHost}"/api/qualitygates/project_status?projectKey=$projectKey"&branch=${3}")
  echo Retrieving the Sonarqube info for the project: "$projectKey" with the command: "$getReportCommand"

  report=$($getReportCommand)
  # setMainBranch=$($setMainBranchCommand)
}

function checkProjectStatus() {
  python - "${report}" $projectKey <<-EOF
import sys
import os
import json
import re
import traceback

format_delimiter = f"\n#{'-'*180}#\n"  # Format delimiter for console output readability
projectKey = None
skip_metric_keys = ["new_coverage"]


def print_success_messages(project_status, project_key):
  warning_message = f" However, the Quality Gate status is: '{project_status}' and this needs to be fixed." if project_status.lower() != 'ok' else ''
  print(f"Project: '{project_key}' did not fail any of the Quality Gate conditions!{warning_message}")
  print(format_delimiter)
  print(f"\n#{'<->'*60}#\n\nSonar Qube Quality Gate Check Was Successfully Completed for the project: '{project_key}'!\n\n#{'<->'*60}#\n\n")


def should_fail_build(error_metric_keys):
  for metric_key in error_metric_keys:
    if(metric_key not in skip_metric_keys):
      return True
  return False

try:
#  print(f"Arguments to Python Script => \n{sys.argv}")

  # Get the report and project name from the command line
  quality_gate_report, project_key = json.loads(sys.argv[1]), sys.argv[2].strip()
  print(f"\nQuality Gate report for project: '{project_key}' => \n{quality_gate_report}")
  print(format_delimiter)

  # Get the project status from the report
  nonexistent_project_status_flag = 'PROJECT STATUS NOT FOUND'
  project_status = quality_gate_report["projectStatus"]["status"] if "projectStatus" in quality_gate_report else None
  print(f"Quality Gate report project status for project: '{project_key}' => '{project_status if project_status else nonexistent_project_status_flag}'")
  print(format_delimiter)

  if project_status and project_status.lower() != "error":
    print_success_messages(project_status, project_key)
  elif not project_status:
    raise RuntimeError(f"An error occurred while retrieving the Quality Gate report for the project: '{project_key}'! " \
                        "Please, ensure that the project exists in the Sonarqube portal.")
  else:
    failure_message = f"Project: '{project_key}' failed one or more Quality Gate condition(s)"
    print(f"{failure_message}!\nPrinting failed gate conditions . . .\n")

    error_metric_keys = []

    # Print out all the failing Quality Gate conditions
    for condition in quality_gate_report["projectStatus"]["conditions"]:
      if condition["status"].lower() != "ok":
        print(f"\t{condition}")
        # Add the current "condition"'s metricKey to an array
        error_metric_keys.append(condition["metricKey"])
    print(format_delimiter)

    if should_fail_build(error_metric_keys):
      build_fail_error_metric_keys = [metric_key for metric_key in error_metric_keys if metric_key not in skip_metric_keys]
      print(f"Aborting build due to errors in these metrics: '{build_fail_error_metric_keys}'")
      print(format_delimiter)
      raise RuntimeError(f"{failure_message}!")
    else:
      print_success_messages(project_status, project_key)
except Exception as quality_checker_error:
  print(f"An exception ('{quality_checker_error}') occurred while checking the Sonarqube Quality Gate status for the project: '{project_key}'.")
  if type(quality_checker_error) != RuntimeError:
    traceback.print_exc()  # Print error stacktrace
  print(format_delimiter)
  exit(1)  # Exit the program with a non-zero exit status (indicating failure)
EOF
  return $? # Return exit status from Python script
}

echo -e "\n\n#<-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><->#\n\nStarting Sonar Qube Quality Gate Check . . .\n\n#<-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><-><->#\n\n"

# Get the SonarQube connection details from the AWS Secrets Manager
sonarSecretID="${2}"
getSonarSecrets

# Get the status of the Quality Gate conditions for this project
getQualityGateAnalysis 'sonar.projectKey' $1 $3

# Check if all the all Quality Gate conditions passed or not and respond a non-zero integer if any Quality Gate conditions failed
checkProjectStatus $report