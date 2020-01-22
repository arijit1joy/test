import boto3
from behave import *
from behave.__main__ import main as behave_main
import subprocess
import os

# cf = boto3.client('cloudformation')
code_pipeline = boto3.client('codepipeline')

os.environ['PATH'] = os.environ['PATH'] + ':' + os.environ['LAMBDA_TASK_ROOT']


def lambda_handler(event, context):
    # TODO implement

    print("...Starting Function...")

    print(event)

    try:
        job_id = event['CodePipeline.job']['id']

        print('job_id: ' + str(job_id))

        job_data = event['CodePipeline.job']['data']

        artifacts = job_data['inputArtifacts']

        print('artifacts: ' + str(artifacts))

        exit_code = behave_main(["./", "--no-capture"])

        print("behave exited with code: " + str(exit_code))

        if exit_code != 0:
            cancel_test_deployment(job_id, "Behave executed unsuccessfully with exit code: " + str(exit_code))

        else:

            continue_test_deployment(job_id, "Behave executed successfully with exit code: " + str(exit_code))

    except Exception as e:

        print("Exception occurred")

        cancel_test_deployment(job_id, 'There was an exception: ' + str(e))

    print('Function complete.')
    return "Complete."


def cancel_test_deployment(job_id, message):
    print("Putting Job Failure")

    print(message)

    code_pipeline.put_job_failure_result(

        jobId=job_id,
        failureDetails={
            'type': 'JobFailed',
            'message': message
        })


def continue_test_deployment(job_id, message):
    print("Putting Job Success")

    print(message)

    code_pipeline.put_job_success_result(jobId=job_id)


# Main for local testing

if __name__ == '__main__':
    lambda_handler('', '')
