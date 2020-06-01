#!/usr/bin/env bash

branch=`git rev-parse --abbrev-ref HEAD`
echo "Checked out branch is : $branch"
if [[ $branch!="master" || $branch!="test" || $branch!="stage" || $branch!="stage_test" ]]; then
        current_deployed_branch=`aws cloudformation describe-stacks --stack-name da-edge-j1939-pipeline-dev --query 'Stacks[*].Parameters[?ParameterKey==\`BranchName\`].ParameterValue' --output text --region us-east-1`
        echo "Current deployed branch in development AWS account is: '$current_deployed_branch'. Do you want to overide with '$branch' changes ? y/n"
        read decision
        if [[ $decision == "y" || $decision == "Y" ]]; then
            if [[ $branch == $current_deployed_branch ]]; then
                release=`aws codepipeline start-pipeline-execution --name da-edge-j1939-services-pipeline-dev --region us-east-1`
                echo "Execution id of the release : $release"
            else
                echo "Build and deploy $branch to AWS dev account"
                update_stack_result=`aws cloudformation update-stack --stack-name da-edge-j1939-pipeline-dev --use-previous-template --parameters ParameterKey=RepositoryName,UsePreviousValue=true ParameterKey=S3Bucket,UsePreviousValue=true ParameterKey=DevAccount,UsePreviousValue=true ParameterKey=CMKARN,UsePreviousValue=true ParameterKey=CrossAccountCondition,UsePreviousValue=true ParameterKey=BranchName,ParameterValue=$branch --capabilities CAPABILITY_NAMED_IAM --region us-east-1`
                echo "result of update $update_stack_result"
                update_stack_status=`aws cloudformation describe-stacks --stack-name da-edge-j1939-pipeline-dev --query 'Stacks[*].StackStatus' --output text --region us-east-1`
                echo "Development Cloudformation update status: $update_stack_status"
                
                while [[ "$update_stack_status" == *"PROGRESS" ]]; do
                        echo "Update stack in $update_stack_status. Waits for to complete"
                        sleep 5
                        update_stack_status=`aws cloudformation describe-stacks --stack-name da-edge-j1939-pipeline-dev --query 'Stacks[*].StackStatus' --output text --region us-east-1`
                    done 
                echo "Pipeline cloudformation stack status : $update_stack_status"
                if [[ "$update_stack_status" == "UPDATE_COMPLETE" ]]; then
                        echo "Pipeline updated to deploy branch '$branch' into development account. Releasing the changes"
                        release=`aws codepipeline start-pipeline-execution --name da-edge-j1939-services-pipeline-dev --region us-east-1`
                        echo "Execution id of the release : $release"

                elif [[ "$update_stack_status" == *"FAILED" ]]; then
                    echo "Pipeline update failed. Please check the pipeline stack for logs"
                fi
            fi
        else
            echo "Not deploying the branch  $branch as decision is $decision (other than y/Y)"
        fi
else
    echo "Use this script to build only feature branch or development branch. Prod, stage and test accounts are build through pull request"
fi

