#!/usr/bin/env bash

branch=`git rev-parse --abbrev-ref HEAD`
echo "Checked out branch is : $branch"
if [[ $branch!="master" || $branch!="test" || $branch!="stage" || $branch!="stage_test" ]]; then
        current_deployed_branch=`aws cloudformation describe-stacks --stack-name edge-platform-apps-daedgej1939-1FKWUVITUI1AM --query 'Stacks[*].Parameters[?ParameterKey==\`DevelopmentBranch\`].ParameterValue' --output text --region us-east-1`
        echo "Current deployed branch in development AWS account is: '$current_deployed_branch'. Do you want to overide with '$branch' changes ? y/n"
        read decision
        if [[ $decision == "y" || $decision == "Y" ]]; then
            if [[ $branch == $current_deployed_branch ]]; then
                release=`aws codepipeline start-pipeline-execution --name edge-platform-apps-daedgej1939-1FKWUVITUI1AM-DevelopmentPipeline-LZLSV5MPEZCB --region us-east-1`
                echo "Execution id of the release : $release"
            else
                echo "Build and deploy $branch to AWS dev account"
                update_stack_result=`aws cloudformation update-stack --stack-name edge-platform-apps-daedgej1939-1FKWUVITUI1AM --use-previous-template --parameters ParameterKey=ApplicationName,UsePreviousValue=true ParameterKey=RepositoryName,UsePreviousValue=true ParameterKey=DevelopmentAccount,UsePreviousValue=true ParameterKey=StageAccount,UsePreviousValue=true ParameterKey=TestAccount,UsePreviousValue=true ParameterKey=ProductionAccount,UsePreviousValue=true ParameterKey=TwoBuildspecs,UsePreviousValue=true ParameterKey=Platform,UsePreviousValue=true ParameterKey=BDD,UsePreviousValue=true ParameterKey=DevelopmentBranch,ParameterValue=$branch --capabilities CAPABILITY_NAMED_IAM  --region us-east-1`
                echo "result of update $update_stack_result"
                update_stack_status=`aws cloudformation describe-stacks --stack-name edge-platform-apps-daedgej1939-1FKWUVITUI1AM --query 'Stacks[*].StackStatus' --output text --region us-east-1`
                echo "Development Cloudformation update status: $update_stack_status"
                
                while [[ "$update_stack_status" == *"PROGRESS" ]]; do
                        echo "Update stack in $update_stack_status. Waits for to complete"
                        sleep 5
                        update_stack_status=`aws cloudformation describe-stacks --stack-name edge-platform-apps-daedgej1939-1FKWUVITUI1AM --query 'Stacks[*].StackStatus' --output text --region us-east-1`
                    done 
                echo "Pipeline cloudformation stack status : $update_stack_status"
                if [[ "$update_stack_status" == "UPDATE_COMPLETE" ]]; then
                        echo "Pipeline updated to deploy branch '$branch' into development account. Releasing the changes"
                        release=`aws codepipeline start-pipeline-execution --name edge-platform-apps-daedgej1939-1FKWUVITUI1AM-DevelopmentPipeline-LZLSV5MPEZCB --region us-east-1`
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

