#!/usr/bin/env bash

branch=`git rev-parse --abbrev-ref HEAD`

# PLEASE MAKE SURE TO UPDATE THIS TO THE CORRECT STACK NAME AND DEV PIPELINE FOR THIS REPO
stackName="stack-name edge-platform-apps-daedgej1939-1FKWUVITUI1AM"
devPipeline="edge-platform-apps-daedgej1939-1FKWUVITUI1AM-DevelopmentPipeline-LZLSV5MPEZCB"

devBranchParameterName="DevelopmentBranch"  # This is the parameter that sets the source branch for the development pipeline

echo -e "\nDeploymnent information: \n\n-> Branch: '$branch' \n-> Stack Name: '$stackName' \n-> Dev Pipeline: '$devPipeline'"

if [[ $branch!="master" || $branch!="test" || $branch!="stage" || $branch!="stage_test" ]]; then
    
    current_deployed_branch=`aws cloudformation describe-stacks --stack-name $stackName --query 'Stacks[*].Parameters[?ParameterKey==\`DevelopmentBranch\`].ParameterValue' --output text --region us-east-1`
    
    echo -e "\nThe currently deployed branch in the development AWS account is: '$current_deployed_branch'. Do you want to override it with the changes in the '$branch' branch? yY/nN"
    read decision
    
    if [[ $decision == "y" || $decision == "Y" ]]; then
        if [[ $branch == $current_deployed_branch ]]; then
            echo -e "\nThe branches are the same. Releasing the latest remote repo changes in the pipeline . . ."
            release=`aws codepipeline start-pipeline-execution --name $devPipeline --region us-east-1`
            echo "Execution id of the release : $release"
            
        else
            # Pull the latest parameters from the CFT and only update the 'DevelopmentBranch' paramter
            #   STACK_OVERFLOW_CITATION -> Question: https://stackoverflow.com/questions/61887550; Question and Answer by: https://stackoverflow.com/users/849256
            currentParameterNames=`aws cloudformation describe-stacks --stack-name $stackName --region us-east-1 --query "Stacks[].Parameters[].ParameterKey" --output text`
            
            # Generate the new parameter list
            newParameterList=""
            
            for parameterName in $currentParameterNames;
            do
                if [ $parameterName == $devBranchParameterName ]
                then
                    newParameterList+=" ParameterKey=$parameterName,ParameterValue=$branch";  # Overwrite the DevelopmentBranch parameter to be the current branch
                else
                    newParameterList+=" ParameterKey=$parameterName,UsePreviousValue=true"; # Retain the previous value for the parameter
                fi
            done
            
            echo -e "\nUpdating the $stackName Stack to point the '$devBranchParameterName' parameter to the $branch branch . . ."
            update_stack_result=`aws cloudformation update-stack --stack-name $stackName --use-previous-template --parameters $newParameterList --capabilities CAPABILITY_NAMED_IAM  --region us-east-1`
            echo -e "Result of Stack update: '$update_stack_result'\n"
            
            update_stack_status=`aws cloudformation describe-stacks --stack-name $stackName --query 'Stacks[*].StackStatus' --output text --region us-east-1`
            
            while [[ "$update_stack_status" == *"PROGRESS" ]];
            do
                echo "The $stackName stack is currently in '$update_stack_status' Status. Waiting for the stack update to complete . . ."
                sleep 5
                update_stack_status=`aws cloudformation describe-stacks --stack-name $stackName --query 'Stacks[*].StackStatus' --output text --region us-east-1`
            done
            
            echo -e "\nStack update action ended. The $stackName stack has the status: '$update_stack_status'\n"
            
            if [[ "$update_stack_status" == "UPDATE_COMPLETE" ]]; then
                echo "The $devPipeline Pipeline is set up to deploy the '$branch' branch into the development account. Releasing the latest remote repo changes in the pipeline . . ."
                release=`aws codepipeline start-pipeline-execution --name $devPipeline --region us-east-1`
                echo "Execution id of the release: $release"
                
            elif [[ "$update_stack_status" == *"FAILED" ]]; then
                echo "The $stackName stack failed to update. Please check the pipeline stack for logs."
            fi
        fi
    else
        echo "\nNot deploying the $branch branch as the decision is '$decision' (other than y/Y)"
    fi
else
    echo "\nUse this script to build only feature branch or development branch. Prod, stage and test accounts are built through Pull Requests"
fi