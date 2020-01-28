#!/usr/bin/env bash
echo -n "Enter ToolsAccount > "
read ToolsAccount
echo -n "Enter ToolsAccount ProfileName for AWS Cli operations> "
read ToolsAccountProfile
echo -n "Enter Dev Account > "
read DevAccount
echo -n "Enter DevAccount ProfileName for AWS Cli operations> "
read DevAccountProfile
echo -n "Enter Stage Account > "
read StageAccount
echo -n "Enter StageAccount ProfileName for AWS Cli operations> "
read StageAccountProfile
echo -n "Enter Prod Account > "
read ProdAccount
echo -n "Enter ProdAccount ProfileName for AWS Cli operations> "
read ProdAccountProfile



aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount ProductionAccount=$ProdAccount --profile $ToolsAccountProfile
#aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount --profile $ToolsAccountProfile

echo -n "Enter S3 Bucket created from above > "
read S3Bucket

echo -n "Enter CMK ARN created from above > "
read CMKArn

aws configure --profile ToolsAccountProfile set region us-east-2

aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-dest --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount ProductionAccount=$ProdAccount --profile $ToolsAccountProfile --region us-east-2
#aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-dest --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount --profile $ToolsAccountProfile --region us-east-2

echo -n "Enter S3 Bucket created from above > "
read S3BucketProd

echo -n "Enter CMK ARN created from above > "
read CMKArnProd

aws configure --profile ToolsAccountProfile set region us-east-1

echo -n "Executing in Dev Account"
aws cloudformation deploy --stack-name da-edge-j1939-codepipeline-cloudformation-role --template-file TestAccount/toolsacct-codepipeline-cloudformation-deployer.yaml --capabilities CAPABILITY_NAMED_IAM --parameter-overrides ToolsAccount=$ToolsAccount CMKARN=$CMKArn  S3Bucket=$S3Bucket --profile $DevAccountProfile

echo -n "Executing in Stage Account us-east-2"
aws cloudformation deploy --stack-name da-edge-j1939-codepipeline-cloudformation-role --template-file TestAccount/toolsacct-codepipeline-cloudformation-deployer.yaml --capabilities CAPABILITY_NAMED_IAM --parameter-overrides ToolsAccount=$ToolsAccount CMKARN=$CMKArn  S3Bucket=$S3Bucket S3BucketIndia=$S3BucketIndia CMKARNIndia=$CMKArnIndia --profile $StageAccountProfile

echo -n "Executing in PROD Account"
aws cloudformation deploy --stack-name da-edge-j1939-codepipeline-cloudformation-role --template-file TestAccount/toolsacct-codepipeline-cloudformation-deployer.yaml --capabilities CAPABILITY_NAMED_IAM --parameter-overrides ToolsAccount=$ToolsAccount CMKARN=$CMKArnProd  S3Bucket=$S3BucketProd --profile $ProdAccountProfile

echo -n "Creating Pipeline in Tools Account"
aws cloudformation deploy --stack-name da-edge-j1939-pipeline --template-file ToolsAcct/code-pipeline.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount ProductionAccount=$ProdAccount CMKARN=$CMKArn CMKARNPROD=$CMKArnProd S3Bucket=$S3Bucket S3BucketProd=$S3BucketProd --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1
#aws cloudformation deploy --stack-name da-edge-j1939-pipeline --template-file ToolsAcct/code-pipeline.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount CMKARN=$CMKArn S3Bucket=$S3Bucket S3BucketProd=$S3BucketProd CMKARNPROD=$CMKArnProd  --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1

echo -n "Adding Permissions to the CMK"
aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides CodeBuildCondition=true --profile $ToolsAccountProfile

aws configure --profile ToolsAccountProfile set region us-east-2

echo -n "Adding Permissions to the PROD CMK"
aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-dest --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides CodeBuildCondition=true --profile $ToolsAccountProfile

aws configure --profile ToolsAccountProfile set region us-east-1

echo -n "Adding Permissions to the Cross Accounts"
aws cloudformation deploy --stack-name da-edge-pipeline-j1939 --template-file ToolsAcct/code-pipeline.yaml --parameter-overrides CrossAccountCondition=true --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile
