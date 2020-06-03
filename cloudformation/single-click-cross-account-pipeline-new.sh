#!/usr/bin/env bash
echo -n "Enter ToolsAccount > "
read ToolsAccount
echo -n "Enter ToolsAccount ProfileName for AWS Cli operations> "
read ToolsAccountProfile
echo -n "Enter Dev Account > "
read DevAccount
echo -n "Enter DevAccount ProfileName for AWS Cli operations> "
read DevAccountProfile
echo -n "Enter Test Account > "
read TestAccount
echo -n "Enter TestAccount ProfileName for AWS Cli operations> "
read TestAccountProfile
echo -n "Enter Stage Account > "
read StageAccount
echo -n "Enter StageAccount ProfileName for AWS Cli operations> "
read StageAccountProfile
echo -n "Enter Prod Account > "
read ProdAccount
echo -n "Enter ProdAccount ProfileName for AWS Cli operations> "
read ProdAccountProfile

aws configure --profile ToolsAccountProfile set region us-east-1
#----------- DEV Brnach Pipeline Creation ------------------
aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src-dev --template-file ToolsAcct/pre-reqs-dev.yaml --parameter-overrides DevAccount=$DevAccount --profile $ToolsAccountProfile --region us-east-1
#aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount --profile $ToolsAccountProfile

echo -n "Enter S3 Bucket created from above > "
read S3Bucket

echo -n "Enter CMK ARN created from above > "
read CMKArn

aws configure --profile ToolsAccountProfile set region us-east-1

echo -n "Executing in Dev Account"
aws cloudformation deploy --stack-name da-edge-j1939-codepipeline-cloudformation-dev-role --template-file TestAccount/toolsacct-codepipeline-cloudformation-deployer-dev.yaml --capabilities CAPABILITY_NAMED_IAM --parameter-overrides ToolsAccount=$ToolsAccount CMKARN=$CMKArn  S3Bucket=$S3Bucket --profile $DevAccountProfile --region us-east-1

echo -n "Creating Pipeline in Tools Account"
aws cloudformation deploy --stack-name da-edge-j1939-pipeline-dev --template-file ToolsAcct/code-pipeline-dev.yaml --parameter-overrides DevAccount=$DevAccount CMKARN=$CMKArn S3Bucket=$S3Bucket CrossAccountCondition=true --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1
#aws cloudformation deploy --stack-name da-edge-j1939-pipeline --template-file ToolsAcct/code-pipeline.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount CMKARN=$CMKArn S3Bucket=$S3Bucket S3BucketProd=$S3BucketProd CMKARNPROD=$CMKArnProd  --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1

echo -n "Adding Permissions to the CMK"
aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src-dev --template-file ToolsAcct/pre-reqs-dev.yaml --parameter-overrides CodeBuildCondition=true --profile $ToolsAccountProfile --region us-east-1


# #----------- Test Branch Pipeline Creation ------------------

# aws configure --profile ToolsAccountProfile set region us-east-2

# aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src-test --template-file ToolsAcct/pre-reqs-test.yaml --parameter-overrides TestAccount=$DevAccount --profile $ToolsAccountProfile --region us-east-2
# #aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount --profile $ToolsAccountProfile

# echo -n "Enter Test S3 Bucket created from above > "
# read S3BucketTest

# echo -n "Enter Test CMK ARN created from above > "
# read CMKArnTest


# echo -n "Executing for Test Account"
# aws cloudformation deploy --stack-name da-edge-j1939-codepipeline-cloudformation-test-role --template-file TestAccount/toolsacct-codepipeline-cloudformation-deployer-test.yaml --capabilities CAPABILITY_NAMED_IAM --parameter-overrides ToolsAccount=$ToolsAccount CMKARNTest=$CMKArnTest  S3BucketTest=$S3BucketTest S3Bucket=$S3Bucket --profile $TestAccountProfile --region us-east-2

# echo -n "Creating Pipeline in Tools Account"
# aws cloudformation deploy --stack-name da-edge-j1939-pipeline-test --template-file ToolsAcct/code-pipeline-test.yaml --parameter-overrides CMKARN=$CMKArn TestAccount=$TestAccount CMKARNTest=$CMKArnTest S3BucketTest=$S3BucketTest S3Bucket=$S3Bucket CrossAccountCondition=true --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1
# #aws cloudformation deploy --stack-name da-edge-j1939-pipeline --template-file ToolsAcct/code-pipeline.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount CMKARN=$CMKArn S3Bucket=$S3Bucket S3BucketProd=$S3BucketProd CMKARNPROD=$CMKArnProd  --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1

# echo -n "Adding Permissions to the CMK"
# aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src-test --template-file ToolsAcct/pre-reqs-test.yaml --parameter-overrides CodeBuildCondition=true --profile $ToolsAccountProfile --region us-east-2


# #----------- Stage Branch Pipeline Creation ------------------

# aws configure --profile TestAccountProfile set region us-east-2

# aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src-stage --template-file ToolsAcct/pre-reqs-stage.yaml --parameter-overrides TestAccount=$TestAccount StageAccount=$StageAccount  --profile $ToolsAccountProfile --region us-east-2
# #aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount --profile $ToolsAccountProfile

# echo -n "Enter Stage S3 Bucket created from above > "
# read S3BucketStage

# echo -n "Enter Stage CMK ARN created from above > "
# read CMKArnStage

# echo -n "Executing for Stage Account"
# aws cloudformation deploy --stack-name da-edge-j1939-codepipeline-cloudformation-stage-role --template-file TestAccount/toolsacct-codepipeline-cloudformation-deployer-stage.yaml --capabilities CAPABILITY_NAMED_IAM --parameter-overrides ToolsAccount=$ToolsAccount CMKARNStage=$CMKArnStage S3Bucket=$S3Bucket  S3BucketStage=$S3BucketStage StageAccount=$StageAccount --profile $StageAccountProfile --region us-east-2

# echo -n "Creating Pipeline in Tools Account"
# aws cloudformation deploy --stack-name da-edge-j1939-pipeline-stage --template-file ToolsAcct/code-pipeline-stage.yaml --parameter-overrides CMKARN=$CMKArn TestAccount=$TestAccount CMKARNStage=$CMKArnStage S3BucketStage=$S3BucketStage S3Bucket=$S3Bucket StageAccount=$StageAccount CrossAccountCondition=true --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1
# #aws cloudformation deploy --stack-name da-edge-j1939-pipeline --template-file ToolsAcct/code-pipeline.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount CMKARN=$CMKArn S3Bucket=$S3Bucket S3BucketProd=$S3BucketProd CMKARNPROD=$CMKArnProd  --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1

# echo -n "Adding Permissions to the CMK"
# aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src-stage --template-file ToolsAcct/pre-reqs-stage.yaml --parameter-overrides CodeBuildCondition=true --profile $ToolsAccountProfile --region us-east-2


# #----------- Prod Branch Pipeline Creation ------------------

# aws configure --profile TestAccountProfile set region us-east-2

# aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src-prod --template-file ToolsAcct/pre-reqs-prod.yaml --parameter-overrides TestAccount=$TestAccount ProdAccount=$ProdAccount --profile $ToolsAccountProfile --region us-east-2
# #aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src --template-file ToolsAcct/pre-reqs.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount --profile $ToolsAccountProfile

# echo -n "Enter S3 Bucket created from above > "
# read S3BucketProd

# echo -n "Enter CMK ARN created from above > "
# read CMKArnProd

# echo -n "Executing for Prod Account"
# aws cloudformation deploy --stack-name da-edge-j1939-codepipeline-cloudformation-prod-role --template-file TestAccount/toolsacct-codepipeline-cloudformation-deployer-prod.yaml --capabilities CAPABILITY_NAMED_IAM --parameter-overrides ToolsAccount=$ToolsAccount ProdAccount=$ProdAccount CMKARNProd=$CMKArnProd  S3BucketProd=$S3BucketProd S3Bucket=$S3Bucket --profile $ProdAccountProfile --region us-east-2

# echo -n "Creating Pipeline in Tools Account"
# aws cloudformation deploy --stack-name da-edge-j1939-pipeline-prod --template-file ToolsAcct/code-pipeline-prod.yaml --parameter-overrides CMKARN=$CMKArn TestAccount=$TestAccount CMKARNProd=$CMKArnProd S3BucketProd=$S3BucketProd S3Bucket=$S3Bucket ProdAccount=$ProdAccount CrossAccountCondition=true --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1
# #aws cloudformation deploy --stack-name da-edge-j1939-pipeline --template-file ToolsAcct/code-pipeline.yaml --parameter-overrides DevAccount=$DevAccount StageAccount=$StageAccount CMKARN=$CMKArn S3Bucket=$S3Bucket S3BucketProd=$S3BucketProd CMKARNPROD=$CMKArnProd  --capabilities CAPABILITY_NAMED_IAM --profile $ToolsAccountProfile --region us-east-1

# echo -n "Adding Permissions to the CMK"
# aws cloudformation deploy --stack-name da-edge-pre-reqs-j1939-src-prod --template-file ToolsAcct/pre-reqs-prod.yaml --parameter-overrides CodeBuildCondition=true --profile $ToolsAccountProfile --region us-east-2