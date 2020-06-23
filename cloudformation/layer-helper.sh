#!/bin/bash

echo "Getting latest layer versions from the System Manager Parameter Store and replacing it in the parameter files . . ."
paramFile=$1
echo "Param File: ${paramFile}"
# environment=$(echo $paramFile| cut -d'-' -f 2)
layerNames=$(python -c 'import json;layer_names=json.load(open("layers.json"));keys=list(layer_names.keys());vals=list(layer_names.values());print(" ".join([i+"~"+j for i,j in zip(keys, vals)]))')
echo $layerNames
for layerName in $layerNames;
do
    realLayerName=$(echo $layerName| cut -d'~' -f 2)
    cftParamName=$(echo $layerName| cut -d'~' -f 1)
    echo "current layer: ${realLayerName}"

    awsConfigList=$(aws configure list)
    echo "AWS Config List 1: ${awsConfigList}"

    aws configure set profile.stage.role_arn arn:aws:iam::732927748536:role/da-edge-j1939-services-CodeBuildRole-stage
    aws configure set profile.stage.source_profile default
    awsConfigList=$(aws configure list)
    echo "AWS Config List 2: ${awsConfigList}"

    latestLayerResults=$(aws lambda list-layer-versions --layer-name "${realLayerName}" --region "us-east-2" --profile stage)
    echo "Latest layer results - Dev: ${latestLayerResults}"

    aws configure set profile.stage.role_arn arn:aws:iam::170736887717:role/da-edge-j1939-services-cloudformationdeployer-stage-role
    aws configure set profile.stage.source_profile default
    awsConfigList=$(aws configure list)
    echo "AWS Config List 3: ${awsConfigList}"

    latestLayerResults=$(aws lambda list-layer-versions --layer-name "${realLayerName}" --region "us-east-2" --profile stage)
    echo "Latest layer results - Stage: ${latestLayerResults}"

    
    awsConfigList=$(aws configure list)
    echo "AWS Config List 4: ${awsConfigList}"


    # latestLayerARN=$(echo $latestLayerResults | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["LayerVersions"][0]["LayerVersionArn"])')
    # echo "Latest layer ARN: ${latestLayerARN}"
    # latestLayerVersion=${latestLayerARN##*:}
    # aws lambda remove-layer-version-permission --layer-name "${realLayerName}" --version-number "${latestLayerVersion}" --statement-id "${realLayerName}_layer_CA_permission_SID"
    # aws lambda add-layer-version-permission --layer-name "${realLayerName}" --statement-id "${realLayerName}_layer_CA_permission_SID" --action lambda:GetLayerVersion --principal 170736887717 --version-number "${latestLayerVersion}" --output json
    # echo "${cftParamName}*-*${latestLayerARN}*-*${paramFile}" | python -c 'import json,sys;layer_obj=sys.stdin.read();layer_names=json.load(open("layers.json"));param_values=layer_obj.strip().split("*-*");parameter_file=json.load(open(param_values[2]));parameter_file["Parameters"][param_values[0]]=param_values[1];json.dump(parameter_file, open(param_values[2], "w"))'
done;