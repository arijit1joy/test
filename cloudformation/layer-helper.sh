#!/bin/bash

# NOTE: Please Do Not Delete This File! It is for Future Implementation

echo "Getting latest layer versions from the System Manager Parameter Store and replacing it in the parameter files . . ."
paramFile=$1
echo "Param File: ${paramFile}"
environment=$(echo $paramFile| cut -d'.' -f 2| cut -d'/' -f 2| cut -d'-' -f 2)
echo "Environment: ${environment}"
accountId=$(echo $environment| python -c 'import json,sys;environment=sys.stdin.read().strip();account_id=json.load(open("layers.json"))[environment];print(account_id)')
echo "AccountID: ${accountId}"
layerNames=$(python -c 'import json;layer_names=json.load(open("layers.json"))["layers"];keys=list(layer_names.keys());vals=list(layer_names.values());print(" ".join([i+"~"+j for i,j in zip(keys, vals)]))')
echo $layerNames
for layerName in $layerNames;
do
    realLayerName=$(echo $layerName| cut -d'~' -f 2)
    cftParamName=$(echo $layerName| cut -d'~' -f 1)
    echo "current layer: ${realLayerName}"
    latestLayerResults=$(aws lambda list-layer-versions --layer-name "${realLayerName}")
    currentEnvironmentARN=$(echo $latestLayerResults | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["LayerVersions"][0]["LayerVersionArn"])')
    echo "Latest layer results: ${latestLayerResults}"
    echo "Latest Dev layer ARN: ${currentEnvironmentARN}"
    if [ $environment != "dev" ] ; then 
        echo "Getting the latest layer for the Environment: ${environment}"
        latestLayerVersion=${currentEnvironmentARN##*:}
        echo "Latest Layer Version: ${latestLayerVersion}"
        currentEnvironmentARN=$(echo $currentEnvironmentARN | sed "s/732927748536/${accountId}/");
        currentEnvironmentARN=$(echo $currentEnvironmentARN | sed "s/us-east-1/us-east-2/");
        echo "Latest ${environment} layer ARN: ${currentEnvironmentARN}"
        while !(latestLayerResults=$(aws lambda get-layer-version-by-arn --arn "${currentEnvironmentARN}" --region "us-east-2")) && (($latestLayerVersion > 0));
        do
            echo "Could not find this layer version: ${currentEnvironmentARN}!"
            latestLayerVersion=$((latestLayerVersion - 1))
            currentEnvironmentARN="${currentEnvironmentARN::-1}${latestLayerVersion}"
            echo "New current Layer Version ARN to check for: ${currentEnvironmentARN}"
        done;
        if (($latestLayerVersion > 0)); then
            latestLayerResults=$(aws lambda get-layer-version-by-arn --arn "${currentEnvironmentARN}" --region "us-east-2")
            echo "Latest Layer Results: ${latestLayerResults}"
        else
            echo "The layer does not exist in the ${environment} environment"
        fi;
        if latestLayerARN=$(echo $latestLayerResults | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["LayerVersionArn"])'); then
            echo "${cftParamName}*-*${latestLayerARN}*-*${paramFile}" | python -c 'import json,sys;layer_obj=sys.stdin.read();layer_names=json.load(open("layers.json"));param_values=layer_obj.strip().split("*-*");parameter_file=json.load(open(param_values[2]));parameter_file["Parameters"][param_values[0]]=param_values[1];json.dump(parameter_file, open(param_values[2], "w"))'
        fi;
    else
        echo "${cftParamName}*-*${currentEnvironmentARN}*-*${paramFile}" | python -c 'import json,sys;layer_obj=sys.stdin.read();layer_names=json.load(open("layers.json"));param_values=layer_obj.strip().split("*-*");parameter_file=json.load(open(param_values[2]));parameter_file["Parameters"][param_values[0]]=param_values[1];json.dump(parameter_file, open(param_values[2], "w"))'
    fi;
done;