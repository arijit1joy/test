#!/bin/bash

echo "Getting latest layer versions from the System Manager Parameter Store and replacing it in the parameter files . . ."
paramFile=$1
echo "Param File: ${paramFile}"
layerNames=$(python -c 'import json;layer_names=json.load(open("layers.json"));keys=list(layer_names.keys());vals=list(layer_names.values());print(" ".join([i+"~"+j for i,j in zip(keys, vals)]))')
echo $layerNames
for layerName in $layerNames;
do
    realLayerName=$(echo $layerName| cut -d'~' -f 2)
    cftParamName=$(echo $layerName| cut -d'~' -f 1)
    echo "current layer: ${realLayerName}"
    latestLayerResults=$(aws lambda list-layer-versions --layer-name "${realLayerName}")
    echo "Latest layer results: ${latestLayerResults}"
    latestLayerARN=$(echo $latestLayerResults | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["LayerVersions"][0]["LayerVersionArn"])')
    echo "Latest layer ARN: ${latestLayerARN}"
    echo "${cftParamName}*-*${latestLayerARN}*-*${paramFile}" | python -c 'import json,sys;layer_obj=sys.stdin.read();layer_names=json.load(open("layers.json"));param_values=layer_obj.strip().split("*-*");parameter_file=json.load(open(param_values[2]));parameter_file["Parameters"][param_values[0]]=param_values[1];json.dump(parameter_file, open(param_values[2], "w"))'
done;