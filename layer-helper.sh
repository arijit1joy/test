#!/bin/bash

echo "Getting latest layer versions from the System Manager Parameter Store and replacing it in the parameter files . . ."
paramFile=$1
echo "Param File: ${paramFile}"
LayerNames=$(python -c 'import json;layer_names=json.load(open("layers.json"));print(" ".join(layer_names))')
echo $LayerNames
for layerName in $LayerNames;
do
    echo "current layer: ${layerName}"
    latestLayerResults=$(aws lambda list-layer-versions --layer-name "${layerName}")
    echo "Latest layer results: ${latestLayerResults}"
    latestLayerARN=$(echo $latestLayerResults | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["LayerVersions"][0]["LayerVersionArn"])')
    echo "Latest layer ARN: ${latestLayerARN}"
    echo "${layerName}*-*${latestLayerARN}*-*${paramFile}" | python -c 'import json,sys;layer_obj=sys.stdin.read();param_values=layer_obj.strip().split("*-*");parameter_file=json.load(open(param_values[2]));parameter_file["Parameters"][param_values[0]]=param_values[1];json.dump(parameter_file, open(param_values[2], "w"))'
done;