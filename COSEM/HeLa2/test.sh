#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkApplyMaskToCleanData
N_NODES=15

cell=${PWD##*/}
for dataset in {NE,NEChrom}
do

datasetNameToUseAsMask=nucleus
if [ "$dataset" = "NE" ]
then
datasetNameToUseAsMask=nucleus_expanded
fi

IFS=',' # hyphen (-) is set as delimiter
read -ra pathArray <<< "$(grep -i path, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"
read -ra setupAndIteration <<< "$(grep ,${dataset}, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"
echo ${setupAndIteration[0]} ${setupAndIteration[2]} $dataset $datasetNameToUseAsMask
done
