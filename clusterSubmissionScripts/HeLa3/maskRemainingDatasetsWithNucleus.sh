#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkApplyMaskToCleanData
N_NODES=15

cell=${PWD##*/} 
for dataset in {NE,NHChrom}
do

datasetNameToUseAsMask=nucleus
if [ "$dataset" = "NE" ]
then
datasetNameToUseAsMask=nucleus_expanded
fi

IFS=',' # hyphen (-) is set as delimiter
read -ra pathArray <<< "$(grep -i path, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"
read -ra setupAndIteration <<< "$(grep ,${dataset}, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"


TRAININGPATH="${setupAndIteration[0]}/${pathArray[1]}${setupAndIteration[2]}.n5"
INPUTN5PATH="/nrs/cosem/cosem/training/v0003.2/$TRAININGPATH"
OUTPUTN5PATH="/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/$TRAININGPATH"

mkdir -p $OUTPUTN5PATH
filename=$(basename -- "$fullfile")
cp $BASH_SOURCE $OUTPUTN5PATH/$filename

ARGV="\
--datasetToMaskN5Path '$INPUTN5PATH' \
--datasetNameToMask '$dataset' \
--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5' \
--datasetNameToUseAsMask '$datasetNameToUseAsMask' \
--outputN5Path '$OUTPUTN5PATH' \
--keepWithinMask \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2

done

