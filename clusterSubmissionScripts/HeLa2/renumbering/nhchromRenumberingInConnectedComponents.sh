#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents

currentDirectory="$(dirname "${PWD}")"
cell=${currentDirectory##*/}

N_NODES=15
dataset=NHChrom

IFS=','
read -ra pathArray <<< "$(grep -i path, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"
read -ra setupAndIteration <<< "$(grep ,${dataset}, ~/Programming/hot-knife/COSEM/bestNetworks/$cell.csv)"


TRAININGPATH="${setupAndIteration[0]}/${pathArray[1]}${setupAndIteration[2]}.n5"
INPUTN5PATH="/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/$TRAININGPATH"
OUTPUTN5PATH="/groups/cosem/cosem/ackermand/testNewConnectedComponents/$TRAININGPATH"

mkdir -p $OUTPUTN5PATH
filename=$(basename -- "$fullfile")
cp $BASH_SOURCE $OUTPUTN5PATH/$filename

ARGV="\
--inputN5DatasetName 'NHChrom_maskedWith_nucleus' \
--minimumVolumeCutoff 8E3 \
--outputN5DatasetSuffix '_cc' \
--inputN5Path '$INPUTN5PATH' \
--outputN5Path '$OUTPUTN5PATH' \
--skipSmoothing
"

export RUNTIME="48:00"
export N_CORES_DRIVER=1
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
