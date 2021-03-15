#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=15

cell=${PWD##*/}

INPUTN5PATH=/groups/cosem/cosem/ackermand/testNewConnectedComponents/collected.n5

ARGV="--inputN5DatasetName 'NHChrom' \
--skipContactSites \
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/testNewConnectedComponents/analysisResults/generalObjectInformation/'"

export RUNTIME="48:00"
export N_CORES_DRIVER=1
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
