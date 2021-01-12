#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkLengthAndThickness
N_NODES=10

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5'

ARGV="--inputN5DatasetName 'mito' \
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/HeLa3'
--minimumBranchLength 80 \
"

export RUNTIME="48:00"
export MEMORY_PER_NODE=500
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
