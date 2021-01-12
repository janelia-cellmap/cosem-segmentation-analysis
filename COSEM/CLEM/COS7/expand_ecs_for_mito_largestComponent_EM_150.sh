#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkExpandMaskToCleanPredictions
N_NODES=10

ARGV="--datasetToMaskN5Path '/groups/cosem/cosem/ackermand/CLEM/imageData/masking.n5' \
--datasetNameToMask 'mito' \
--onlyKeepLargestComponent \
--skipConnectedComponents \
--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/CLEM/imageData/masking.n5' \
--datasetNameToUseAsMask 'ecs' \
--expansion 150 \
--outputN5Path '/groups/cosem/cosem/ackermand/CLEM/imageData/masking.n5'"

export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
