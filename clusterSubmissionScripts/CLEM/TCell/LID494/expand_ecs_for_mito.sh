#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkExpandMaskToCleanPredictions
N_NODES=10

ARGV="--datasetToMaskN5Path '/groups/cosem/cosem/ackermand/CLEM/TCell/LID494/imageData/masking.n5' \
--datasetNameToMask 'mito' \
--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/CLEM/TCell/LID494/imageData/masking.n5' \
--datasetNameToUseAsMask 'ecs_cc' \
--expansion 60 \
--skipConnectedComponents \
--outputN5Path '/groups/cosem/cosem/ackermand/CLEM/TCell/LID494/imageData/masking.n5'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
