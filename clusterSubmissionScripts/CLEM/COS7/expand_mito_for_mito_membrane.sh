#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/backup-hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkExpandMaskToCleanPredictions
N_NODES=15

ARGV="--datasetToMaskN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/masking.n5' \
--datasetNameToMask 'mito_membrane' \
--skipConnectedComponents \
--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/masking.n5' \
--datasetNameToUseAsMask 'mito_maskedWith_ecs_largestComponent_expansion_150_cc_volumeFiltered20E6_filled' \
--expansion 16 \
--keepWithinMask \
--outputN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/masking.n5'"

export MEMORY_PER_NODE=500
export N_EXECUTORS_PER_NODE=3
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
