#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=10


ARGV="\
--inputN5DatasetName 'mito_maskedWith_ecs_largestComponent_expansion_150' \
--minimumVolumeCutoff 0 \
--outputN5DatasetSuffix '_cc' \
--inputN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/masking.n5' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
