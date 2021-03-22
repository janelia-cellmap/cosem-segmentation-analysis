#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkFillHolesInConnectedComponents
N_NODES=10


ARGV="\
--inputN5DatasetName 'mito_maskedWith_ecs_cc_expansion_60_maskedWith_plasma_membrane_cc_expansion_30_cc' \
--outputN5DatasetSuffix '_filled_volumeFilter20E6' \
--minimumVolumeCutoff 20E6 \
--inputN5Path '/groups/cosem/cosem/ackermand/CLEM/TCell/LID494/imageData/masking.n5' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
