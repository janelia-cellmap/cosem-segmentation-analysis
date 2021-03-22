#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkFillHolesInConnectedComponents
N_NODES=10

#volume filter finished
#mv mito_maskedWith_ecs_largestComponent_expansion_150_cc_volumeFilteredTemp__filled_volumeFilter20E6 mito_maskedWith_ecs_largestComponent_expansion_150_cc_volumeFiltered20E6
ARGV="\
--inputN5DatasetName 'mito_maskedWith_ecs_largestComponent_expansion_150_cc_volumeFiltered20E6' \
--outputN5DatasetSuffix '_filled' \
--minimumVolumeCutoff 20E6 \
--skipVolumeFilter \
--inputN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/masking.n5' \
"

export RUNTIME="48:00"
export MEMORY_PER_NODE=500
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
