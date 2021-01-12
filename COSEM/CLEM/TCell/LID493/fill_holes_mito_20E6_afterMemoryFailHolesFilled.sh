#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkFillHolesInConnectedComponents
N_NODES=10

#symbolic links after failed stuff
#lrwxrwxrwx  1 ackermand cosem  124 Jul  8 20:19 mito_maskedWith_ecs_cc_expansion_60_maskedWith_plasma_membrane_cc_expansion_30_cc_volumeFilter20E6 -> mito_maskedWith_ecs_cc_expansion_60_maskedWith_plasma_membrane_cc_expansion_30_cc_volumeFilteredTemp_filled_volumeFilter20E6
#lrwxrwxrwx  1 ackermand cosem  130 Jul  8 20:19 mito_maskedWith_ecs_cc_expansion_60_maskedWith_plasma_membrane_cc_expansion_30_cc_volumeFilter20E6_holes -> mito_maskedWith_ecs_cc_expansion_60_maskedWith_plasma_membrane_cc_expansion_30_cc_volumeFilteredTemp_filled_volumeFilter20E6_holes

ARGV="\
--inputN5DatasetName 'mito_maskedWith_ecs_cc_expansion_60_maskedWith_plasma_membrane_cc_expansion_30_cc_volumeFilter20E6' \
--outputN5DatasetSuffix '_filled' \
--minimumVolumeCutoff 20E6 \
--skipVolumeFilter \
--skipCreatingHoleDataset \
--inputN5Path '/groups/cosem/cosem/ackermand/CLEM/TCell/LID493/imageData/masking.n5' \
"

export RUNTIME="48:00"
export MEMORY_PER_NODE=500
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
