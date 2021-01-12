#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkFillHolesInConnectedComponents
N_NODES=20

cell=${PWD##*/}

ARGV="\
--inputN5DatasetName 'cellVolume_volumeFiltered_filteredIDs' \
--minimumVolumeCutoff 0 \
--skipVolumeFilter \
--outputN5DatasetSuffix '_filled' \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
