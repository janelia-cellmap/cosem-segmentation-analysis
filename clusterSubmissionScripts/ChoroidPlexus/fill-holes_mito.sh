#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkFillHolesInConnectedComponents
N_NODES=15

INPUTN5PATH="/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/setup04/Choroid-Plexus_8x8x8nm/Choroid-Plexus_8x8x8nm_it875000.n5"

ARGV="\
--inputN5DatasetName 'mito_cc' \
--minimumVolumeCutoff 20E6 \
--outputN5DatasetSuffix '_filled' \
--inputN5Path '$INPUTN5PATH' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
