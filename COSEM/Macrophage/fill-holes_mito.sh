#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkFillHolesInConnectedComponents
N_NODES=10

INPUTN5PATH="/groups/cosem/cosem/ackermand/Cryo_FS80_Cell2_4x4x4nm_setup03_it1100000.n5"

ARGV="\
--inputN5DatasetName '50_0.7_smoothed' \
--minimumVolumeCutoff 20E6 \
--outputN5DatasetSuffix '_filled' \
--inputN5Path '$INPUTN5PATH' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
