#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkIDFilter
N_NODES=10

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/setup03/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm_it650000.n5'  \
--inputN5DatasetName 'plasma_membrane_ccDefaultVolumeFilteredSkipSmoothing' \
--idsToKeep '117817749215,105387956894,108020761256,115159455920,67952919544,8487617456,142992015976,60493388129,124300276411' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
