#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCellVolume
N_NODES=10

ARGV="--ecsN5Path '/nrs/cosem/cosem/training/v0003.2/setup45/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm_it725000.n5' \
--plasmaMembraneN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--maskN5Path '/groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5' \
--minimumVolumeCutoff 0 \
"

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
