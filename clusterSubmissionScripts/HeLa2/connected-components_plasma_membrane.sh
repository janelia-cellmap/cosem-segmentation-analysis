#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=10

INPUTN5PATH='/nrs/cosem/cosem/training/v0003.2/setup03/HeLa_Cell2_4x4x4nm/HeLa_Cell2_4x4x4nm_it650000.n5'
OUTPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/setup03/HeLa_Cell2_4x4x4nm/HeLa_Cell2_4x4x4nm_it650000.n5'

ARGV="\
--inputN5DatasetName 'plasma_membrane' \
--onlyKeepLargestComponent \
--skipSmoothing \
--outputN5DatasetSuffix '_largestComponentSkipSmoothing'
--inputN5Path '$INPUTN5PATH' \
--outputN5Path '$OUTPUTN5PATH' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
