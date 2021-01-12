#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkTopologicalThinning
N_NODES=10

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5'

ARGV="--inputN5DatasetName 'mito' \
--inputN5Path '$INPUTN5PATH' \
--outputN5DatasetSuffix '_skeleton' "

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
