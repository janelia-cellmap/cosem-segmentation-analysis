#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=15

cell=${PWD##*/}

ARGV="\
--inputPairs '\
er_to_vesicle,\
golgi_to_vesicle' \
--contactDistance 10 \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5' \
--minimumVolumeCutoff 0 \
--skipGeneratingContactBoundaries
"


export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2
