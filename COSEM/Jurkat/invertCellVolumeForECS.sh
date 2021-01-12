#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkInvertLabel
N_NODES=15

cell=${PWD##*/}

ARGV="\
--inputN5DatasetName 'cellVolume_volumeFiltered_filteredIDs_filled' \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5' \
--outputN5DatasetName 'ecs' 
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
