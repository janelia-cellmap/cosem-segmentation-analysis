#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkExpandMicrotubules
N_NODES=20

cell=${PWD##*/}

ARGV="--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5' \
--inputN5DatasetName 'microtubules_centerAxis' \
--innerRadiusInNm 6 \
--outerRadiusInNm 12.5 \
"


export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV

sleep 2
ln -s /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/microtubules_centerAxis_expanded /groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/microtubules
