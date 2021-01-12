#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=10

TRAININGPATH='setup03/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm_it825000.n5'
INPUTN5PATH="/nrs/cosem/cosem/training/v0003.2/$TRAININGPATH"
OUTPUTN5PATH="/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/$TRAININGPATH"

mkdir -p $OUTPUTN5PATH
filename=$(basename -- "$fullfile")
cp $BASH_SOURCE $OUTPUTN5PATH/$filename

ARGV="\
--inputN5DatasetName 'vesicle' \
--minimumVolumeCutoff 33E3 \
--skipSmoothing \
--outputN5DatasetSuffix '_ccSkipSmoothing' \
--inputN5Path '$INPUTN5PATH' \
--outputN5Path '$OUTPUTN5PATH' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
