#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=15

TRAININGPATH='setup01/Jurkat_Cell1_4x4x4nm/Jurkat_Cell1_FS96-Area1_4x4x4nm_it875000.n5'
INPUTN5PATH="/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/$TRAININGPATH"

mkdir -p $OUTPUTN5PATH
filename=$(basename -- "$fullfile")
cp $BASH_SOURCE $OUTPUTN5PATH/$filename

ARGV="\
--inputN5DatasetName 'ERES_maskedWith_er_expansion_0' \
--minimumVolumeCutoff 300E3 \
--outputN5DatasetSuffix '_cc' \
--inputN5Path '$INPUTN5PATH' \
--skipSmoothing \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
