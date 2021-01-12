#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=10


mkdir -p $OUTPUTN5PATH
filename=$(basename -- "$fullfile")
cp $BASH_SOURCE $OUTPUTN5PATH/$filename

ARGV="\
--inputN5DatasetName 'er_sheetnessVolumeAveraged' \
--skipSmoothing \
--thresholdIntensityCutoff 230 \
--outputN5DatasetSuffix '_ccSkipSmoothing' \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
