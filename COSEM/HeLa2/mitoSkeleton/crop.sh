#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCrop
N_NODES=5

path=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5
datasetnames=mito,mito_skeleton,mito_skeleton_pruned,mito_skeleton_pruned_longestShortestPath

ARGV="--dimensions '1313,401,546' \
--offsetsToCropTo '9008,344,10540' \
--blockSize '128,128,128' \
--inputN5Path  '$path' \
--inputN5DatasetName '$datasetnames' \
--outputN5Path '/groups/cosem/cosem/ackermand/mitoFigure.n5' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2


