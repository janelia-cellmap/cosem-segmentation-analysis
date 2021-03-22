#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkIDFilter
N_NODES=3

for datasetName in {mito,mito_skeleton,mito_skeleton_pruned,mito_skeleton_pruned_longestShortestPath}
do


idsToKeep=225776320033

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/mitoFigure.n5'  \
--outputN5Path '/groups/cosem/cosem/ackermand/mitoFigure.n5/idFiltered.n5'
--inputN5DatasetName '$datasetName' \
--idsToKeep '$idsToKeep' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2

done
