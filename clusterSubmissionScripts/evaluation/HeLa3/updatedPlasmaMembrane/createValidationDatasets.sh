#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkCreateValidationDatasets
N_NODES=3

ARGV="\
--n5PathRefinedPredictions '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--n5PathRawPredictions '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/rawPredictions/HeLa3.n5' \
--n5PathValidationData '/groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5/volumes/groundtruth/0003/Crop111/labels' \
--outputPath '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa3/'
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV

