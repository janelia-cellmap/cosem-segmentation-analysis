#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkTopologicalThinning
N_NODES=15

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/jrc_ctl-id8_a01.n5'

ARGV="--inputN5DatasetName 'cell_segmentation_filteredIDs_expandedSurfaceBy_500' \
--inputN5Path '$INPUTN5PATH' \
--outputN5DatasetSuffix '_medialSurface' \
--doMedialSurface"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
