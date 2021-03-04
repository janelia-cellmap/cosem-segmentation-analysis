#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.saalfeldlab.hotknife.SparkGetRenumbering
N_NODES=10

currentDirectory="$(dirname "${PWD}")"
cell=${currentDirectory##*/}


ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5'  \
--inputN5DatasetName 'er,mito,MVB,nucleus,plasma_membrane,vesicle' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/renumbering/${cell}'
"

export N_CORES_DRIVER=1
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &


