#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
export N_CORES_DRIVER=1

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.cosem.analysis.SparkGeneralCosemObjectInformation
N_NODES=10

ARGV="--inputN5DatasetName '410_roi1' \
--skipContactSites \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/jrc_ctl-id8_a01.n5' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/jrc_ctl-id8_a01/generalObjectInformation/'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
