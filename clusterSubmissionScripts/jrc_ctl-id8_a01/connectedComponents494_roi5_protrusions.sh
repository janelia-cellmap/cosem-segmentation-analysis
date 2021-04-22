#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
export N_CORES_DRIVER=1

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.cosem.analysis.SparkConnectedComponents
N_NODES=10

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/jrc_ctl-id8_a01.n5' \
--inputN5DatasetName '494_roi5_protrusions_id_2' \
--skipSmoothing \
--thresholdIntensityCutoff 1 \
--minimumVolumeCutoff 0 \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
