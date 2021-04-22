#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
export N_CORES_DRIVER=1

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.cosem.analysis.SparkExpandDataset
N_NODES=15

ARGV="--inputN5DatasetName '494_roi5_id_1' \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/jrc_ctl-id8_a01.n5' \
--thresholdIntensityCutoff 0 \
--expansionInNm 1000 \
--outputN5DatasetSuffix '_expansion_1000'"

export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
