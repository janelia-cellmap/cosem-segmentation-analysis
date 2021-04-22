#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
export N_CORES_DRIVER=1

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.cosem.analysis.SparkProtrusionArea
N_NODES=10
ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/jrc_ctl-id8_a01.n5' \
--protrusionDataset '494_roi5_protrusions_id_2_cc' \
--protrusionCellDataset '494_roi5_id_2' \
--expandedCellDataset '494_roi5_id_1_expansion_1000' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/jrc_ctl-id8_a01' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
