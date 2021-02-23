#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkSeparateRibosomes
N_NODES=15

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Macrophage.n5' \
--inputN5DatasetName 'ribosomes' \
--sheetnessCSV '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Macrophage/er_reconstructed_maskedWith_ribosomes_to_ribosomes_cc__ribosomes_sheetness.csv' \
--sheetnessMaskedCSV '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Macrophage/er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes_cc__ribosomes_sheetness.csv' \
--sheetnessCutoff 0.6 "

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
