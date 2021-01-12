#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkSeparateRibosomes
N_NODES=15

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Jurkat.n5' \
--inputN5DatasetName 'ribosomes' \
--sheetnessCSV '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Jurkat/er_reconstructed_maskedWith_ribosomes_to_ribosomes_cc__ribosomes_sheetness.csv' \
--sheetnessMaskedCSV '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Jurkat/er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes_cc__ribosomes_sheetness.csv' \
--sheetnessCutoff 0.6 "

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
