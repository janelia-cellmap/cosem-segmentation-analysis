#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=15

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5'

ARGV="--inputN5DatasetName '\
er_reconstructed_maskedWith_nucleus_expanded,\
er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes,\
ribosomes,ribosomes_cytosolic,ribosomes_nuclear,ribosomes_sheet,ribosomes_tube'
--inputPairs '\
er_reconstructed_maskedWith_nucleus_expanded_to_mito_maskedWith_er_reconstructed,\
er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes' \
--skipContactSites \
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/HeLa3/generalObjectInformation/'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
