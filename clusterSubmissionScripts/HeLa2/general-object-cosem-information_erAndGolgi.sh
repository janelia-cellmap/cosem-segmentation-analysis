#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=10

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5'

ARGV="--inputN5DatasetName '\
er,\
er_maskedWith_nucleus_expanded,\
er_maskedWith_nucleus_expanded_maskedWith_ribosomes,\
er_reconstructed,\
er_reconstructed_maskedWith_nucleus_expanded,\
er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes'
--inputPairs '\
er_maskedWith_nucleus_expanded_to_mito_maskedWith_er,\
er_to_mito_maskedWith_er,\
er_reconstructed_to_mito_maskedWith_er_reconstructed,\
er_reconstructed_maskedWith_nucleus_expanded_to_mito_maskedWith_er_reconstructed,\
er_maskedWith_ribosomes_to_ribosomes,\
er_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes,\
er_reconstructed_maskedWith_ribosomes_to_ribosomes,\
er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes,\
er_maskedWith_plasma_membrane_to_plasma_membrane,\
er_maskedWith_golgi_to_golgi,\
er_to_MVB,\
er_to_vesicle,\
golgi_to_MVB,\
golgi_to_vesicle'
--skipContactSites \
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/HeLa2/generalObjectInformation/'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
