#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=30

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5/contactDistance20.n5'

ARGV="--inputN5DatasetName 'er_reconstructed_maskedWith_microtubules,er_reconstructed_maskedWith_nucleus_expanded_maskedWith_microtubules' \
--inputPairs '\
er_reconstructed_maskedWith_microtubules_to_microtubules,\
er_reconstructed_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules' \
--skipContactSites \
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/HeLa2/generalObjectInformation/contactDistance20/'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
