#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=20

cell=${PWD##*/}

INPUTN5PATH=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5

ARGV="--inputN5DatasetName 'plasma_membrane,er_maskedWith_plasma_membrane,cellVolume_volumeFiltered_filteredIDs_filled' \
--inputPairs 'er_maskedWith_plasma_membrane_to_plasma_membrane,plasma_membrane_to_mito' \
--skipContactSites \
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/${cell}/generalObjectInformation/'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 5


ARGV="--inputN5DatasetName 'plasma_membrane_maskedWith_microtubules' \
--skipContactSites \
--inputN5Path '$INPUTN5PATH/contactDistance20.n5' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/${cell}/generalObjectInformation/'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 5

ARGV="--inputPairs 'plasma_membrane_maskedWith_microtubules_to_microtubules' \
--skipContactSites \
--inputN5Path '$INPUTN5PATH/contactDistance20.n5' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/${cell}/generalObjectInformation/contactDistance20/'"


TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
