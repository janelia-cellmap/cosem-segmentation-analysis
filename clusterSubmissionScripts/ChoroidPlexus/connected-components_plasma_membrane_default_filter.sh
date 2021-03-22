#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=15

INPUTN5PATH='/nrs/cosem/cosem/training/v0003.2/setup04/Choroid-Plexus_8x8x8nm/Choroid-Plexus_8x8x8nm_it975000.n5'
OUTPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/setup04/Choroid-Plexus_8x8x8nm/Choroid-Plexus_8x8x8nm_it975000.n5'

ARGV="\
--inputN5DatasetName 'plasma_membrane' \
--skipSmoothing \
--outputN5DatasetSuffix '_ccDefaultVolumeFilteredSkipSmoothing'
--inputN5Path '$INPUTN5PATH' \
--outputN5Path '$OUTPUTN5PATH' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
