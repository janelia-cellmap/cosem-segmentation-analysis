#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=15

ARGV="\
--inputPairs 'er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150_to_mito,\
er_maskedWith_nucleus_expansion_150_to_mito,\
er_to_mito,\
er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150_to_ribosomes,\
er_maskedWith_nucleus_expansion_150_to_ribosomes,\
er_to_ribosomes,\
er_to_golgi,\
er_to_plasma_membrane,\
golgi_to_MVB,\
plasma_membrane_to_mito' \
--contactDistance 10 \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Jurkat.n5' \
--minimumVolumeCutoff 0 \
--skipGeneratingContactBoundaries
"

export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
