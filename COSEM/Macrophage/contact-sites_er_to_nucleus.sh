#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

#for i in {'HeLa3.n5','HeLa3.n5','Jurkat.n5','Macrophage.n5'}; do cd $i; ln -s er_maskedWith_nucleus_expansion_150 er_contactDistance0; ln -s nucleus_expansion_150 nucleus_contactDistance0; ln -s er_contactDistance0 er_contactDistance0_contact_boundary_temp_to_delete; ln -s nucleus_contactDistance0 nucleus_contactDistance0_contact_boundary_temp_to_delete; cd ..; done

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=15

ARGV="\
--inputPairs 'er_contactDistance0_to_nucleus_contactDistance0'
--contactDistance 0 \
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Macrophage.n5' \
--minimumVolumeCutoff 0 \
--skipGeneratingContactBoundaries
"

export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
