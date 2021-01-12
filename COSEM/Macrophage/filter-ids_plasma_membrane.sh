#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkIDFilter
N_NODES=10

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/setup03/Macrophage_FS80_Cell2_4x4x4nm/Cryo_FS80_Cell2_4x4x4nm_it650000.n5'  \
--inputN5DatasetName 'plasma_membrane_ccDefaultVolumeFilteredSkipSmoothing' \
--idsToKeep '151182472083,7226470714,102302596680,112203357404,71542934922,55643507776,30243963613,80962618194,146343126983,194385169510,174184309189,29884274633,197206478274,71342965327,205145709055,55962952593,95022537236' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
