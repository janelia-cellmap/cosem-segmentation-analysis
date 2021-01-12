#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=2

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa3
i=rawPredictions #{rawPredictions,refinedPredictions} #{validation,rawPredictions,refinedPredictions}

for j in {whole,cropLeft,cropRight,cropFront,cropBack,cropUp,cropDown}
do

INPUTPAIRS=er_to_mito,er_to_nucleus;

	ARGV="\
	--inputPairs '$INPUTPAIRS' \
	--contactDistance 10 \
	--inputN5Path '$BASENAME/$i/${j}CC.n5' \
	--minimumVolumeCutoff 0 \
	"

	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
	sleep 2 
done

