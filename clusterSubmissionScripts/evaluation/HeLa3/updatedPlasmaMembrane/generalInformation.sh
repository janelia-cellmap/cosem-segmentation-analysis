#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=2

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa3
for i in refinedPredictions
do

for j in {whole,cropLeft,cropRight,cropFront,cropBack,cropUp,cropDown}
do


	ARGV="\
	--inputPairs 'er_to_plasma_membrane' \
	--inputN5DatasetName 'plasma_membrane' \
	--skipContactSites
	--inputN5Path '$BASENAME/${i}/${j}CC.n5' \
	--outputDirectory '$BASENAME/analysisResults/$i/$j'
	"
	sleep 2
	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV  &
done

done
