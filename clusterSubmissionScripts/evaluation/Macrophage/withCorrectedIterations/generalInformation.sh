#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=2

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/Macrophage
for i in {rawPredictions,refinedPredictions} #{validation,rawPredictions,refinedPredictions}
do

for j in {whole,cropLeft,cropRight,cropFront,cropBack,cropUp,cropDown}
do

INPUTPAIRS=er_to_mito #just to have something there
if [[ "$i" == "refinedPredictions" ]];
        then 
	INPUTDATASETS=vesicle;
        else 
	INPUTPAIRS=er_to_mito,er_to_nucleus;
	INPUTDATASETS=mito,nucleus,vesicle;
fi

	ARGV="\
	--inputPairs '$INPUTPAIRS' \
	--inputN5DatasetName '$INPUTDATASETS' \
	--skipContactSites
	--inputN5Path '$BASENAME/${i}/${j}CC.n5' \
	--outputDirectory '$BASENAME/analysisResults/$i/$j'
	"

	sleep 2
	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV & 
done

done
