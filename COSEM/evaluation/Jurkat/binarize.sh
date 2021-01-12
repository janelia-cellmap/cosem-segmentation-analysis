#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkBinarize
N_NODES=2

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/Jurkat
for i in {validation,rawPredictions,refinedPredictions}
do

if [[ "$i" == "refinedPredictions" ]];
then ERTOMITO=er_maskedWith_nucleus_expanded_to_mito_maskedWith_er_cc;  
else ERTOMITO=er_to_mito_cc;
fi

	ARGV="\
	--inputN5DatasetName '$ERTOMITO,plasma_membrane,mito_membrane,mito,golgi,vesicle,MVB,er,nucleus,ribosomes' \
	--inputN5Path '$BASENAME/${i}CC.n5' \
	"

	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
done
