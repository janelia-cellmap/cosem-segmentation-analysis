#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=2

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/Jurkat
for i in {validation,rawPredictions,refinedPredictions}
do

for j in {whole,cropLeft,cropRight,cropFront,cropBack,cropUp,cropDown}
do

if [[ "$i" == "refinedPredictions" ]];
        then INPUTPAIRS=er_to_plasma_membrane,er_to_mito,er_to_nucleus,er_to_ribosomes,er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes,er_maskedWith_nucleus_expanded_to_mito_maskedWith_er,er_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes,er_reconstructed_maskedWith_nucleus_expanded_to_mito_maskedWith_er_reconstructed;
        else INPUTPAIRS=er_to_plasma_membrane,er_to_mito,er_to_nucleus,er_to_ribosomes;
fi

	ARGV="\
	--inputPairs '$INPUTPAIRS' \
	--contactDistance 10 \
	--inputN5Path '$BASENAME/$i/${j}CC.n5' \
	--minimumVolumeCutoff 0 \
	--skipGeneratingContactBoundaries
	"

        TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
        sleep 2
done

done
