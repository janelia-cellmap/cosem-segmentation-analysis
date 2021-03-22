#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkIDFilter
N_NODES=3

for datasetName in {microtubules,er,er_maskedWith_nucleus_expanded,nucleus,MVB,mito,vesicle,golgi,golgi_maskedWith_microtubules_to_microtubules_cc,er_maskedWith_microtubules_to_microtubules_cc,nucleus_maskedWith_microtubules_to_microtubules_cc,er_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules_cc,MVB_maskedWith_microtubules_to_microtubules_cc,mito_maskedWith_microtubules_to_microtubules_cc,vesicle_maskedWith_microtubules_to_microtubules_cc}
do


idsToKeep=''
case $datasetName in

	microtubules) idsToKeep=44275 ;;
	er) idsToKeep=58756962048 ;;
	er_maskedWith_nucleus_expanded) idsToKeep=58756962048;;
	nucleus) idsToKeep=76655033983 ;;
	MVB) idsToKeep=40362354095,37808418016,41860074048 ;;
	mito) idsToKeep=0 ;;
	vesicle) idsToKeep=43837350141,43222926120,43645314134,42742962112 ;;
	golgi) idsToKeep=45545346185 ;;
	golgi_maskedWith_microtubules_to_microtubules_cc) idsToKeep=41802402098,42186294100,42147894100 ;; 
	er_maskedWith_microtubules_to_microtubules_cc) idsToKeep=40938426088,42224754102,39671346052,42051966095 ;;
	er_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules_cc) idsToKeep=40938426088,42224754102,42051966095 ;;
	nucleus_maskedWith_microtubules_to_microtubules_cc) idsToKeep=39632934050 ;;
	MVB_maskedWith_microtubules_to_microtubules_cc) idsToKeep=40612026093,39498306047,42359202101 ;;
	mito_maskedWith_microtubules_to_microtubules_cc) idsToKeep=0 ;;
	vesicle_maskedWith_microtubules_to_microtubules_cc) idsToKeep=43837398137,43223070117,43664586132,42762294111 ;;

esac

ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/figure3Microtubules/44275.n5'  \
--outputN5Path '/groups/cosem/cosem/ackermand/figure3Microtubules/44275.n5/idFiltered.n5'
--inputN5DatasetName '$datasetName' \
--idsToKeep '$idsToKeep' \
--outputN5DatasetSuffix ''
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
sleep 2

done
