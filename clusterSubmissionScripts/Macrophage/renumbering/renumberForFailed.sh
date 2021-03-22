#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.saalfeldlab.hotknife.SparkRenumberN5
N_NODES=10

currentDirectory="$(dirname "${PWD}")"
cell=${currentDirectory##*/}


ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5'  \
--inputN5DatasetName 'ribosomes,er_reconstructed_maskedWith_ribosomes_to_ribosomes_cc,er_maskedWith_golgi_to_golgi_cc,er_reconstructed_to_mito_maskedWith_er_reconstructed_cc,er_to_MVB_cc,er_maskedWith_plasma_membrane_to_plasma_membrane_cc,er_to_vesicle_cc,golgi_to_vesicle_cc,golgi_to_MVB_cc,plasma_membrane_to_mito_cc' \
--outputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/renumbered/${cell}.n5'  \
--inputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/renumbering/${cell}' \
--renumberingCSV 'ribosomes,er_reconstructed_maskedWith_ribosomes_to_ribosomes_cc,er_maskedWith_golgi_to_golgi_cc,er_reconstructed_to_mito_maskedWith_er_reconstructed_cc,er_to_MVB_cc,er_maskedWith_plasma_membrane_to_plasma_membrane_cc,er_to_vesicle_cc,golgi_to_vesicle_cc,golgi_to_MVB_cc,plasma_membrane_to_mito_cc'"

export N_CORES_DRIVER=1
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

