#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem

JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

CLASS=org.janelia.saalfeldlab.hotknife.SparkGetRenumbering
N_NODES=10

currentDirectory="$(dirname "${PWD}")"
cell=${currentDirectory##*/}

#first do non microtubules (those that are not in contactDistance20.n5 directory)
#want er_reconstructed to have same id as er, but peripheral contact sites should have same ids as non peripheral since will be uploading all (and peripheral would be subset)
ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5'  \
--inputN5DatasetName 'er_reconstructed,er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes_to_ribosomes_cc,er_reconstructed_maskedWith_nucleus_expanded_to_mito_maskedWith_er_reconstructed_cc' \
--pathToUseForRenumbering '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/renumbered/${cell}.n5' \
--datasetToUseForRenumbering 'er,er_reconstructed_maskedWith_ribosomes_to_ribosomes_cc,er_reconstructed_to_mito_maskedWith_er_reconstructed_cc' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/renumbering/${cell}'
"

export RUNTIME="48:00"
export N_CORES_DRIVER=1
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

sleep 2

#then do microtubules
#ARGV="\
#--inputN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/${cell}.n5/contactDistance20.n5'  \
#--inputN5DatasetName 'er_reconstructed_maskedWith_nucleus_expanded_maskedWith_microtubules_to_microtubules_cc' \
#--pathToUseForRenumbering '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/renumbered/${cell}.n5/contactDistance20.n5'  \
#--datasetToUseForRenumbering 'er_reconstructed_maskedWith_microtubules_to_microtubules_cc' \
#--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/renumbering/${cell}/contactDistance20'
#"
#TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
