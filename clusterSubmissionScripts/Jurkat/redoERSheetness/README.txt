er_reconstructed should be the same regardless of curvature metric since it is really just thresholding the expanded medial surface, so can remove files depending on that. ie masking or contact sites

really just need to redo anything that involves the actual sheetness

mkdir backupOldSheetness
for i in er_sheetness*; do if [ $i != "er_sheetnessVolumeAveraged_ccAt1SkipSmoothing"] ; then mv $i backupOldSheetness/; fi; done

steps:
1. curvature_er.sh
2. calculatePropertiesFromMedialSurface_er.sh
3. mask_er_with_nucleus.sh
4. mask_er_with_ribosomes.sh
5. calculateSheetnessAreaAndVolumeHistograms_erMasked.sh
(contact sites will have remained unchanged)
6. calculateSheetnessOfContactSites_mito.sh
7. calculateSheetnessOfContactSites_ribosomes.sh
8. separateRibosomes.sh
