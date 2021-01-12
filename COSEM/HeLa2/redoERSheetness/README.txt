er_reconstructed should be the same regardless of curvature metric since it is really just thresholding the expanded medial surface, so can remove files depending on that. ie masking or contact sites

really just need to redo anything that involves the actual sheetness

mkdir backupOldSheetness
for i in er_sheetness*; do if [ $i != "er_sheetnessVolumeAveraged_ccAt1SkipSmoothing"] ; then mv $i backupOldSheetness/; fi; done

steps:
x 1. curvature_er.sh
x 2. calculatePropertiesFromMedialSurface_er.sh
x 3. connected-components_er_sheetnessVolumeAveragedAt1.sh 
x 4. mask_er_with_nucleus.sh
x 5. mask_er_with_ribosomesAndMicrotubules.sh (for non-HeLa2, mask_er_with_ribosomes.sh)
x 6. mask_mito_with_er.sh
x 7. contact-sites_updatedUsingMaskedER.sh
x 8. contactSitesMicrotubulesContactDistance20.sh (for HeLa2 only)
x 9. calculateSheetnessAreaAndVolumeHistograms_erMasked.sh
x 10. calculateSheetnessOfContactSites_mito.sh
x 11. calculateSheetnessOfContactSites_ribosomes.sh
x 12. separateRibosomes.sh
x 13. general-object-cosem-information_er.sh
x 14. generalObjectCosemInformationMicrotubulesContactDistance20.sh (for HeLa2 only)
x 15. general-object-cosem-information_ribosomes.sh
