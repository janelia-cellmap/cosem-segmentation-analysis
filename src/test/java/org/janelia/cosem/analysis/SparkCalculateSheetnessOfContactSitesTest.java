package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.janelia.cosem.analysis.SparkCalculateSheetnessOfContactSites;
import org.junit.Test;

public class SparkCalculateSheetnessOfContactSitesTest {
    
    @Test
    public void testCalculateSheetnessOfContactSites() throws Exception {
	SparkCalculateSheetnessOfContactSites.setupSparkAndCalculateSheetnessOfContactSites(TestHelper.testN5Locations, "shapes_cc_sheetnessVolumeAveraged", TestHelper.tempFileLocations, "shapes_cc_to_planes_cc_cc");
	
	assertTrue(TestHelper.validationAndTestFilesAreEqual("shapes_cc_to_planes_cc_cc__planes_cc_sheetness.csv"));
	assertTrue(TestHelper.validationAndTestFilesAreEqual("shapes_cc_to_planes_cc_cc_sheetnessSurfaceAreaHistograms.csv"));
	assertTrue(TestHelper.validationAndTestFilesAreEqual("shapes_cc_to_planes_cc_cc_sheetness.csv"));
    }

}
