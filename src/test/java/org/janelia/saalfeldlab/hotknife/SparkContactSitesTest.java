package org.janelia.saalfeldlab.hotknife;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.junit.Test;

public class SparkContactSitesTest {
    
    @Test
    public void testConnectedComponents() throws IOException {
	SparkContactSites.setupSparkAndCalculateContactSites(TestConstants.tempFileLocations, TestConstants.tempFileLocations, "cylinderAndRectangle_cc,twoPlanes_cc", null, 10, 1, false,false,false);
	boolean areEqual = SparkCompareDatasets.setupSparkAndCompare(TestConstants.testFileLocations, TestConstants.tempFileLocations, "cylinderAndRectangle_cc_to_twoPlanes_cc_cc");
	assertTrue(areEqual);
    }

}
