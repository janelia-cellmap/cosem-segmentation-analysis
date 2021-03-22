package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.janelia.cosem.analysis.SparkContactSites;
import org.junit.Test;

public class SparkContactSitesTest {
    
    @Test
    public void testContactSites() throws IOException {
	SparkContactSites.setupSparkAndCalculateContactSites(TestHelper.testN5Locations, TestHelper.tempN5Locations, "shapes_cc,planes_cc", null, 10, 1, false,false,false);
	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc_to_planes_cc_cc"));
    }

}
