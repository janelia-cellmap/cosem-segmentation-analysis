package org.janelia.saalfeldlab.hotknife;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.junit.Test;

public class SparkGeneralCosemObjectInformationTest {
    
    @Test
    public void testCalculatePropertiesFromMedialSurface() throws Exception {
	SparkGeneralCosemObjectInformation.setupSparkAndRunGeneralCosemObjectInformation("shapes_cc", TestHelper.testN5Locations, "shapes_cc_to_planes_cc", TestHelper.tempFileLocations, true, true);

	assertTrue(TestHelper.validationAndTestFilesAreEqual("shapes_cc.csv"));
	assertTrue(TestHelper.validationAndTestFilesAreEqual("shapes_cc_to_planes_cc_cc.csv"));
    }

}
