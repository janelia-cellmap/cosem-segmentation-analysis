package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.io.IOException;

import org.janelia.cosem.analysis.SparkGeneralCosemObjectInformation;
import org.janelia.cosem.analysis.SparkRenumberN5;
import org.junit.Test;

public class SparkRenumberN5Test {
    
    @Test
    public void testConnectedComponents() throws Exception {
	SparkRenumberN5.setupSparkAndRenumberN5(TestHelper.testFileLocations, "shapes_cc", TestHelper.testN5Locations, TestHelper.tempN5Locations, "shapes_cc", true); 
	SparkGeneralCosemObjectInformation.setupSparkAndRunGeneralCosemObjectInformation("shapes_cc_renumbered", TestHelper.tempN5Locations, null, TestHelper.tempFileLocations, true, true);
	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc_renumbered"));
    }

}
