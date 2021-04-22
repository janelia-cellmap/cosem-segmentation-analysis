package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.janelia.cosem.analysis.SparkFillHolesInConnectedComponents;
import org.junit.Test;

public class SparkFillHolesInConnectedComponentsTest {
    
    @Test
    public void testFillHolesInConnectedComponents() throws Exception {
	SparkFillHolesInConnectedComponents.setupSparkAndFillHolesInConnectedComponents(TestHelper.testN5Locations, TestHelper.tempN5Locations, "shapes_cc", 0, "_filled", false, false);

	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc_filled"));
    }

}
