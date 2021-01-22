package org.janelia.saalfeldlab.hotknife;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.junit.Test;

public class SparkFillHolesInConnectedComponentsTest {
    
    @Test
    public void testConnectedComponents() throws Exception {
	SparkFillHolesInConnectedComponents.setupSparkAndFillHolesInConnectedComponents(TestHelper.testN5Locations, "shapes_cc", 0, "_filled", false, false);
	assertTrue(TestHelper.validationAndTestN5sAreEqual("shapes_cc_filled"));
    }

}
