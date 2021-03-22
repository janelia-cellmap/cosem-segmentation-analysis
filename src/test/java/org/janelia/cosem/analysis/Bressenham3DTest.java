package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.janelia.cosem.util.Bressenham3D;
import org.junit.Test;

public class Bressenham3DTest {

	@Test
	public void testGetLine() {
	    long [] start = {0,0,0};
	    long [] end = {5,5,5};
	    List<long[]> bressenham3Dline = Bressenham3D.getLine(start, end);

	    boolean areEqual = true;
	    for (int i = 0; i<=5; i++) {
	    	if(!Arrays.equals(bressenham3Dline.get(i),new long [] {i,i,i})) {
	    		areEqual=false;
	    	}
	    }
	    
	    assertTrue(areEqual);
	}

}
