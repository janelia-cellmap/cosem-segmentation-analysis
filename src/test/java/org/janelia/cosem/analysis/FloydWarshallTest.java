package org.janelia.cosem.analysis;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.janelia.cosem.util.FloydWarshall;
import org.junit.Test;

public class FloydWarshallTest {

    private FloydWarshall setupTestFW() {
	Map<List<Integer>, Float> distanceMap = new HashMap<>();
	for (int i = 0; i < 10; i++) {
	    distanceMap.put(Arrays.asList(i, i + 1), 1.0f);
	}

	distanceMap.put(Arrays.asList(8, 11), 1.0f);
	distanceMap.put(Arrays.asList(11, 12), 1.0f);
	distanceMap.put(Arrays.asList(12, 13), 1.0f);
	distanceMap.put(Arrays.asList(13, 14), 1.0f);
	return new FloydWarshall(distanceMap);
    }

    @Test
    public void testBranchpoints() {
	FloydWarshall fw = setupTestFW();
	fw.getEndpointsAndBranchpoints();
	Set<Integer> branchpoint = new HashSet<Integer>(Arrays.asList(8));
	assertEquals(branchpoint, fw.branchpoints);
    }
    
    @Test
    public void testPruning() {
	FloydWarshall fw = setupTestFW();
	fw.pruneAndCalculateLongestShortestPathInformation(3);
	Set<Integer> prunedVertices = new HashSet<Integer>(Arrays.asList(9, 10));
	List<Integer> longestShortestPath = Arrays.asList(14, 13, 12, 11, 8, 7, 6, 5, 4, 3, 2, 1, 0);
	assertEquals(longestShortestPath, fw.longestShortestPath);
	assertEquals(prunedVertices, fw.prunedVertices);
    }

}
