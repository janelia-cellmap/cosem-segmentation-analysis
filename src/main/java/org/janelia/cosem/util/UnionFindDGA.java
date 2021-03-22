package org.janelia.cosem.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.imglib2.algorithm.util.unionfind.IntArrayRankedUnionFind;

public class UnionFindDGA {
	//Algorithm based on wiki: https://en.wikipedia.org/wiki/Disjoint-set_data_structure
	
	public Map<Long, Long> globalIDtoRootID;
	public Map<Long, Integer> globalIDtoRank;

	public UnionFindDGA(Set<List<Long>> initialGlobalIDtoGlobalID) {
		this.globalIDtoRootID = new HashMap<Long, Long>();
		this.globalIDtoRank = new HashMap<Long, Integer>();
		for (final List<Long> currentPair : initialGlobalIDtoGlobalID) {
			long globalID1 = currentPair.get(0);
			long globalID2 = currentPair.get(1);
			globalIDtoRootID.put(globalID1, globalID1);
			globalIDtoRootID.put(globalID2, globalID2);
			globalIDtoRank.put(globalID1, 0);
			globalIDtoRank.put(globalID2, 0);
		}

		for (List<Long> currentPair: initialGlobalIDtoGlobalID) {
			long globalID1 = currentPair.get(0);
			long globalID2 = currentPair.get(1);
			union(globalID1, globalID2);
		}
		
	}
	
	public UnionFindDGA(long[][] initialGlobalIDtoGlobalID) {
		this.globalIDtoRootID = new HashMap<Long, Long>();
		this.globalIDtoRank = new HashMap<Long, Integer>();
		for (int i = 0; i < initialGlobalIDtoGlobalID.length; i++) {
			long globalID1 = initialGlobalIDtoGlobalID[i][0];
			long globalID2 = initialGlobalIDtoGlobalID[i][1];
			globalIDtoRootID.put(globalID1, globalID1);
			globalIDtoRootID.put(globalID2, globalID2);
			globalIDtoRank.put(globalID1, 0);
			globalIDtoRank.put(globalID2, 0);
		}

		for (int i = 0; i < initialGlobalIDtoGlobalID.length; i++) {
			long globalID1 = initialGlobalIDtoGlobalID[i][0];
			long globalID2 = initialGlobalIDtoGlobalID[i][1];
			union(globalID1, globalID2);
		}
		
	}

	public long findRoot(long globalID) {
		if (globalIDtoRootID.get(globalID) != globalID) {
			globalIDtoRootID.put(globalID, findRoot(globalIDtoRootID.get(globalID)));
		}
		return globalIDtoRootID.get(globalID);

	}
	
	public void getFinalRoots() {
		for (Map.Entry<Long, Long> entry : globalIDtoRootID.entrySet()) {
			findRoot(entry.getKey());			
		}		
	}
	
	public void getRenumberedRoots() {
		getFinalRoots();
		renumberRoots();
	}
	
	
	public void renumberRoots() {
		Map <Long, Long> renumberRootsMap = new HashMap<Long, Long>();
		long count = 1;
		for (Map.Entry<Long, Long> entry : globalIDtoRootID.entrySet()) {
			long key = entry.getKey();
			long root = findRoot(key);
			
			if (!renumberRootsMap.containsKey(root)) {
				renumberRootsMap.put(root, count);
				count++;
			}
		}
		
		for (Map.Entry<Long, Long> entry : globalIDtoRootID.entrySet()) {
			long key = entry.getKey();
			long value = entry.getValue();			
			globalIDtoRootID.put(key, renumberRootsMap.get(value));
		}		
	}

	public void union(long globalID1, long globalID2) {
		long globalID1Root = findRoot(globalID1);
		long globalID2Root = findRoot(globalID2);

		// globalID1 and globalID2 are already in the same set
		if (globalID1Root == globalID2Root)
			return;

		// globalID1 and globalID2 are not currently in the same set, so merge them
		if (globalIDtoRank.get(globalID1) < globalIDtoRank.get(globalID2)) {
			// swap roots
			long tmp = globalID1Root;
			globalID1Root = globalID2Root;
			globalID2Root = tmp;
		}
		// merge globalID2Root into globalID1Root
		globalIDtoRootID.put(globalID2Root, globalID1Root);
		
		if (globalIDtoRank.get(globalID1Root) == globalIDtoRank.get(globalID2Root))
			globalIDtoRank.put(globalID1Root, globalIDtoRank.get(globalID1Root) + 1);
	}

	public static void main(final String[] args) {
		long[][] initialGlobalIDtoGlobalID = { {1,1}, { 1, 3 }, {3,3}, { 3, 2 },{2,20}, {14,14}, { 7, 6 }, { 5, 6 }, { 10, 5 }, {3,14}, { 9, 7 }, { 4, 7 },
				{ 7, 8 }, { 8, 10 }, { 8, 12 }, { 8, 15 }, { 16, 17 },{14,1}};
		//long [][] initialGlobalIDtoGlobalID = {{1,2},{2,3},{3,4},{5,3},{6,7},{8,6},{6,7},{6,1}};
		/*long[][] initialGlobalIDtoGlobalID = new long [1000][2];
		for(int i=0; i< 1000;i++) {
			initialGlobalIDtoGlobalID[i][0]=(long)(Math.random()*200);
			initialGlobalIDtoGlobalID[i][1]=(long)(Math.random()*200);
		}*/
		long startTime = System.currentTimeMillis();
		UnionFindDGA testing = new UnionFindDGA(initialGlobalIDtoGlobalID);
		for (Map.Entry<Long, Long> entry : testing.globalIDtoRootID.entrySet()) {
			System.out.println("final " + entry.getKey() + ":" + entry.getValue().toString());
		}

		testing.renumberRoots();
		for (Map.Entry<Long, Long> entry : testing.globalIDtoRootID.entrySet()) {
			System.out.println("renumbered " + entry.getKey() + ":" + entry.getValue());
		}
		System.out.println(System.currentTimeMillis() - startTime);

		startTime = System.currentTimeMillis();
		IntArrayRankedUnionFind arrayRankedUnionFind = new IntArrayRankedUnionFind(18);
		for (int i = 0; i < initialGlobalIDtoGlobalID.length; i++) {
			long globalID1 = initialGlobalIDtoGlobalID[i][0];
			long globalID2 = initialGlobalIDtoGlobalID[i][1];
			arrayRankedUnionFind.join(arrayRankedUnionFind.findRoot(globalID1),
					arrayRankedUnionFind.findRoot(globalID2));
		}
		int [] temp = new int [18];
		for (int i = 0; i < 18; i++) {
			temp[i]=arrayRankedUnionFind.findRoot(i);
			System.out.println("theirs " + i + " " + temp[i]);
		}
		System.out.println(System.currentTimeMillis() - startTime);
	}
}
