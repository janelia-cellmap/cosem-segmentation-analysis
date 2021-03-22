package org.janelia.cosem.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;

@SuppressWarnings("serial")
public class BlockInformation implements Serializable {
	/**
	 * Class to contain relevant block information for doing COSEM analysis
	 */
	public long[][] gridBlock;
	public long[][] paddedGridBlock;
	public boolean[][][] thinningLocations;
	public int[][] padding;
	public int paddingForMedialSurface;
	
	public boolean needToThinAgainPrevious;
	public boolean needToThinAgainCurrent;
	public boolean isIndependent;
	public boolean areObjectsTouching;
		
	public Map<Long,Long> edgeComponentIDtoVolumeMap;
	public Map<Long,Long> currentContactingPairEdgeComponentIDtoVolumeMap;
	public Map<Long, Long> edgeComponentIDtoRootIDmap;
	public Map<Long, long[][]> objectIDtoBoundingBoxMap;
	public Map<Long, List<Long>> edgeComponentIDtoOrganelleIDs;//for contact sites
	public Set<Long> selfContainedMaxVolumeOrganelles;
	public Long selfContainedMaxVolume;
	public Set<Long> maxVolumeObjectIDs;
	public Set<Long> allRootIDs;
	
	public BlockInformation() {
		this.selfContainedMaxVolume =0L;
		this.selfContainedMaxVolumeOrganelles = new HashSet<Long>();
		this.allRootIDs = new HashSet<Long>();
	}
	
	public BlockInformation(long[][] gridBlock, Map<Long,Long> edgeComponentIDs,
			Map<Long, Long> edgeComponentIDtoRootIDmap) {
		this.edgeComponentIDtoVolumeMap = edgeComponentIDs;
		this.edgeComponentIDtoRootIDmap = edgeComponentIDtoRootIDmap;
		this.gridBlock = gridBlock;
		this.selfContainedMaxVolume =0L;
		this.selfContainedMaxVolumeOrganelles = new HashSet<Long>();
		this.allRootIDs = new HashSet<Long>();
		this.currentContactingPairEdgeComponentIDtoVolumeMap = new HashMap<Long,Long>();


	}
	
	public BlockInformation(long[][] gridBlock, long[][] paddedGridBlock, int[][] padding, Map<Long,Long> edgeComponentIDs,
			Map<Long, Long> edgeComponentIDtoRootIDmap) {
		this.edgeComponentIDtoVolumeMap = edgeComponentIDs;
		this.edgeComponentIDtoRootIDmap = edgeComponentIDtoRootIDmap;
		this.gridBlock = gridBlock;
		this.padding = padding;
		this.paddedGridBlock = paddedGridBlock;
		this.needToThinAgainPrevious = true;
		this.needToThinAgainCurrent = true;
		this.isIndependent = false;
		this.areObjectsTouching = true;
		this.selfContainedMaxVolume =0L;
		this.thinningLocations = new boolean[3][3][3];
	}
	
	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws IOException {
		//Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		
		//Build list
		return buildBlockInformationList(outputDimensions, blockSize);
	}
	
	public static  List<BlockInformation> buildBlockInformationList(long [] outputDimensions, int [] blockSize){
		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new ArrayList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			long[][] currentGridBlock = gridBlockList.get(i);
			blockInformationList.add(new BlockInformation(currentGridBlock, null, null));
		}
		return blockInformationList;
	}
	
	public long [] getPaddedOffset(long padding) {
		long [] offset = this.gridBlock[0];
		return new long[]{offset[0]-padding, offset[1]-padding, offset[2]-padding};
	}
	
	public long [] getPaddedDimension(long padding) {
		long [] dimension = this.gridBlock[1];
		return new long[]{dimension[0]+2*padding, dimension[1]+2*padding, dimension[2]+2*padding};
	}
}