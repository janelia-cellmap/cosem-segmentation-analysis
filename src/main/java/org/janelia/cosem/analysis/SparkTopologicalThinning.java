/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.cosem.analysis;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.Skeletonize3D_;
import org.janelia.cosem.util.Grid;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Class to do topological thinning, ie, skeletonization and medial surface finding. Default is skeletonization unless --doMedialSurface is true
 *
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkTopologicalThinning {
	public static final int pad = 50;//I think for doing 6 borders (N,S,E,W,U,B) where we do the 8 indpendent iterations, the furthest a voxel in a block can be affected is from something 48 away, so add 2 more just as extra border
	
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/data.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/skeletonization.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _skeleton so output would be organelle_skeleton")
		private String outputN5DatasetSuffix = "";
		
		@Option(name = "--doMedialSurface", required = false, usage = "Whether to do do medial surface, by default is set to false and skeletonization is performed")
		private Boolean doMedialSurface = false;

		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);

				if (outputN5Path == null)
					outputN5Path = inputN5Path;

				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}

		public String getOutputN5DatasetSuffix() {
			return outputN5DatasetSuffix;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public Boolean getDoMedialSurface() {
			return doMedialSurface;
		}

	}
	
	/**
	 * Perform a topological thinning operation.
	 *
	 * Takes in an input dataset path and name, an output dataset path and name, whether or not to do medial surface thinning and the current thinning iteration
	 *
	 * @param sc
	 * @param n5Path
	 * @param originalInputdatsetName
	 * @param n5OutputPath
	 * @param originalOutputDatasetName
	 * @param doMedialSurface
	 * @param blockInformationList
	 * @param iteration
	 * @throws IOException
	 */
	public static <T extends IntegerType<T> & NativeType<T>> List<BlockInformation> performTopologicalThinningIteration(final JavaSparkContext sc, final String n5Path,
			final String originalInputDatasetName, final String n5OutputPath, String originalOutputDatasetName, boolean doMedialSurface,
			List<BlockInformation> blockInformationList, final int iteration) throws IOException {

		//For each iteration, create the correspondingly correct input/output datasets. The reason for this is that we do not want to overwrite data in one block that will be read in by another block that has yet to be processed
		final String inputDatasetName = originalOutputDatasetName+(iteration%2==0 ? "_odd" : "_even");
		final String outputDatasetName = originalOutputDatasetName+(iteration%2==0 ? "_even" : "_odd");
		
		N5Reader n5Reader = null;
		DatasetAttributes attributes = null;
		if(iteration == 0) {
			n5Reader = new N5FSReader(n5Path);
			attributes = n5Reader.getDatasetAttributes(originalInputDatasetName);
		}
		else {
			n5Reader = new N5FSReader(n5OutputPath);
			attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		}
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		
		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, attributes.getDataType(), new GzipCompression());
		n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, originalInputDatasetName)));
		
		List<BlockInformation> blockInformationListThinningRequired = new LinkedList<BlockInformation>();
		for(BlockInformation currentBlockInformation : blockInformationList) {
			if(currentBlockInformation.needToThinAgainCurrent || iteration<=1) {//need two iterations to complete to ensure block is in _even and _odd
				blockInformationListThinningRequired.add(currentBlockInformation);
			}
		}
		
		long tic = System.currentTimeMillis();

		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationListThinningRequired);
		JavaRDD<BlockInformation> updatedBlockInformationThinningRequired = rdd.map(blockInformation -> {
			
			//Get relevant block informtation
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			//long [] paddedOffset = blockInformation.paddedGridBlock[0];
			//long [] paddedDimension = blockInformation.paddedGridBlock[1];
			int [][] padding = blockInformation.padding;
			int [] paddingNeg = padding[0];
			int [] paddingPos = padding[1];
			long [] paddedOffset = new long[] {offset[0]-paddingNeg[0], offset[1]-paddingNeg[1], offset[2]-paddingNeg[2]};
			long [] paddedDimension = new long[] {dimension[0]+(paddingNeg[0]+paddingPos[0]), dimension[1]+(paddingNeg[1]+paddingPos[1]), dimension[2]+(paddingNeg[2]+paddingPos[2])};

			
			//Input source is now the previously completed iteration image, and output is initialized to that
			String currentInputDatasetName;
			N5FSReader n5BlockReader = null;
			if(iteration==0) {
				currentInputDatasetName = originalInputDatasetName;
				n5BlockReader = new N5FSReader(n5Path);
			}
			else {
				currentInputDatasetName = inputDatasetName;
				n5BlockReader = new N5FSReader(n5OutputPath);

			}
			final RandomAccessibleInterval<T> previousThinningResult = (RandomAccessibleInterval<T>)N5Utils.open(n5BlockReader, currentInputDatasetName);
			IntervalView<T> thinningResultCropped = Views.offsetInterval(Views.extendZero(previousThinningResult), paddedOffset, paddedDimension);

			//IntervalView<UnsignedLongType> outputImage = Views.offsetInterval(Views.extendValue(previousThinningResult, new UnsignedLongType(0)), paddedOffset, paddedDimension);
			//IntervalView<UnsignedLongType> outputImage = Views.offsetInterval(ArrayImgs.unsignedLongs(paddedDimension),new long[]{0,0,0}, paddedDimension);
			
			
			//Assume we don't need to thin again and that the output image to write is the appropriately cropped version of outputImage
			
			//For skeletonization and medial surface:
			//All blocks start off dependent, so it will only be independent after it made it through one iteration, ensuring all blocks are checked.
			//Perform thinning, then check if block is independent. If so, complete the block.
			blockInformation = updateThinningResult(thinningResultCropped, padding, paddedOffset, paddedDimension, doMedialSurface, blockInformation ); //to prevent one skeleton being created for two distinct objects that are touching	
			IntervalView<T> croppedOutputImage = Views.offsetInterval(thinningResultCropped, new long[] {paddingNeg[0],paddingNeg[1],paddingNeg[2]}, dimension);
			//Write out current thinned block and return block information updated with whether it needs to be thinned again
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(croppedOutputImage, n5BlockWriter, outputDatasetName, gridBlock[2]);
			
			return blockInformation;
			
			
		});
		
		//Figure out whether another thinning iteration is needed. Update blockInformationList
		boolean needToThinAgain = false;
		LinkedList<BlockInformation> updatedBlockInformationThinningRequiredList = new LinkedList<BlockInformation>(updatedBlockInformationThinningRequired.collect());
		/*for(int i=updatedBlockInformationThinningRequiredList.size()-1; i>=0; i--) {
			BlockInformation currentBlockInformation = updatedBlockInformationThinningRequiredList.get(i);
			
			
			//If a block is completed it needs to appear in both the _even and _odd outputs, otherwise update it and process again
			if(currentBlockInformation.isIndependent && (!currentBlockInformation.needToThinAgainPrevious && !currentBlockInformation.needToThinAgainCurrent)) {// if current block is independent and had no need to thin over two iterations, then can stop processing it since it will be identical in even/odd outputs
				blockInformationList.remove(i);
			}
			else {
				needToThinAgain |= currentBlockInformation.needToThinAgainCurrent;
				currentBlockInformation.needToThinAgainPrevious = currentBlockInformation.needToThinAgainCurrent;
				updatedBlockInformationThinningRequiredList.set(i,currentBlockInformation);
			}
			
		}*/
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(dateFormat.format(date)+" Timing: "+(System.currentTimeMillis()-tic)/1000.0+" Number of Remaining Blocks: "+blockInformationListThinningRequired.size()+", Full iteration complete: "+iteration);
		
		
		HashMap<List<Long>,BlockInformation> blockWasThinnedInPreviousIterationMap = new HashMap<List<Long>,BlockInformation>();
		for(int i=0; i<updatedBlockInformationThinningRequiredList.size(); i++) {
			BlockInformation currentBlockInformation = updatedBlockInformationThinningRequiredList.get(i);
			needToThinAgain |= currentBlockInformation.needToThinAgainCurrent;
			currentBlockInformation.needToThinAgainPrevious = currentBlockInformation.needToThinAgainCurrent;
			updatedBlockInformationThinningRequiredList.set(i,currentBlockInformation);
			blockWasThinnedInPreviousIterationMap.put(Arrays.asList(currentBlockInformation.gridBlock[0][0], currentBlockInformation.gridBlock[0][1], currentBlockInformation.gridBlock[0][2]), currentBlockInformation);
			if(updatedBlockInformationThinningRequiredList.size()<=10) {
				if(currentBlockInformation.needToThinAgainCurrent) {
					System.out.println(Arrays.toString(currentBlockInformation.gridBlock[0])+" "+currentBlockInformation.isIndependent+" "+Arrays.toString(currentBlockInformation.padding[0]));
					System.out.println(Arrays.toString(currentBlockInformation.gridBlock[0])+" "+currentBlockInformation.isIndependent+" "+Arrays.toString(currentBlockInformation.padding[1]));
				}
				
			}
		}
		
		for(int i=0; i<blockInformationList.size(); i++) {
			BlockInformation currentBlockInformation = blockInformationList.get(i);
			long[] currentBlockOffset = currentBlockInformation.gridBlock[0];
			currentBlockInformation = blockWasThinnedInPreviousIterationMap.getOrDefault(Arrays.asList(currentBlockOffset[0],currentBlockOffset[1],currentBlockOffset[2]),currentBlockInformation);
			if(currentBlockInformation.isIndependent) {
				currentBlockInformation.padding = new int[2][3];
			}
			else if(!currentBlockInformation.needToThinAgainCurrent){ //then only need to check this round if any of its 26 neighbors changed in the appropriate places
				currentBlockInformation.padding = new int[][] {{50,50,50},{50,50,50}};//need at least a +1 padding in every direction since start thinning at 1 in
				outerloop:
				for(int deltaX = -1; deltaX <= 1; deltaX++) {
					for(int deltaY = -1; deltaY <= 1; deltaY++) {
						for(int deltaZ = -1; deltaZ <= 1; deltaZ++) {
							List<Long> neighboringBlockOffset = Arrays.asList(currentBlockOffset[0]+deltaX*blockSize[0], 
									currentBlockOffset[1]+deltaY*blockSize[1],
									currentBlockOffset[2]+deltaZ*blockSize[2]);
							
							//convert detlaX to index to check in neighboring block to see if need to thin again
							List<Integer> xIndices = 1-deltaX==1 ? Arrays.asList(0,1,2) : Arrays.asList(1-deltaX);
							List<Integer> yIndices = 1-deltaY==1 ? Arrays.asList(0,1,2) : Arrays.asList(1-deltaY);
							List<Integer> zIndices = 1-deltaZ==1 ? Arrays.asList(0,1,2) : Arrays.asList(1-deltaZ);
							
							boolean[][][] thinningLocations = blockWasThinnedInPreviousIterationMap.containsKey(neighboringBlockOffset) ? 
									blockWasThinnedInPreviousIterationMap.get(neighboringBlockOffset).thinningLocations
									: new boolean[3][3][3];
							for(int xIndex: xIndices) {
								for(int yIndex: yIndices) {
									for(int zIndex: zIndices) {
										if(thinningLocations[xIndex][yIndex][zIndex]) {
											currentBlockInformation.needToThinAgainCurrent = true;
											/*if(deltaX == -1) currentBlockInformation.padding[0][0] = pad;
											else if(deltaX == 1) currentBlockInformation.padding[1][0] = pad;
											if(deltaY == -1) currentBlockInformation.padding[0][1] = pad;
											else if(deltaY == 1) currentBlockInformation.padding[1][1] = pad;
											if(deltaZ == -1) currentBlockInformation.padding[0][2] = pad;
											else if(deltaZ == 1) currentBlockInformation.padding[1][2] = pad;*/
											break outerloop;
										}
									}
								}
							}
							
							
						}
					}
				}
			}
		
			blockInformationList.set(i,currentBlockInformation);

		}
		
		if(!needToThinAgain)
			blockInformationList = new LinkedList<BlockInformation>();
		
		return blockInformationList;
	}
	
	
	private static <T extends IntegerType<T>> Map<Long,Set<Long>> getTouchingObjectIDs(IntervalView<T> thinningResult, long[] paddedDimension) {
		Map<Long,Set<Long>> touchingObjectIDs = new HashMap<Long,Set<Long>>();
		RandomAccess<T> thinningResultRandomAccess = thinningResult.randomAccess();
		for(int x=0; x<paddedDimension[0]; x++) {
			for(int y=0; y<paddedDimension[1]; y++) {
				for(int z=0; z<paddedDimension[2]; z++) {
					thinningResultRandomAccess.setPosition(new int[] {x,y,z});
					long objectID = thinningResultRandomAccess.get().getIntegerLong();
					if(objectID>0) {
						for(int dx=-1; dx<=1; dx++) {
							for(int dy=-1; dy<=1; dy++) {
								for(int dz=-1; dz<=1; dz++) {
									int newX = x+dx;
									int newY = y+dy;
									int newZ = z+dz;
									if(newX>=0 && newX<paddedDimension[0] && newY>=0 & newY<paddedDimension[1] && newZ>=0 && newZ<paddedDimension[2]) {//Then still inside block
										thinningResultRandomAccess.setPosition(new int[] {newX,newY,newZ});
										long neighboringObjectID = thinningResultRandomAccess.get().getIntegerLong();
										if(neighboringObjectID>0 && neighboringObjectID!=objectID) {//then two objects are touching
											Set<Long> objectIDTouchers = touchingObjectIDs.getOrDefault(objectID, new HashSet<Long>());
											objectIDTouchers.add(neighboringObjectID);
											touchingObjectIDs.put(objectID, objectIDTouchers);
											
											Set<Long> neighboringIDTouchers = touchingObjectIDs.getOrDefault(neighboringObjectID, new HashSet<Long>());
											neighboringIDTouchers.add(objectID);
											touchingObjectIDs.put(neighboringObjectID, neighboringIDTouchers);
										}
									}
								}
							}
						}
					}
					
				}
			}
		}
		return touchingObjectIDs;
	}

	private static <T extends IntegerType<T>> BlockInformation updateThinningResult(IntervalView<T> thinningResult, int [][] padding, long [] paddedOffset, long [] paddedDimension, boolean doMedialSurface, BlockInformation blockInformation ) {
		blockInformation.thinningLocations = new boolean[3][3][3];
		Map<Long,Set<Long>> touchingObjectIDs = null;
		if(blockInformation.areObjectsTouching) {//check if objects are still touching
			touchingObjectIDs = getTouchingObjectIDs(thinningResult, paddedDimension);
			blockInformation.areObjectsTouching = touchingObjectIDs.size()>0;
		}
		//System.out.println("Are objects touching:"+ blockInformation.areObjectsTouching);
		if(blockInformation.areObjectsTouching) {
			blockInformation = thinTouchingObjectsIndependently(thinningResult, padding, paddedOffset, paddedDimension, doMedialSurface, touchingObjectIDs, blockInformation );
		}
		else {
			blockInformation = thinEverythingTogether(thinningResult, padding, paddedOffset, paddedDimension, doMedialSurface, blockInformation );
		}
		return blockInformation;
	}
	
	private static <T extends IntegerType<T>> BlockInformation thinTouchingObjectsIndependently(IntervalView<T> thinningResult, int [][] padding, long [] paddedOffset, long [] paddedDimension, boolean doMedialSurface, Map<Long,Set<Long>> touchingObjectIDs, BlockInformation blockInformation ){
		blockInformation.needToThinAgainCurrent = false;
		//blockInformation.isIndependent = true;
		
		Cursor<T> thinningResultCursor = thinningResult.localizingCursor();
		thinningResultCursor.reset();
		
		Set<Long> objectIDsInBlockLeftToProcess = new HashSet<Long>();
		while(thinningResultCursor.hasNext()) {
			Long objectID = thinningResultCursor.next().getIntegerLong();
			if (objectID >0) {
				objectIDsInBlockLeftToProcess.add(objectID);
			}
		}
		
		boolean isIndependent = true;
		while(objectIDsInBlockLeftToProcess.size()>0) {
			Set<Long> currentIndependentSetOfObjectIDsToProcess = new HashSet<>(objectIDsInBlockLeftToProcess);
			for(Long currentID : objectIDsInBlockLeftToProcess) {
				if(currentIndependentSetOfObjectIDsToProcess.contains(currentID)) {
					currentIndependentSetOfObjectIDsToProcess.removeAll(touchingObjectIDs.getOrDefault(currentID, new HashSet<Long>()));//remove all that were touching object
				}
			}
			objectIDsInBlockLeftToProcess.removeAll(currentIndependentSetOfObjectIDsToProcess);
				
			IntervalView<UnsignedByteType> current = null;
			Cursor<UnsignedByteType> currentCursor = null;
			//System.out.println(currentIndependentSetOfObjectIDsToProcess.size());
			thinningResultCursor.reset();
			current = Views.offsetInterval(ArrayImgs.unsignedBytes(paddedDimension),new long[]{0,0,0}, paddedDimension);
			currentCursor = current.cursor();
			while(thinningResultCursor.hasNext()) { //initialize to only look at current object
				thinningResultCursor.next();
				currentCursor.next();
				if(currentIndependentSetOfObjectIDsToProcess.contains(thinningResultCursor.get().getIntegerLong()))
					currentCursor.get().set(1);
			}
		
			Skeletonize3D_ skeletonize3D = new Skeletonize3D_(current, padding, paddedOffset);
			if(doMedialSurface) {
				blockInformation.needToThinAgainCurrent  |= skeletonize3D.computeMedialSurfaceIteration();
				if(!blockInformation.isIndependent) {
					isIndependent &= skeletonize3D.isMedialSurfaceBlockIndependent();
				}
			}
			else {
				blockInformation.needToThinAgainCurrent  |= skeletonize3D.computeSkeletonIteration();
				if(!blockInformation.isIndependent) {
					isIndependent &= skeletonize3D.isSkeletonBlockIndependent();
				}
			}
			
			//update output
			currentCursor.reset();
			thinningResultCursor.reset();//update in case need to rethin
			while(currentCursor.hasNext()) {
				currentCursor.next();
				thinningResultCursor.next();
				if(currentCursor.get().get() ==0) {
					if (currentIndependentSetOfObjectIDsToProcess.contains(thinningResultCursor.get().getIntegerLong())) {
						blockInformation = updateBlockInformationThinningLocations(thinningResultCursor, padding, paddedDimension, blockInformation);
						thinningResultCursor.get().setInteger(0);//Then this voxel was thinned out
					}
				}
			}
		}
		blockInformation.isIndependent = isIndependent;
		//previous thinning result should now equal the current thinning result
		return blockInformation;
	}
	
	private static <T extends IntegerType<T>> BlockInformation thinEverythingTogether(IntervalView<T> thinningResult, int [][] padding, long [] paddedOffset, long [] paddedDimension, boolean doMedialSurface, BlockInformation blockInformation ){
		blockInformation.needToThinAgainCurrent = false;
		//blockInformation.isIndependent = true;
		
		Cursor<T> thinningResultCursor = thinningResult.localizingCursor();
		thinningResultCursor.reset();
		
		IntervalView<UnsignedByteType> current = Views.offsetInterval(ArrayImgs.unsignedBytes(paddedDimension),new long[]{0,0,0}, paddedDimension); //need this as unsigned byte type
		RandomAccess<UnsignedByteType> currentRandomAccess = current.randomAccess();
		Cursor<UnsignedByteType> currentCursor = current.cursor();	
		while(thinningResultCursor.hasNext()) {
			long objectID = thinningResultCursor.next().getIntegerLong();
			if (objectID >0) {
				int [] pos = new int [] {thinningResultCursor.getIntPosition(0), thinningResultCursor.getIntPosition(1), thinningResultCursor.getIntPosition(2)};
				currentRandomAccess.setPosition(pos);
				currentRandomAccess.get().set(1);
			}
		}
		
	
		Skeletonize3D_ skeletonize3D = new Skeletonize3D_(current, padding, paddedOffset);
		if(doMedialSurface) {
			blockInformation.needToThinAgainCurrent  |= skeletonize3D.computeMedialSurfaceIteration();
			if(!blockInformation.isIndependent) {
				blockInformation.isIndependent = skeletonize3D.isMedialSurfaceBlockIndependent();
			}
		}
		else {
			blockInformation.needToThinAgainCurrent  |= skeletonize3D.computeSkeletonIteration();
			if(!blockInformation.isIndependent) {
				blockInformation.isIndependent = skeletonize3D.isSkeletonBlockIndependent();
			}
		}
		
		//update output
		currentCursor.reset();
		thinningResultCursor.reset();//update in case need to rethin
		while(currentCursor.hasNext()) {
			currentCursor.next();
			thinningResultCursor.next();
			if(currentCursor.get().get() == 0) {
				if(thinningResultCursor.get().getIntegerLong()>0) {
					blockInformation = updateBlockInformationThinningLocations(thinningResultCursor, padding, paddedDimension, blockInformation);
				}
				thinningResultCursor.get().setInteger(0);//Then this voxel was thinned out
			}
		}
	
		//previous thinning result should now equal the current thinning result
		return blockInformation;
	}
	
	public static <T extends IntegerType<T>> BlockInformation updateBlockInformationThinningLocations(	Cursor<T> thinningResultCursor, int[][] padding, long [] paddedDimension, BlockInformation blockInformation) {		
		//in the case that this block is independent, then it doesnt matter because it will not have any effect since its edge doesnt change.
		int xIndex = getThinningLocationsIndex(0, thinningResultCursor, padding, paddedDimension);
		int yIndex = getThinningLocationsIndex(1, thinningResultCursor, padding, paddedDimension);
		int zIndex = getThinningLocationsIndex(2, thinningResultCursor, padding, paddedDimension);
		
		blockInformation.thinningLocations[xIndex][yIndex][zIndex] = true;
		return blockInformation;
	}

	public static <T extends IntegerType<T>> int getThinningLocationsIndex(int d, Cursor<T> thinningResultCursor, int[][] padding, long[] paddedDimension){
		int [] paddingNeg = padding[0];
		int [] paddingPos = padding[1];
		
		int pos = thinningResultCursor.getIntPosition(d);
		int idx;
		if(pos>=paddingNeg[d] && pos<(pad+paddingNeg[d])) idx=0;
		else if(pos>paddedDimension[d]-(pad+paddingPos[d]) && pos<=paddedDimension[d]-paddingPos[d]) idx = 2;
		else idx =1;
		
		return idx;
	}
	public static List<BlockInformation> buildBlockInformationList(final String inputN5Path,
			final String inputN5DatasetName) throws Exception {
		// Get block attributes
		N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();

		// Build list
		List<long[][]> gridBlockList = Grid.create(outputDimensions, blockSize);
		List<BlockInformation> blockInformationList = new LinkedList<BlockInformation>();
		for (int i = 0; i < gridBlockList.size(); i++) {
			//int pad = 50;//I think for doing 6 borders (N,S,E,W,U,B) where we do the 8 indpendent iterations, the furthest a voxel in a block can be affected is from something 48 away, so add 2 more just as extra border
			if(pad>=blockSize[0] || pad>=blockSize[1] || pad>=blockSize[2]) {
				throw new Exception("Padding is bigger than block size...");
			}
			long[][] currentGridBlock = gridBlockList.get(i);
			long[][] paddedGridBlock = { {currentGridBlock[0][0]-pad, currentGridBlock[0][1]-pad, currentGridBlock[0][2]-pad}, //initialize padding
										{currentGridBlock[1][0]+2*pad, currentGridBlock[1][1]+2*pad, currentGridBlock[1][2]+2*pad}};
			int [][] padding = {{pad,pad, pad},
					{pad,pad, pad}};
			blockInformationList.add(new BlockInformation(currentGridBlock, paddedGridBlock, padding, null, null));
		}
		return blockInformationList;
	}
	
	public static void setupSparkAndDoTopologicalThinning(String inputN5Path, String outputN5Path, String inputN5DatasetName, String outputN5DatasetSuffix, boolean doMedialSurface) throws Exception {
	    final SparkConf conf = new SparkConf().setAppName("SparkTopologicalThinning");

		// Get all organelles
		String[] organelles = { "" };
		if (inputN5DatasetName != null) {
			organelles = inputN5DatasetName.split(",");
		} else {
			File file = new File(inputN5Path);
			organelles = file.list(new FilenameFilter() {
				@Override
				public boolean accept(File current, String name) {
					return new File(current, name).isDirectory();
				}
			});
		}

		System.out.println(Arrays.toString(organelles));
		String tempOutputN5DatasetName = null;
		String finalOutputN5DatasetName = null;
		for (String currentOrganelle : organelles) {
			finalOutputN5DatasetName = currentOrganelle + outputN5DatasetSuffix;

			// Create block information list
			List<BlockInformation> blockInformationList = buildBlockInformationList(inputN5Path, currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			int fullIterations = 0;
						
			while(blockInformationList.size()>0){ //Thin until block information list is empty
				blockInformationList = performTopologicalThinningIteration(sc, inputN5Path, currentOrganelle, outputN5Path,
							finalOutputN5DatasetName, doMedialSurface, blockInformationList, fullIterations);
				fullIterations++;
			}
			
			String finalFileName = finalOutputN5DatasetName + '_'+ ((fullIterations-1)%2==0 ? "even" : "odd");
			FileUtils.deleteDirectory(new File(outputN5Path + "/" + finalOutputN5DatasetName));
			FileUtils.moveDirectory(new File(outputN5Path + "/" + finalFileName), new File(outputN5Path + "/" + finalOutputN5DatasetName));
			sc.close();
		}

		// Remove temporary files
		for (String currentOrganelle : organelles) {
			tempOutputN5DatasetName = currentOrganelle + outputN5DatasetSuffix+ "_even";
			FileUtils.deleteDirectory(new File(outputN5Path + "/" + tempOutputN5DatasetName));
			tempOutputN5DatasetName = currentOrganelle + outputN5DatasetSuffix + "_odd";
			FileUtils.deleteDirectory(new File(outputN5Path + "/" + tempOutputN5DatasetName));
		}

	}


	public static final void main(final String... args) throws Exception {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		String inputN5Path = options.getInputN5Path();
		String outputN5Path = options.getOutputN5Path();
		String inputN5DatasetName = options.getInputN5DatasetName();
		String outputN5DatasetSuffix = options.getOutputN5DatasetSuffix();
		boolean doMedialSurface = options.getDoMedialSurface();
		
		setupSparkAndDoTopologicalThinning(inputN5Path, outputN5Path, inputN5DatasetName, outputN5DatasetSuffix, doMedialSurface);
		
	}
}
