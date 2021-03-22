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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.saalfeldlab.n5.DataType;
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

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;


/**
 * Expand microtbule center axis predictions to create tubes
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkExpandMicrotubules {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path")
		private String inputN5Path = null;
		
		@Option(name = "--inputN5DatasetName", required = true, usage = "input N5 datasetname")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--innerRadiusInNm", required = false, usage = "inner radius (nm)")
		private Double innerRadiusInNm = 6.0;
		
		@Option(name = "--outerRadiusInNm", required = false, usage = "outer radius (nm)")
		private Double outerRadiusInNm = 12.5;

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
		
		public String getOutputN5Path() {
			if(outputN5Path == null) {
				return inputN5Path;
			}
			else {
				return outputN5Path;
			}
		}
		
		public Double getInnerRadiusInNm() {
			return innerRadiusInNm;
		}
		
		public Double getOuterRadiusInNm() {
			return outerRadiusInNm;
		}

	}

	/**
	  * Expand microtubules to create a tube with a set inner radius and outer radius
	  *
	  *
	  * @param sc					Spark context
	  * @param n5Path				Path to n5
	  * @param inputDatasetName		Dataset name
	  * @param n5OutputPath			Output n5 path
	  * @param outputDatasetName	Output datset name
	  * @param innerRadiusInNm		Inner tube radius
	  * @param outerRadiusInNm		Outer tube radius
	  * @param blockInformationList	Block information list
	  * @throws IOException
	  */
	public static final <T extends IntegerType<T>>  void expandMicrotubules(final JavaSparkContext sc, final String n5Path,
			final String inputDatasetName, final String n5OutputPath, final String outputDatasetName, final double innerRadiusInNm, final double outerRadiusInNm,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		
		double [] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
		n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT64, new GzipCompression());
		n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputDatasetName)));
		n5Writer.setAttribute(outputDatasetName, "offset", IOHelper.getOffset(n5Reader, inputDatasetName));

		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		
		long padding = (long) (Math.ceil(outerRadiusInNm/pixelResolution[0])+2);
		double outerRadiusInVoxels = outerRadiusInNm/pixelResolution[0];
		double innerRadiusInVoxels = innerRadiusInNm/pixelResolution[0];
		
		rdd.foreach(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];//new long[] {64,64,64};//gridBlock[0];////
			long[] dimension = gridBlock[1];
			long[] paddedOffset = new long[]{offset[0]-padding, offset[1]-padding, offset[2]-padding};
			long[] paddedDimension = new long []{dimension[0]+2*padding, dimension[1]+2*padding, dimension[2]+2*padding};
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			

			RandomAccessibleInterval<T> microtubuleCenterline = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<T>)N5Utils.open(n5BlockReader, inputDatasetName)),paddedOffset, paddedDimension);	
			RandomAccess<T> microtubuleCenterlineRA = microtubuleCenterline.randomAccess();
			IntervalView<UnsignedLongType> expandedMicrotubule = Views.offsetInterval(ArrayImgs.unsignedLongs(paddedDimension),new long[]{0,0,0}, paddedDimension);
			RandomAccess<UnsignedLongType> expandedMicrotubuleRA = expandedMicrotubule.randomAccess();
			
			
			//Create maps of object ID to centerline voxels as well as map of object id to extended endpoint voxels.
			//Extended endpoint voxels are the hypothetical extension of the microtubule
			Map<Long, List<List<Integer>>> idToCenterlineVoxels = new HashMap<Long,List<List<Integer>>>();
			Map<Long, List<int[]>> idToExtendedEndpointVoxels = new HashMap<Long,List<int[]>>();
			
			for(int x=1; x<paddedDimension[0]-1; x++) {
				for(int y=1; y<paddedDimension[1]-1; y++) {
					for(int z=1; z<paddedDimension[2]-1; z++) {
						int pos[] = new int[] {x,y,z};
						microtubuleCenterlineRA.setPosition(pos);
						long objectID = microtubuleCenterlineRA.get().getIntegerLong();
						if(objectID>0) {//then it is on microtubule center axis
							List<List<Integer>> currentCenterlineVoxels = idToCenterlineVoxels.getOrDefault(objectID, new ArrayList<List<Integer>>());
							currentCenterlineVoxels.add(Arrays.asList(pos[0],pos[1],pos[2]));
							idToCenterlineVoxels.put(objectID, currentCenterlineVoxels);
							
							int [] extendedEndpointVoxel = getExtendedEndpointVoxel(microtubuleCenterlineRA, objectID, pos);
							if(extendedEndpointVoxel!=null) {
								List<int[]> currentExtendedEndpointVoxels = idToExtendedEndpointVoxels.getOrDefault(objectID, new ArrayList<int[]>());
								currentExtendedEndpointVoxels.add(extendedEndpointVoxel);
								idToExtendedEndpointVoxels.put(objectID, currentExtendedEndpointVoxels);
							}
							
						}
					}
				}
			}
			
			//Do the expansion to create the tube
			expandMicrotubules(expandedMicrotubuleRA, idToCenterlineVoxels,idToExtendedEndpointVoxels,innerRadiusInVoxels,outerRadiusInVoxels, padding, paddedDimension);
			
			//Write it out
			IntervalView<UnsignedLongType> output = Views.offsetInterval(expandedMicrotubule,new long[]{padding,padding,padding}, dimension);
			final N5Writer n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
						
		});

	}
	
	/**
	 * Expand the microtubule enter axis.
	 * 
	 * @param expandedMicrotubulesRA		The output image
	 * @param idToCenterlineVoxels			Map of object id to centerline voxel
	 * @param idToExtendedEndpointVoxels	Map of object id to extended endpoint voxel
	 * @param innerRadiusInVoxels			Inner radius of tube in voxels
	 * @param outerRadiusInVoxels			Outer radius of tube in voxels
	 * @param padding
	 * @param dimension
	 */
	private static void expandMicrotubules(RandomAccess<UnsignedLongType> expandedMicrotubulesRA, Map<Long, List<List<Integer>>> idToCenterlineVoxels,
			Map<Long, List<int[]>> idToExtendedEndpointVoxels, double innerRadiusInVoxels, double outerRadiusInVoxels, long padding, long [] dimension) {
			int outerRadiusInVoxelsCeiling = (int) Math.ceil(outerRadiusInVoxels);

			double outerRadiusInVoxelsSquared = outerRadiusInVoxels*outerRadiusInVoxels;
			double innerRadiusInVoxelsSquared = innerRadiusInVoxels*innerRadiusInVoxels;
			
			//Get the set of deltas of voxels within inner and outer radius.
			Set<List<Integer>> setOfDeltasWithinOuterRadius = new HashSet<List<Integer>>();
			Set<List<Integer>> setOfDeltasWithinInnerRadius = new HashSet<List<Integer>>();;
			for(int dx = -outerRadiusInVoxelsCeiling; dx<=outerRadiusInVoxelsCeiling;  dx++) {
				for(int dy = -outerRadiusInVoxelsCeiling; dy<=outerRadiusInVoxelsCeiling;  dy++) {
					for(int dz = -outerRadiusInVoxelsCeiling; dz<=outerRadiusInVoxelsCeiling;  dz++) {
						int radiusSquared = dx*dx+dy*dy+dz*dz;
						if(radiusSquared<=outerRadiusInVoxelsSquared) {
							setOfDeltasWithinOuterRadius.add(Arrays.asList(dx,dy,dz));
						}
						if(radiusSquared<innerRadiusInVoxelsSquared) {
							setOfDeltasWithinInnerRadius.add(Arrays.asList(dx,dy,dz));
						}
					}	
				}
			}
			
			//Loop over each object
			for( Entry<Long, List<List<Integer>>> entry : idToCenterlineVoxels.entrySet()) {
				Long objectID = entry.getKey();
				List<List<Integer>> centerlineVoxels = entry.getValue();
				
				//create set of voxels that for the tube
				Set<List<Integer>> currentTube = new HashSet<List<Integer>>();
				
				//fill in all voxels within outer radius of center axis; this will still have caps on the end and be full
				for(List<Integer> currentCenterlineVoxel : centerlineVoxels) {	
					for(List<Integer> delta : setOfDeltasWithinOuterRadius) {
						currentTube.add(Arrays.asList(currentCenterlineVoxel.get(0)+delta.get(0),currentCenterlineVoxel.get(1)+delta.get(1),currentCenterlineVoxel.get(2)+delta.get(2)));
					}	
				}
				
				//hollow out the tube by removing all voxels within inner radius of center axis; this will still have caps
				for(List<Integer> currentCenterlineVoxel : centerlineVoxels) {	
					for(List<Integer> delta : setOfDeltasWithinInnerRadius) {
						currentTube.remove(Arrays.asList(currentCenterlineVoxel.get(0)+delta.get(0),currentCenterlineVoxel.get(1)+delta.get(1),currentCenterlineVoxel.get(2)+delta.get(2)));
					}	
				}
				
				//remove cap by removing any voxel that is closer to the expanded endpoint than to the endpoint itself;
				List<int[]> extendedEndpointVoxels = idToExtendedEndpointVoxels.getOrDefault(objectID,null);
				if(extendedEndpointVoxels!=null) {//then an endpoint is present
					for(int[] currentExtendedEndpoint : extendedEndpointVoxels) {
						int endpoint[] = new int[] {currentExtendedEndpoint[0],currentExtendedEndpoint[1],currentExtendedEndpoint[2]};
						int extendedEndpoint[] = new int[] {currentExtendedEndpoint[3],currentExtendedEndpoint[4],currentExtendedEndpoint[5]};
						for(List<Integer> delta : setOfDeltasWithinOuterRadius) {
							int dx = delta.get(0);
							int dy = delta.get(1);
							int dz = delta.get(2);
							int distanceToEndpointSquared = dx*dx+dy*dy+dz*dz;
							
							dx-=extendedEndpoint[0];
							dy-=extendedEndpoint[1];
							dz-=extendedEndpoint[2];
							int distanceToExtendedEndpointSquared = dx*dx+dy*dy+dz*dz;
							
							if(distanceToExtendedEndpointSquared<=distanceToEndpointSquared) {
								List<Integer> possiblePointThatShouldBeDeleted = Arrays.asList(endpoint[0]+delta.get(0), endpoint[1]+delta.get(1), endpoint[2]+delta.get(2));
								currentTube.remove(possiblePointThatShouldBeDeleted);
							}
						}	
						
					}
				}
				
				//update image
				for(List<Integer> currentPos : currentTube) {
					int x=(int) currentPos.get(0);
					int y=(int) currentPos.get(1);
					int z=(int) currentPos.get(2);
					if(x>=0 && x<dimension[0] && 
							y>=0 && y<dimension[1] &&
							z>=0 && z<dimension[2]) {
						expandedMicrotubulesRA.setPosition(new int[] {x, y,z});
						expandedMicrotubulesRA.get().set(objectID);
					}
				}		
			}
	}

	/**
	 * Get the extendended endpoint voxel if it exists
	 * @param ra			Image random access
	 * @param objectID		Object ID	
	 * @param pos			Current position
	 * @return				ExtendedEndpointVoxelPos which is an array containing the ednpoint pos and the delta to reach the extended endpoint
	 */
	public static <T extends IntegerType<T>> int[] getExtendedEndpointVoxel(RandomAccess<T> ra, long objectID, int [] pos) {
		int [] extendedEndpointVoxelPos = null;
		
		//get endpoint and extended endpoint for simple case where there is just 1 neighbor of endpoint
		int numNeighbors = 0;
		List<int[]> neighboringVoxels = new ArrayList<int[]>();
		for(int dx = -1; dx<=1; dx++) {
			for(int dy=-1; dy<=1; dy++) {
				for(int dz=-1; dz<=1; dz++) {
					if(!(dx==0 && dy==0 && dz==-0)) {
						ra.setPosition(new int[] {pos[0]+dx,pos[1]+dy,pos[2]+dz});
						long currentID = ra.get().getIntegerLong();
						if(currentID == objectID) {
							numNeighbors ++;
							if(numNeighbors>2) {//Then it isn't an endpoint
								return null;
							}
							extendedEndpointVoxelPos = new int [] {pos[0], pos[1], pos[2], -dx, -dy, -dz}; //continue but beyond it
							neighboringVoxels.add(new int[] {dx,dy,dz});
						}
					}
				}
			}
		}
		
		//May be an endpoint but have multiple neighbors
		if(numNeighbors==2) {//could potentially still be endpoint since could be like stair [][]
							//																   [][]
			int[] neighborZero = neighboringVoxels.get(0);
			int[] neighborOne = neighboringVoxels.get(1);
			
			int dx = neighborZero[0]-neighborOne[0];
			int dy = neighborZero[1]-neighborOne[1];
			int dz = neighborZero[2]-neighborOne[2];
			
			if(dx*dx+dy*dy+dz*dz>2) {//then the neighbors are far enough apart that the center point is not an endpoint
				return null;
			}
			else {
				int n0mag = neighborZero[0]*neighborZero[0]+neighborZero[1]*neighborZero[1]+neighborZero[2]*neighborZero[2];
				int n1mag = neighborOne[0]*neighborOne[0]+neighborOne[1]*neighborOne[1]+neighborOne[2]*neighborOne[2];
				if(n0mag<n1mag) {//take nearest voxel
					extendedEndpointVoxelPos = new int [] {pos[0], pos[1], pos[2], -neighborZero[0], -neighborZero[1], -neighborZero[2]}; //continue but beyond it
				}
				else {
					extendedEndpointVoxelPos = new int [] {pos[0], pos[1], pos[2], -neighborOne[0], -neighborOne[1], -neighborOne[2]}; //continue but beyond it
				}
			}
		}
		
		return extendedEndpointVoxelPos;
	}
	
	/**
	 * Fill in all voxels within expanded region
	 * 
	 * @param expandedSkeletonsRA 	Output expanded skeletons random access
	 * @param objectID				Object ID of skeleton
	 * @param pos					Position of skeleton voxel
	 * @param paddedDimension		Padded dimensions
	 * @param expansionInVoxels		Expansion radius in voxels
	 */
	public static void fillInExpandedRegion(RandomAccess<UnsignedLongType> expandedSkeletonsRA, long objectID, int [] pos, long [] paddedDimension, int expansionInVoxels) {
		int expansionInVoxelsSquared = expansionInVoxels*expansionInVoxels;
		for(int x=pos[0]-expansionInVoxels; x<=pos[0]+expansionInVoxels; x++) {
			for(int y=pos[1]-expansionInVoxels; y<=pos[1]+expansionInVoxels; y++) {
				for(int z=pos[2]-expansionInVoxels; z<=pos[2]+expansionInVoxels; z++) {
					int dx = x-pos[0];
					int dy = y-pos[1];
					int dz = z-pos[2];
						if((dx*dx+dy*dy+dz*dz)<=expansionInVoxelsSquared) {
						if((x>=0 && y>=0 && z>=0) && (x<paddedDimension[0] && y<paddedDimension[1] && z<paddedDimension[2])) {
							expandedSkeletonsRA.setPosition(new int[] {x,y,z});
							expandedSkeletonsRA.get().set(objectID);
						}
					}
				}

			}
		}
	}

	/**
	 * Expand microtubules to create tubes from center axis
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkEpandMicrotubules");

		// Get all organelles
		String[] organelles = { "" };
		if (options.getInputN5DatasetName() != null) {
			organelles = options.getInputN5DatasetName().split(",");
		} else {
			File file = new File(options.getInputN5Path());
			organelles = file.list(new FilenameFilter() {
				@Override
				public boolean accept(File current, String name) {
					return new File(current, name).isDirectory();
				}
			});
		}

		System.out.println(Arrays.toString(organelles));

		for (String currentOrganelle : organelles) {
			// Create block information list
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			expandMicrotubules(sc, options.getInputN5Path(),
					currentOrganelle, options.getOutputN5Path(), currentOrganelle+"_expanded",
					options.getInnerRadiusInNm(), options.getOuterRadiusInNm(), blockInformationList);
			sc.close();
		}

	}
}
