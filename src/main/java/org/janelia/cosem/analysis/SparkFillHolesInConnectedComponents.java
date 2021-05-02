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
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.cosem.util.SparkDirectoryDelete;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
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
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.GenericIntType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkFillHolesInConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;
		
		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be /mito_cc")
		private String outputN5DatasetSuffix = "";

		@Option(name = "--skipVolumeFilter", required = false, usage = "Skip volume filtering")
		private boolean skipVolumeFilter = false;
		
		@Option(name = "--skipCreatingHoleDataset", required = false, usage = "Skip creating hole dataset (if it exists already)")
		private boolean skipCreatingHoleDataset = false;
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Volume above which objects will be kept (nm^3)")
		private float minimumVolumeCutoff = 20000000;

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
		
		public String getOutputN5Path() {
			return outputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}

		public String getOutputN5DatasetSuffix() {
			return outputN5DatasetSuffix;
		}
		
		public boolean getSkipVolumeFilter() {
			return skipVolumeFilter;
		}
		
		public boolean getSkipCreatingHoleDataset() {
			return skipCreatingHoleDataset;
		}
		
		public float getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}
	

	}
	
	/**
	 * Class containing maps used for the hole-filling process
	 */
	@SuppressWarnings("serial")
	public static class MapsForFillingHoles implements Serializable{
		public Map<Long,Long> objectIDtoVolumeMap;
		public Map<Long,Long> holeIDtoVolumeMap;
		public Map<Long,Long> holeIDtoObjectIDMap;
		public Set<Long> objectIDsBelowVolumeFilter;

		/**
		 * Constructor for maps class
		 * 
		 * @param objectIDtoVolumeMap	Map of object ID to its volume
		 * @param holeIDtoVolumeMap		Map of hole ID to its volume 
		 * @param holeIDtoObjectIDMap	Map of hole ID to its surrounding object ID
		 */
		public MapsForFillingHoles(Map<Long,Long> objectIDtoVolumeMap, Map<Long,Long> holeIDtoVolumeMap, Map<Long,Long> holeIDtoObjectIDMap){
			this.objectIDtoVolumeMap = objectIDtoVolumeMap;
			this.holeIDtoVolumeMap = holeIDtoVolumeMap;
			this.holeIDtoObjectIDMap = holeIDtoObjectIDMap;
			this.objectIDsBelowVolumeFilter = new HashSet<Long>();
		}
		
		/**
		 * Method for merging two instances of this class
		 * 
		 * @param newMapsForFillingHoles	The new instance to be merged into the current instance
		 */
		public void merge(MapsForFillingHoles newMapsForFillingHoles) {
			//merge holeIDtoObjectIDMap
			for(Entry<Long,Long> entry : newMapsForFillingHoles.holeIDtoObjectIDMap.entrySet()) {
				long holeID = entry.getKey();
				long objectID = entry.getValue();
				//Then is not a hole because it is surrounded by multiple objects
				if(	holeIDtoObjectIDMap.containsKey(holeID) && holeIDtoObjectIDMap.get(holeID)!=objectID) 
					holeIDtoObjectIDMap.put(holeID, 0L);
				else 
					holeIDtoObjectIDMap.put(holeID, objectID);
			}
			
			//merge holeIDtoVolumeMap
			for(Entry<Long,Long> entry : newMapsForFillingHoles.holeIDtoVolumeMap.entrySet())
				holeIDtoVolumeMap.put(entry.getKey(), holeIDtoVolumeMap.getOrDefault(entry.getKey(), 0L) + entry.getValue() );
			
			//merge objectIDtoVolumeMap
			for(Entry<Long,Long> entry : newMapsForFillingHoles.objectIDtoVolumeMap.entrySet())
				objectIDtoVolumeMap.put(entry.getKey(), objectIDtoVolumeMap.getOrDefault(entry.getKey(), 0L) + entry.getValue() );
		
		}
	}
	
	private static final<T extends IntegerType<T> & NativeType<T>> T getMaxValue(DataType dataType){
	    if(dataType == DataType.UINT8) {
		return (T) new UnsignedByteType((int) (Math.pow(2,8)-1));
	    }
	    if(dataType == DataType.UINT16) {
		return (T) new UnsignedShortType((int) (Math.pow(2,16)-1));
	    }
	    else if(dataType == DataType.UINT32) {
		return (T) new UnsignedIntType((int) (Math.pow(2,32)-1));
	    }
	    else if(dataType == DataType.UINT64) {
		return (T) new UnsignedLongType((int) (Math.pow(2,64)-1));
	    }
	    return null;
	}
	/**
	 * Find connected components on a block-by-block basis and write out to
	 * temporary n5.
	 *
	 * Takes as input a threshold intensity, above which voxels are used for
	 * calculating connected components. Parallelization is done using a
	 * blockInformationList.
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param inputN5DatasetName
	 * @param outputN5Path
	 * @param outputN5DatasetName
	 * @param maskN5PathName
	 * @param thresholdIntensity
	 * @param blockInformationList
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends IntegerType<T> & NativeType<T>> MapsForFillingHoles getMapsForFillingHoles(
			final JavaSparkContext sc, final String inputN5Path, final String outputN5Path, final String inputN5DatasetName,
			List<BlockInformation> blockInformationList) throws IOException {
	   	final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DataType dataType = n5Reader.getDatasetAttributes(inputN5DatasetName).getDataType();
	
		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<MapsForFillingHoles> javaRDDFillingHoleResults = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			long[] paddedOffset = new long [] {offset[0]-1, offset[1]-1, offset[2]-1};//add one extra for distance transform
			long[] paddedDimension = new long[] {dimension[0]+2, dimension[1]+2, dimension[2]+2};
			
			// Read in source block
			final N5Reader n5InputReaderLocal = new N5FSReader(inputN5Path);
			final N5Reader n5OutputReaderLocal = new N5FSReader(outputN5Path);

			long[] fullDimensions = new long [] {0,0,0};
			long maxValue = fullDimensions[0]*fullDimensions[1]*fullDimensions[2]*10;//no object should have a value larger than this
			//TODO: fix edge case here?
			new UnsignedByteType();
			final RandomAccessibleInterval<T> objects = Views.offsetInterval(Views.extendValue((RandomAccessibleInterval<T>) N5Utils.open(n5InputReaderLocal, inputN5DatasetName), getMaxValue(dataType)),paddedOffset, paddedDimension); 
			final RandomAccessibleInterval<T> holes = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<T>) N5Utils.open(n5OutputReaderLocal, inputN5DatasetName+"_holes")),paddedOffset, paddedDimension);
			
			final RandomAccessibleInterval<BoolType> objectsBinarized = Converters.convert(objects,
					(a, b) -> b.set(a.getRealDouble() > 0), new BoolType());

			//Get distance transform
			ArrayImg<FloatType, FloatArray> distanceFromObjects = ArrayImgs.floats(paddedDimension);	
			DistanceTransform.binaryTransform(objectsBinarized, distanceFromObjects, DISTANCE_TYPE.EUCLIDIAN);
	
			//Reassign black values
			ArrayCursor<FloatType> distanceFromObjectCursor = distanceFromObjects.cursor();
			RandomAccess<T> holeComponentsRandomAccess = holes.randomAccess();
			RandomAccess<T> objectsRandomAccess = objects.randomAccess();
			
			
			Map<Long,Long> holeIDtoObjectIDMap = new HashMap<Long,Long>();
			Map<Long,Long> objectIDtoVolumeMap = new HashMap<Long,Long>();
			Map<Long,Long> holeIDtoVolumeMap = new HashMap<Long,Long>();
			
			while(distanceFromObjectCursor.hasNext()) {
				float distanceFromObjectSquared = distanceFromObjectCursor.next().get();
				int pos [] = new int[] {distanceFromObjectCursor.getIntPosition(0), distanceFromObjectCursor.getIntPosition(1), distanceFromObjectCursor.getIntPosition(2)};
				
				if (distanceFromObjectSquared>0 && distanceFromObjectSquared <= 3) { //3 for corners. If true, then is on edge of hole
					if(pos[0]>0 && pos[0]<=dimension[0] && pos[1]>0 && pos[1]<=dimension[1] && pos[2]>0 && pos[2]<=dimension[2]) {//Then in original block
						holeComponentsRandomAccess.setPosition(pos);
						long holeID = holeComponentsRandomAccess.get().getIntegerLong();

						for(int dx=-1; dx<=1; dx++) {
							for(int dy=-1; dy<=1; dy++) {
								for(int dz=-1; dz<=1; dz++) {
									if((dx==0 && dy==0 && dz!=0) || (dx==0 && dz==0 && dy!=0) || (dy==0 && dz==0 && dx!=0)) {//diamond checking
										int newPos [] = new int[] {pos[0]+dx, pos[1]+dy, pos[2]+dz};
										objectsRandomAccess.setPosition(newPos);
										long objectID = objectsRandomAccess.get().getIntegerLong();
										if(objectID>0) {//can still be outside
											if ( objectID == maxValue || (holeIDtoObjectIDMap.containsKey(holeID) && objectID != holeIDtoObjectIDMap.get(holeID)) ) //is touching outside or then has already been assigned to an object and is not really a hole since it is touching multiple objects
												holeIDtoObjectIDMap.put(holeID,0L);
											else 
												holeIDtoObjectIDMap.put(holeID,objectID);										
										}
									}
								}
							}
						}
						
					}
				}
			}

			return new MapsForFillingHoles(objectIDtoVolumeMap, holeIDtoVolumeMap, holeIDtoObjectIDMap);
		});

		MapsForFillingHoles mapsForFillingHoles = javaRDDFillingHoleResults.reduce((a,b) -> {
			a.merge(b);
			return a;
		});
		return mapsForFillingHoles;
	}
	
	public static final <T extends IntegerType<T> & NativeType<T>> void fillHoles(
			final JavaSparkContext sc, final String inputN5Path, final String outputN5Path,final String inputN5DatasetName, final String outputN5DatasetName, MapsForFillingHoles mapsForFillingHoles,
			List<BlockInformation> blockInformationList) throws IOException {
				// Get attributes of input data set
				final N5Reader n5Reader = new N5FSReader(inputN5Path);
				final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
				final int[] blockSize = attributes.getBlockSize();

				// Create output dataset
				final N5Writer n5Writer = new N5FSWriter(outputN5Path);
				n5Writer.createGroup(outputN5DatasetName);
				n5Writer.createDataset(outputN5DatasetName, attributes.getDimensions(), blockSize,
						attributes.getDataType(), attributes.getCompression());
				n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputN5DatasetName)));

				
				// Set up rdd to parallelize over blockInformation list and run RDD, which will
				// return updated block information containing list of components on the edge of
				// the corresponding block
				final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
				rdd.foreach(currentBlockInformation -> {
					// Get information for reading in/writing current block
					long[][] gridBlock = currentBlockInformation.gridBlock;
					long[] offset = gridBlock[0];
					long[] dimension = gridBlock[1];
			
					// Read in source block
					final N5Reader n5ReaderInputLocal = new N5FSReader(inputN5Path);
					final N5Reader n5ReaderOutputLocal = new N5FSReader(outputN5Path);

					final RandomAccessibleInterval<T> objects = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<T>) N5Utils.open(n5ReaderInputLocal, inputN5DatasetName)),offset, dimension); 
					final RandomAccessibleInterval<T> holes = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<T>) N5Utils.open(n5ReaderOutputLocal, inputN5DatasetName+"_holes")),offset, dimension);
					final IntervalView<T> output = ProcessingHelper.getZerosIntegerImageRAI(dimension, attributes.getDataType()) ;

					Cursor<T> outputCursor = output.cursor();
					RandomAccess<T> objectsRandomAccess = objects.randomAccess();
					RandomAccess<T> holesRandomAccess = holes.randomAccess();
					while(outputCursor.hasNext()) {
						T voxel = outputCursor.next();
						long pos [] = new long[] {outputCursor.getLongPosition(0), outputCursor.getLongPosition(1), outputCursor.getLongPosition(2)};
						holesRandomAccess.setPosition(pos);
						objectsRandomAccess.setPosition(pos);
						
						long holeID = holesRandomAccess.get().getIntegerLong();
						long objectID = objectsRandomAccess.get().getIntegerLong();
						
						
						long setValue = objectID;	
						
						if( holeID > 0) {
							setValue = mapsForFillingHoles.holeIDtoObjectIDMap.get(holeID);
						}
						
						voxel.setInteger(setValue);
					}


					final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
					N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);
					
					//Get distance transform
				});
			
		
	}

	public static void setupSparkAndFillHolesInConnectedComponents(String inputN5Path, String outputN5Path, String inputN5DatasetName, float minimumVolumeCutoff, String outputN5DatasetSuffix, boolean skipCreatingHoleDataset, boolean skipVolumeFilter) throws IOException {
		final SparkConf conf = new SparkConf().setAppName("SparkFillHolesInConnectedComponents");
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

		String datasetToHoleFill = null;
		String tempOutputN5DatasetName = null;
		String finalOutputN5DatasetName = null;
		List<String> directoriesToDelete = new ArrayList<String>();
		for (String currentOrganelle : organelles) {
			ProcessingHelper.logMemory(currentOrganelle);
			
			// Create block information list
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path, currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			datasetToHoleFill = currentOrganelle;
			if(!skipVolumeFilter) {
				String tempVolumeFilteredDatasetName = currentOrganelle + "_volumeFilteredTemp"+outputN5DatasetSuffix;
				SparkVolumeFilterConnectedComponents.volumeFilterConnectedComponents(sc, inputN5Path, outputN5Path, currentOrganelle, tempVolumeFilteredDatasetName, minimumVolumeCutoff, blockInformationList);
				directoriesToDelete.add(outputN5Path + "/" + tempVolumeFilteredDatasetName);
				datasetToHoleFill = tempVolumeFilteredDatasetName;
				ProcessingHelper.logMemory("Volume filter complete");
			}
			tempOutputN5DatasetName = datasetToHoleFill + "_holes" + "_blockwise_temp_to_delete";
			finalOutputN5DatasetName = datasetToHoleFill + "_holes";
			directoriesToDelete.add(outputN5Path + "/" + tempOutputN5DatasetName);
			directoriesToDelete.add(outputN5Path + "/" + finalOutputN5DatasetName);
			
			if(!skipCreatingHoleDataset) {
				// Get connected components of holes in *_holes
				int minimumVolumeCutoffZero = 0;
				blockInformationList = SparkConnectedComponents.blockwiseConnectedComponents(sc, inputN5Path,
						datasetToHoleFill, outputN5Path, tempOutputN5DatasetName, null, 1, minimumVolumeCutoffZero,
						blockInformationList, true, false);
				ProcessingHelper.logMemory("Stage 1 complete");
	
				blockInformationList = SparkConnectedComponents.unionFindConnectedComponents(sc, outputN5Path,
						tempOutputN5DatasetName, minimumVolumeCutoffZero, blockInformationList);
				ProcessingHelper.logMemory("Stage 2 complete");
	
				SparkConnectedComponents.mergeConnectedComponents(sc, outputN5Path, tempOutputN5DatasetName,
						finalOutputN5DatasetName, blockInformationList);
				ProcessingHelper.logMemory("Stage 3 complete");
			}
			

			MapsForFillingHoles mapsForFillingHoles = getMapsForFillingHoles(sc,  inputN5Path, outputN5Path, datasetToHoleFill, blockInformationList);
			fillHoles(sc, inputN5Path, outputN5Path, datasetToHoleFill, currentOrganelle+outputN5DatasetSuffix, mapsForFillingHoles, blockInformationList);
			
			sc.close();
		}

		// Remove temporary files
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
		ProcessingHelper.logMemory("Stage 4 complete");
	}

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		String inputN5DatasetName = options.getInputN5DatasetName();
		String inputN5Path = options.getInputN5Path();
		String outputN5Path = options.getOutputN5Path();
		Float minimumVolumeCutoff = options.getMinimumVolumeCutoff();
		String outputN5DatasetSuffix = options.getOutputN5DatasetSuffix();
		boolean skipCreatingHoleDataset = options.getSkipCreatingHoleDataset();
		boolean skipVolumeFilter = options.getSkipVolumeFilter();

		setupSparkAndFillHolesInConnectedComponents(inputN5Path, outputN5Path, inputN5DatasetName, minimumVolumeCutoff, outputN5DatasetSuffix, skipCreatingHoleDataset, skipVolumeFilter);

	}
}
