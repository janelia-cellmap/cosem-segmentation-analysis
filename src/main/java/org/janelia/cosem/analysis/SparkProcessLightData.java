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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.SparkDirectoryDelete;
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

import net.imagej.ImageJ;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkProcessLightData {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /path/to/input/predictions.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /path/to/output/connected_components.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. organelle")
		private String inputN5DatasetName = null;

		@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _cc so output would be organelle_cc")
		private String outputN5DatasetSuffix = "_cc";

		@Option(name = "--thresholdIntensity", required = true, usage = "Intesnity for thresholding (positive inside, negative outside) (nm)")
		private String thresholdIntensity = "0,0";
		
		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Volume above which objects will be kept (nm^3)")
		private double minimumVolumeCutoff = 20E6;
		
		@Option(name = "--minimumContactSiteVolumeCutoff", required = false, usage = "Volume above which contact site objects will be kept (nm^3)")
		private double minimumContactSiteVolumeCutoff = 35E3;

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
			if(outputN5Path==null) {
				return inputN5Path;
			}
			else {
				return outputN5Path;
			}
		}

		public double[] getThresholdIntensityCutoff() {
			String[] thresholdIntensitiesAsString = thresholdIntensity.split(",");
			return new double[] {Double.parseDouble(thresholdIntensitiesAsString[0]), Double.parseDouble(thresholdIntensitiesAsString[1])};
		}
		
		public double getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}
		
		public double getMinimumContactSiteVolumeCutoff() {
			return minimumContactSiteVolumeCutoff;
		}

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
	 * @param thresholdIntensity
	 * @param blockInformationList
	 * @throws IOException
	 */
	
	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> List<BlockInformation> blockwiseConnectedComponents(
			final JavaSparkContext sc, final String inputN5Path, final String inputN5DatasetName,
			final String outputN5Path, final String outputN5DatasetName,
			final double thresholdIntensityCutoff, double minimumVolumeCutoff, List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, inputN5DatasetName);
				
		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<FloatType> sourceInterval = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<FloatType>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)
						),offset, dimension);
			RandomAccessibleInterval<UnsignedByteType> sourceConverted = Converters.convert(sourceInterval,
					(a, b) -> b.set( a.getRealFloat()>=thresholdIntensityCutoff ? 255 : 0), new UnsignedByteType());
			// Create the output based on the current dimensions
			long[] currentDimensions = { 0, 0, 0 };
			sourceInterval.dimensions(currentDimensions);
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType())
					.create(currentDimensions);

			// Compute the connected components which returns the components along the block
			// edges, and update the corresponding blockInformation object
			int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff/Math.pow(pixelResolution[0],3));
			currentBlockInformation = SparkConnectedComponents.computeConnectedComponents(currentBlockInformation, sourceConverted, output, outputDimensions,
					blockSizeL, offset, thresholdIntensityCutoff, minimumVolumeCutoffInVoxels);

			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

			return currentBlockInformation;
		});

		// Run, collect and return blockInformationList
		blockInformationList = javaRDDsets.collect();

		return blockInformationList;
	}

	@SuppressWarnings("unchecked")
	public static final <T extends NativeType<T>> List<BlockInformation> getBlockwiseContactSites(
			final JavaSparkContext sc, final String inputN5Path, final String organelle1,
			final String organelle2, final String outputN5Path, final String outputN5DatasetName,
			final double thresholdIntensityCutoff, double minimumVolumeCutoff, List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data set
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(organelle1);
		final int[] blockSize = attributes.getBlockSize();
		final long[] blockSizeL = new long[] { blockSize[0], blockSize[1], blockSize[2] };
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(n5Reader, organelle1);
				
		// Create output dataset
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];

			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<FloatType> organelle1Data = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<FloatType>) N5Utils.open(n5ReaderLocal, organelle1)
						),offset, dimension);
			final RandomAccessibleInterval<FloatType> organelle2Data = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<FloatType>) N5Utils.open(n5ReaderLocal, organelle2)
					),offset, dimension);
			
			// Create the output based on the current dimensions
			long[] currentDimensions = { 0, 0, 0 };
			organelle1Data.dimensions(currentDimensions);
			final Img<UnsignedByteType> contactSites = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(dimension);
			
			// Cursors
			RandomAccess<FloatType> organelle1RandomAccess = organelle1Data.randomAccess();
			RandomAccess<FloatType> organelle2RandomAccess = organelle2Data.randomAccess();
			RandomAccess<UnsignedByteType> contactSitesRandomAccess = contactSites.randomAccess();

			for(int x=0; x<currentDimensions[0];x++) {
				for(int y=0;y<currentDimensions[1]; y++) {
					for(int z=0; z<currentDimensions[2]; z++) {
						int [] pos = new int[] {x,y,z};
						organelle1RandomAccess.setPosition(pos);
						organelle2RandomAccess.setPosition(pos);
						if(organelle1RandomAccess.get().get()>=thresholdIntensityCutoff && organelle2RandomAccess.get().get()>=thresholdIntensityCutoff) {
							contactSitesRandomAccess.setPosition(pos);
							contactSitesRandomAccess.get().set(1);
						}
					}
				}
			}
	
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType())
					.create(currentDimensions);

			// Compute the connected components which returns the components along the block
			// edges, and update the corresponding blockInformation object
			int minimumVolumeCutoffInVoxels = 0;
			currentBlockInformation = SparkConnectedComponents.computeConnectedComponents(currentBlockInformation, contactSites, output, outputDimensions,
					blockSizeL, offset, thresholdIntensityCutoff, minimumVolumeCutoffInVoxels);

			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

			return currentBlockInformation;
		});

		// Run, collect and return blockInformationList
		blockInformationList = javaRDDsets.collect();

		return blockInformationList;
	}

	public static final void copyN5(
			final JavaSparkContext sc,
			final String inputN5Path,
			final String outputN5Path,
			final String inputDatasetName,
			final String outputDatasetName,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(inputN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();		

		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createDataset(
				outputDatasetName,
				dimensions,
				blockSize,
				DataType.UINT64,
				new GzipCompression());
		n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, inputDatasetName)));

		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(inputN5Path);
			boolean show=false;
			if(show) new ImageJ();
			final RandomAccessibleInterval<UnsignedLongType> source = 
					(RandomAccessibleInterval<UnsignedLongType>)(RandomAccessibleInterval)N5Utils.open(n5BlockReader, inputDatasetName);
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(Views.offsetInterval(Views.extendZero(source), gridBlock[0], gridBlock[1]), n5BlockWriter, outputDatasetName, gridBlock[2]);
		});
	}

	
	

	public static void logMemory(final String context) {
		final long freeMem = Runtime.getRuntime().freeMemory() / 1000000L;
		final long totalMem = Runtime.getRuntime().totalMemory() / 1000000L;
		logMsg(context + ", Total: " + totalMem + " MB, Free: " + freeMem + " MB, Delta: " + (totalMem - freeMem)
				+ " MB");
	}

	public static void logMsg(final String msg) {
		final String ts = new SimpleDateFormat("HH:mm:ss").format(new Date()) + " ";
		System.out.println(ts + " " + msg);
	}

	
	
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkConnectedComponents");

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

	
		List<String> directoriesToDelete = new ArrayList<String>();
		double[] thresholdIntensities = options.getThresholdIntensityCutoff();
		for(int i=0; i<organelles.length;i++) {
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), organelles[i]);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			//Do connected components of mito and er
			String finalOutputN5DatasetName = organelles[i]+"_cc_contact_boundary_temp_to_delete";
			String tempOutputN5DatasetName = finalOutputN5DatasetName + "_blockwise_temp_to_delete";
			blockInformationList = blockwiseConnectedComponents(
					sc, options.getInputN5Path(), organelles[i],
					options.getOutputN5Path(), tempOutputN5DatasetName,
					thresholdIntensities[i], options.getMinimumVolumeCutoff(), blockInformationList); 
	
			double minimumVolumeCutoff = options.getMinimumVolumeCutoff();			
			blockInformationList = SparkConnectedComponents.unionFindConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5DatasetName, minimumVolumeCutoff,
					blockInformationList);	
			SparkConnectedComponents.mergeConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5DatasetName, finalOutputN5DatasetName,
					blockInformationList);
			directoriesToDelete.add(options.getOutputN5Path() + "/" + tempOutputN5DatasetName);
			// done with connected components
			copyN5(sc, options.getInputN5Path(), options.getOutputN5Path(), finalOutputN5DatasetName, organelles[i]+"_cc_pairs_contact_boundary_temp_to_delete", blockInformationList);
			copyN5(sc, options.getInputN5Path(), options.getOutputN5Path(), finalOutputN5DatasetName, organelles[i]+"_cc", blockInformationList);
			sc.close();
			//Remove temporary files
			SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
		}
		organelles[0]+="_cc";
		organelles[1]+="_cc";
		boolean doSelfContacts = true;
		boolean doLM = true;
		SparkContactSites.calculateContactSites(conf, organelles,doSelfContacts, options.getMinimumContactSiteVolumeCutoff(), 0, doLM, options.getInputN5Path(), options.getOutputN5Path(), false);
		//For each pair of object classes, calculate the contact sites and get the connected component information
		/*directoriesToDelete = new ArrayList<String>();
		for (int i = 0; i<organelles.length; i++) {
			final String organelle1 =organelles[i];
			for(int j= i+1; j< organelles.length; j++) {
				String organelle2 = organelles[j];
				List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(), organelle1);
				JavaSparkContext sc = new JavaSparkContext(conf);
				
				final String organelleContactString = organelle1 + "_to_" + organelle2;
				final String tempOutputN5ConnectedComponents = organelleContactString + "_cc_blockwise_temp_to_delete";
				final String finalOutputN5DatasetName = organelleContactString + "_cc";
				
				double minimumVolumeCutoff = options.getMinimumVolumeCutoff();
				blockInformationList = SparkContactSites.blockwiseConnectedComponents(
						sc, options.getOutputN5Path(),
						organelle1, organelle2, 
						options.getOutputN5Path(),
						tempOutputN5ConnectedComponents,
						minimumVolumeCutoff,
						blockInformationList);
				
				HashMap<Long, List<Long>> edgeComponentIDtoOrganelleIDs = new HashMap<Long,List<Long>>();
				for(BlockInformation currentBlockInformation : blockInformationList) {
					edgeComponentIDtoOrganelleIDs.putAll(currentBlockInformation.edgeComponentIDtoOrganelleIDs);
				}
				blockInformationList = SparkContactSites.unionFindConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5ConnectedComponents, minimumVolumeCutoff,edgeComponentIDtoOrganelleIDs, blockInformationList);
				
				SparkConnectedComponents.mergeConnectedComponents(sc, options.getOutputN5Path(), tempOutputN5ConnectedComponents, finalOutputN5DatasetName, blockInformationList);				
				directoriesToDelete.add(options.getOutputN5Path() + "/" + tempOutputN5ConnectedComponents);
				sc.close();
			}
		}
		*/
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
	}
}
