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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
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
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

/**
 * Expand a given dataset to mask out predictions for improving downstream analysis.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkExpandMaskToCleanPredictions {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--datasetToMaskN5Path", required = true, usage = "Dataset to mask N5 path")
		private String datasetToMaskN5Path = null;
		
		@Option(name = "--datasetToUseAsMaskN5Path", required = true, usage = "Dataset to use as mask N5 path")
		private String datasetToUseAsMaskN5Path = null;

		@Option(name = "--outputN5Path", required = true, usage = "Output N5 path")
		private String outputN5Path = null;

		@Option(name = "--datasetNameToMask", required = false, usage = "Dataset name to mask")
		private String datasetNameToMask = null;
		
		@Option(name = "--datasetNameToUseAsMask", required = false, usage = "Dataset name to use as mask")
		private String datasetNameToUseAsMask = null;
		
		@Option(name = "--thresholdIntensityCutoff", required = false, usage = "Threshold intensity cutoff for mask")
		private double thresholdIntensityCutoff = 127;
		
		@Option(name = "--onlyKeepLargestComponent", required = false, usage = "Keep only the largest connected component")
		private boolean onlyKeepLargestComponent = false;
		
		@Option(name = "--skipConnectedComponents", required = false, usage = "Skip connected components")
		private boolean skipConnectedComponents = false;
		
		@Option(name = "--expansion", required = false, usage = "Expansion in nm")
		private Integer expansion = 160;
		
		@Option(name = "--skipSmoothing", required = false, usage = "Skip smoothing")
		private boolean skipSmoothing = false;
		
		@Option(name = "--keepWithinMask", required = false, usage = "If true, keep data within the mask region")
		private boolean keepWithinMask = false;

		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getDatasetToMaskN5Path() {
			return datasetToMaskN5Path;
		}
		
		public String getDatasetToUseAsMaskN5Path() {
			return datasetToUseAsMaskN5Path;
		}

		public String getDatasetNameToMask() {
			return datasetNameToMask;
		}
		

		public String getDatasetNameToUseAsMask() {
			return datasetNameToUseAsMask;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public double getThresholdIntensityCutoff() {
			return thresholdIntensityCutoff;
		}
		
		public boolean getOnlyKeepLargestComponent() {
			return onlyKeepLargestComponent;
		}
		
		public Integer getExpansion() {
			return expansion;
		}
		
		public boolean getSkipConnectedComponents() {
			return skipConnectedComponents;
		}
		
		public boolean getSkipSmoothing() {
			return skipSmoothing;
		}
		
		public boolean getKeepWithinMask() {
			return keepWithinMask;
		}

	}

	/**
	 * Use as segmented dataset to mask out a prediction dataset, where the mask can either be inclusive or exclusive.
	 * @param sc						Spark context
	 * @param datasetToMaskN5Path		N5 path for dataset that will be masked
	 * @param datasetNameToMask			Dataset name that will be masked
	 * @param datasetToUseAsMaskN5Path	N5 path for mask dataset 
	 * @param datasetNameToUseAsMask	Dataset name to use as mask
	 * @param n5OutputPath				Output N5 path
	 * @param expansion					Mask expansion in nm
	 * @param keepWithinMask			If true, keep data that is within mask; else exclude data within mask
	 * @param blockInformationList		List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final void expandAndApplyMask(
			final JavaSparkContext sc,
			final String datasetToMaskN5Path,
			final String datasetNameToMask,
			final String datasetToUseAsMaskN5Path,
			final String datasetNameToUseAsMask,
			final String n5OutputPath,
			final Integer expansion,
			final boolean keepWithinMask,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(datasetToMaskN5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetNameToMask);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();		

		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		String maskedDatasetName = datasetNameToMask + "_maskedWith_"+datasetNameToUseAsMask+"_expansion_"+Integer.toString(expansion);
		n5Writer.createDataset(
				maskedDatasetName,
				dimensions,
				blockSize,
				DataType.UINT8,
				new GzipCompression());
		double[] pixelResolution = IOHelper.getResolution(n5Reader, datasetNameToMask);
		n5Writer.setAttribute(maskedDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		final int expansionInVoxels = (int) Math.ceil(expansion/pixelResolution[0]);
		
		
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(blockInformation -> {
			int padding = expansionInVoxels+1;
			final long [] offset= blockInformation.gridBlock[0];
			final long [] dimension = blockInformation.gridBlock[1];
			final N5Reader n5MaskReader = new N5FSReader(datasetToUseAsMaskN5Path);
			final N5Reader n5BlockReader = new N5FSReader(datasetToMaskN5Path);
			final long [] paddedBlockMin =  new long [] {offset[0]-padding, offset[1]-padding, offset[2]-padding};
			final long [] paddedBlockSize =  new long [] {dimension[0]+2*padding, dimension[1]+2*padding, dimension[2]+2*padding};
			
			final RandomAccessibleInterval<UnsignedByteType> dataToMask;
			try {
				dataToMask = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<UnsignedByteType>)N5Utils.open(n5BlockReader, datasetNameToMask)), offset, dimension);
			} catch (Exception e) {
				System.out.println(datasetToMaskN5Path+" "+datasetNameToMask);
				System.out.println(Arrays.toString(offset));
				System.out.println(Arrays.toString(dimension));
				throw e;
			}
			
			final RandomAccessibleInterval<UnsignedLongType> maskDataPreExpansion = Views.offsetInterval(Views.extendZero((RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5MaskReader, datasetNameToUseAsMask)), paddedBlockMin, paddedBlockSize);
			final RandomAccessibleInterval<NativeBoolType> maskDataPreExpansionConverted = Converters.convert(
					maskDataPreExpansion,
					(a, b) -> {
						b.set(a.getIntegerLong()>0);
					},
					new NativeBoolType());
			
			ArrayImg<FloatType, FloatArray> distanceTransform = ArrayImgs.floats(paddedBlockSize);
			DistanceTransform.binaryTransform(maskDataPreExpansionConverted, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);

			RandomAccess<FloatType> distanceTransformRA = distanceTransform.randomAccess();
			RandomAccess<UnsignedByteType> dataToMaskRA = dataToMask.randomAccess();
			
			int expansionInVoxelsSquared = expansionInVoxels*expansionInVoxels;
			for(int x=padding; x<dimension[0]+padding; x++) {
				for(int y= padding; y<dimension[1]+padding; y++) {
					for(int z=padding; z<dimension[2]+padding; z++) {
						long [] pos = new long[] {x, y, z};
						distanceTransformRA.setPosition(pos);
						if(distanceTransformRA.get().get() <= expansionInVoxelsSquared ) {
							if(!keepWithinMask) {//then use mask as regions to set to 0
								long [] newPos = new long[] {x-padding, y-padding, z-padding};
								dataToMaskRA.setPosition(newPos);
								dataToMaskRA.get().set( 0 );
							}
						}
						else { //set region outside mask to 0
							if(keepWithinMask) {
								long [] newPos = new long[] {x-padding, y-padding, z-padding};
								dataToMaskRA.setPosition(newPos);
								dataToMaskRA.get().set( 0 );
							}
						}
						

					}
				}
			}
			
			final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			N5Utils.saveBlock(dataToMask, n5BlockWriter, maskedDatasetName, blockInformation.gridBlock[2]);
		
		});
	}
	
	/**
	 * Perform connected components on mask - if it is not already a segmented dataset - and use expanded version of segmented dataset as mask for prediction dataset.
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
		
		final SparkConf conf = new SparkConf().setAppName("SparkExpandMaskToCleanPredictions");

		double x = (options.getThresholdIntensityCutoff() - 127)/128;
		double thresholdDistance = 50 * 0.5*Math.log ((1.0 + x)/ (1.0 - x) );
		
		//SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow(conf, options.getDatasetNameToUseAsMask(), options.getDatasetToUseAsMaskN5Path(), null, options.getOutputN5Path(), "_largestComponent", 0, -1, true);
		String datasetToUseAsMaskN5Path = options.getDatasetToUseAsMaskN5Path();
		String suffix = "";
		
		List<BlockInformation> blockInformationList;
		if(!options.getSkipConnectedComponents()) {
			blockInformationList = BlockInformation.buildBlockInformationList(options.getDatasetToUseAsMaskN5Path(), options.getDatasetNameToUseAsMask());
			suffix = options.getOnlyKeepLargestComponent() ? "_largestComponent" :  "_cc";
			boolean smooth = !options.getSkipSmoothing();
			SparkConnectedComponents.standardConnectedComponentAnalysisWorkflow(options.getDatasetNameToUseAsMask(), options.getDatasetToUseAsMaskN5Path(), null, options.getOutputN5Path(), suffix, thresholdDistance, -1, options.getOnlyKeepLargestComponent(), smooth);
			datasetToUseAsMaskN5Path = options.getOutputN5Path();
		}
		
		blockInformationList = BlockInformation.buildBlockInformationList(options.getDatasetToMaskN5Path(), options.getDatasetNameToMask());
		JavaSparkContext sc = new JavaSparkContext(conf);
		expandAndApplyMask(
				sc,
				options.getDatasetToMaskN5Path(),
				options.getDatasetNameToMask(),
				datasetToUseAsMaskN5Path,
				options.getDatasetNameToUseAsMask()+suffix,
				options.getOutputN5Path(),
				options.getExpansion(),
				options.getKeepWithinMask(),
				blockInformationList) ;

	}
}
