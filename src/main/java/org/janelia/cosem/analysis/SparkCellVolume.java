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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.cosem.util.SparkDirectoryDelete;
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
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Calculate the volume representing the entire cell using ECS and plasma
 * membrane results.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCellVolume {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--ecsN5Path", required = true, usage = "ECS N5 path")
		private String ecsN5Path = null;

		@Option(name = "--plasmaMembraneN5Path", required = false, usage = "Plasma membrane N5 path")
		private String plasmaMembraneN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
		private String outputN5Path = null;

		@Option(name = "--maskN5Path", required = true, usage = "Mask N5 path")
		private String maskN5Path = null;

		@Option(name = "--minimumVolumeCutoff", required = false, usage = "Volume above which objects will be kept (nm^3)")
		private double minimumVolumeCutoff = 20E6;

		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);

				if (outputN5Path == null)
					outputN5Path = plasmaMembraneN5Path;

				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getEcsN5Path() {
			return ecsN5Path;
		}

		public String getPlasmaMembraneN5Path() {
			return plasmaMembraneN5Path;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}

		public double getMinimumVolumeCutoff() {
			return minimumVolumeCutoff;
		}

		public String getMaskN5Path() {
			return maskN5Path;
		}
	}

	/**
	 * Connected components in a blockwise fashion for the cell volume. Calculated
	 * as the space that is not part of the mask, ecs expanded by 3 voxels or the
	 * plasma membrane.
	 * 
	 * @param sc                   Spark context
	 * @param ecsN5Path            ECS N5 path
	 * @param plasmaMembraneN5Path Plasma membrane N5 path
	 * @param maskN5Path           Mask N5 Path
	 * @param outputN5Path         Output N5 Path
	 * @param outputN5DatasetName  Output N5 dataset name
	 * @param minimumVolumeCutoff  Minimum volume above which objects will be kept
	 * @param blockInformationList List of block informations
	 * @return Block information list
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final List<BlockInformation> blockwiseConnectedComponents(final JavaSparkContext sc,
			final String ecsN5Path, final String plasmaMembraneN5Path, final String maskN5Path,
			final String outputN5Path, final String outputN5DatasetName, double minimumVolumeCutoff,
			List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data sets. ECS and plasma membrane should have same
		// properties
		final N5Reader n5Reader = new N5FSReader(ecsN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes("ecs");
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		final double[] pixelResolution = IOHelper.getResolution(n5Reader, "ecs");
		final double[] maskPixelResolution = IOHelper.getResolution(new N5FSReader(maskN5Path), "/volumes/masks/foreground");
		
		// Create output dataset
		ProcessingHelper.createDatasetUsingTemplateDataset(ecsN5Path, "ecs", outputN5Path, outputN5DatasetName);

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<BlockInformation> javaRDDsets = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			long expandBy = 3;
			long expandBySquared = expandBy * expandBy;
			long[] paddedOffset = currentBlockInformation.getPaddedOffset(expandBy); 
			long[] paddedDimension = currentBlockInformation.getPaddedDimension(expandBy);

			// Read in ecs data and calculate distance transform, used for expansion
			RandomAccessibleInterval<UnsignedByteType> ecsPredictionsExpanded = ProcessingHelper.getOffsetIntervalExtendZeroRAI(ecsN5Path, "ecs",
					paddedOffset, paddedDimension);

			final RandomAccessibleInterval<NativeBoolType> ecsPredictionsExpandedBinarized = Converters
					.convert(ecsPredictionsExpanded, (a, b) -> {
						b.set(a.getRealDouble() >= 127 ? true : false);
					}, new NativeBoolType());

			ArrayImg<FloatType, FloatArray> distanceFromExpandedEcs = ArrayImgs.floats(paddedDimension);
			DistanceTransform.binaryTransform(ecsPredictionsExpandedBinarized, distanceFromExpandedEcs,
					DISTANCE_TYPE.EUCLIDIAN);
			IntervalView<FloatType> distanceFromEcs = Views.offsetInterval(distanceFromExpandedEcs,
					new long[] { expandBy, expandBy, expandBy }, dimension);
			Cursor<FloatType> distanceFromEcsCursor = distanceFromEcs.cursor();

			// Read in plasma membrane data
			Cursor<UnsignedLongType> plasmaMembraneCursor = ProcessingHelper.getOffsetIntervalExtendZeroC(plasmaMembraneN5Path, "plasma_membrane", offset, dimension);

			// Read in mask block. Mask scale differs from plasma membrane/ecs scale
			double scale = maskPixelResolution[0] / pixelResolution[0];
			long [] maskOffset = new long[] { (long) (offset[0] / scale), (long) (offset[1] / scale), (long) (offset[2] / scale) };
			long [] maskDimension = new long[] { (long) (dimension[0] / scale), (long) (dimension[1] / scale), (long) (dimension[2] / scale) };
			final RandomAccess<UnsignedByteType> maskRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(maskN5Path, "/volumes/masks/foreground", maskOffset, maskDimension);

			// Create cell volume image
			final Img<UnsignedByteType> cellVolume = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
					.create(dimension);
			Cursor<UnsignedByteType> cellVolumeCursor = cellVolume.cursor();
			
			// Create cell volume
			while (distanceFromEcsCursor.hasNext()) {
				distanceFromEcsCursor.next();
				plasmaMembraneCursor.next();
				cellVolumeCursor.next();
				final long[] positionInMask = { (long) Math.floor(distanceFromEcsCursor.getDoublePosition(0) / scale),
						(long) Math.floor(distanceFromEcsCursor.getDoublePosition(1) / scale),
						(long) Math.floor(distanceFromEcsCursor.getDoublePosition(2) / scale) };
				maskRA.setPosition(positionInMask);

				if (distanceFromEcsCursor.get().get() > expandBySquared && plasmaMembraneCursor.get().get() == 0
						&& maskRA.get().get() > 0) { // then it is neither pm nor ecs
					cellVolumeCursor.get().set(255);
				}
			}

			// Create the output based on the current dimensions
			final Img<UnsignedLongType> output = new ArrayImgFactory<UnsignedLongType>(new UnsignedLongType())
					.create(dimension);

			// Compute the connected components which returns the components along the block
			// edges, and update the corresponding blockInformation object
			int minimumVolumeCutoffInVoxels = (int) Math.ceil(minimumVolumeCutoff / Math.pow(pixelResolution[0], 3));
			currentBlockInformation = SparkConnectedComponents.computeConnectedComponents(currentBlockInformation,
					cellVolume, output, outputDimensions,  new long[] { blockSize[0], blockSize[1], blockSize[2] }, offset, 1, minimumVolumeCutoffInVoxels);

			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

			return currentBlockInformation;
		});

		// Run, collect and return blockInformationList
		blockInformationList = javaRDDsets.collect();

		return blockInformationList;
	}

	/**
	 * Take input arguments and run create cell volume by running connected
	 * components of masked region.
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

		final SparkConf conf = new SparkConf().setAppName("SparkCellVolume");
		String tempOutputN5DatasetName = "cellVolume_blockwise_temp_to_delete";
		String finalOutputN5DatasetName = "cellVolume";

		// Create block information list
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getEcsN5Path(),
				"ecs");

		// Run connected components
		JavaSparkContext sc = new JavaSparkContext(conf);
		String outputN5Path = options.getOutputN5Path();
		blockInformationList = blockwiseConnectedComponents(sc, options.getEcsN5Path(),
				options.getPlasmaMembraneN5Path(), options.getMaskN5Path(), outputN5Path, tempOutputN5DatasetName,
				options.getMinimumVolumeCutoff(), blockInformationList);
		blockInformationList = SparkConnectedComponents.unionFindConnectedComponents(sc, outputN5Path,
				tempOutputN5DatasetName, options.getMinimumVolumeCutoff(), blockInformationList);
		SparkConnectedComponents.mergeConnectedComponents(sc, outputN5Path, tempOutputN5DatasetName,
				finalOutputN5DatasetName, false, blockInformationList);
		sc.close();

		List<String> directoriesToDelete = new ArrayList<String>();
		directoriesToDelete.add(outputN5Path + "/" + tempOutputN5DatasetName);
		SparkDirectoryDelete.deleteDirectories(conf, directoriesToDelete);
	}
}
