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
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.CorrectlyPaddedDistanceTransform;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
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

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * For a given
 * 
 * TODO: start bins at 1 so that wont run into issues with 0 being mixed in with
 * background
 * 
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCalculatePropertiesFromMedialSurface {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "Input N5 path")
	private String inputN5Path = null;

	@Option(name = "--outputDirectory", required = false, usage = "Output directory")
	private String outputDirectory = null;

	@Option(name = "--inputN5DatasetName", required = false, usage = "Onput N5 dataset")
	private String inputN5DatasetName = null;

	@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
	private String outputN5Path = null;

	@Option(name = "--calculateAreaAndVolumeFromExistingDataset", required = false, usage = "Use existing volume averaged sheetness")
	private boolean calculateAreaAndVolumeFromExistingDataset = false;

	public Options(final String[] args) {
	    final CmdLineParser parser = new CmdLineParser(this);
	    try {
		parser.parseArgument(args);
		if (outputN5Path == null)
		    outputN5Path = inputN5Path;
		parsedSuccessfully = true;
	    } catch (final CmdLineException e) {
		parser.printUsage(System.err);
	    }
	}

	public String getInputN5Path() {
	    return inputN5Path;
	}

	public String getInputN5DatasetName() {
	    return inputN5DatasetName;
	}

	public String getOutputDirectory() {
	    if (outputDirectory == null) {
		outputDirectory = outputN5Path.split(".n5")[0] + "_results";
	    }
	    return outputDirectory;
	}

	public String getOutputN5Path() {
	    return outputN5Path;
	}

	public boolean getCalculateAreaAndVolumeFromExistingDataset() {
	    return calculateAreaAndVolumeFromExistingDataset;
	}

    }

    /**
     * Class to contain histograms as maps for sheetness, thickness, surface area
     * and volume
     */
    public static class HistogramMaps implements Serializable {
	private static final long serialVersionUID = 1L;
	public Map<List<Integer>, Long> sheetnessAndThicknessHistogram;
	public Map<Integer, Double> sheetnessAndSurfaceAreaHistogram;
	public Map<Integer, Double> sheetnessAndVolumeHistogram;

	public HistogramMaps(Map<List<Integer>, Long> sheetnessAndThicknessHistogram,
		Map<Integer, Double> sheetnessAndSurfaceAreaHistogram,
		Map<Integer, Double> sheetnessAndVolumeHistogram) {
	    this.sheetnessAndThicknessHistogram = sheetnessAndThicknessHistogram;
	    this.sheetnessAndSurfaceAreaHistogram = sheetnessAndSurfaceAreaHistogram;
	    this.sheetnessAndVolumeHistogram = sheetnessAndVolumeHistogram;
	}

	public void merge(HistogramMaps newHistogramMaps) {
	    // merge holeIDtoObjectIDMap
	    for (Entry<List<Integer>, Long> entry : newHistogramMaps.sheetnessAndThicknessHistogram.entrySet())
		sheetnessAndThicknessHistogram.put(entry.getKey(),
			sheetnessAndThicknessHistogram.getOrDefault(entry.getKey(), 0L) + entry.getValue());

	    // merge holeIDtoVolumeMap
	    for (Entry<Integer, Double> entry : newHistogramMaps.sheetnessAndSurfaceAreaHistogram.entrySet())
		sheetnessAndSurfaceAreaHistogram.put(entry.getKey(),
			sheetnessAndSurfaceAreaHistogram.getOrDefault(entry.getKey(), 0.0) + entry.getValue());

	    // merge objectIDtoVolumeMap
	    for (Entry<Integer, Double> entry : newHistogramMaps.sheetnessAndVolumeHistogram.entrySet())
		sheetnessAndVolumeHistogram.put(entry.getKey(),
			sheetnessAndVolumeHistogram.getOrDefault(entry.getKey(), 0.0) + entry.getValue());

	}

    }

    /**
     * 
     * Calculate the distance transform of a dataset along its medial surface.
     * 
     * @param sc                   Spark context
     * @param n5Path               Path to n5
     * @param datasetName          N5 dataset name
     * @param n5OutputPath         Path to output n5
     * @param blockInformationList Block inforation list
     * @return blockInformationList
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> List<BlockInformation> calculateDistanceTransformAtMedialSurface(final JavaSparkContext sc,
	    final String n5Path, final String datasetName, final String n5OutputPath,
	    final List<BlockInformation> blockInformationList) throws IOException {

	String outputDatasetName = datasetName + "_medialSurfaceDistanceTransform";
	// General information
	final int[] blockSize = new N5FSReader(n5Path).getDatasetAttributes(datasetName).getBlockSize();
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, datasetName, n5OutputPath, outputDatasetName,
		DataType.FLOAT32);

	// calculate distance from medial surface
	// Parallelize analysis over blocks
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<BlockInformation> blockInformationListWithPadding = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final long[] offset = gridBlock[0];
	    final long[] dimension = gridBlock[1];
	    final N5Reader n5BlockReader = new N5FSReader(n5Path);

	    // Get correctly padded distance transform first
	    RandomAccessibleInterval<T> segmentation = (RandomAccessibleInterval<T>) N5Utils
		    .open(n5BlockReader, datasetName);
	    CorrectlyPaddedDistanceTransform cpdt = new CorrectlyPaddedDistanceTransform(segmentation, offset,
		    dimension);
	    RandomAccess<FloatType> distanceTransformRA = cpdt.correctlyPaddedDistanceTransform.randomAccess();

	    RandomAccess<T> medialSurfaceRandomAccess = ProcessingHelper.getOffsetIntervalExtendZeroRA(
		    n5Path, datasetName + "_medialSurface", cpdt.paddedOffset, cpdt.paddedDimension);

	    blockInformation.paddingForMedialSurface = 0;

	    for (long x = cpdt.padding[0]; x < cpdt.paddedDimension[0] - cpdt.padding[0]; x++) {
		for (long y = cpdt.padding[1]; y < cpdt.paddedDimension[1] - cpdt.padding[1]; y++) {
		    for (long z = cpdt.padding[2]; z < cpdt.paddedDimension[2] - cpdt.padding[2]; z++) {
			long[] pos = new long[] { x, y, z };
			medialSurfaceRandomAccess.setPosition(pos);
			if (medialSurfaceRandomAccess.get().getIntegerLong() > 0) {
			    distanceTransformRA.setPosition(pos);
			    float dist = (float) Math.sqrt(distanceTransformRA.get().get());

			    for (int i = 0; i < 3; i++) {
				float negativePadding = dist - (pos[i] - cpdt.padding[i]);

				long positiveBorder = cpdt.paddedDimension[i] - cpdt.padding[i] - 1;
				float positivePadding = dist - (positiveBorder - pos[i]);

				int maxPadding = (int) Math.ceil(Math.max(negativePadding, positivePadding)) + 1;

				if (maxPadding > blockInformation.paddingForMedialSurface) {
				    blockInformation.paddingForMedialSurface = maxPadding;
				}
			    }

			}
		    }
		}
	    }
	    IntervalView<FloatType> output = Views.offsetInterval(cpdt.correctlyPaddedDistanceTransform, cpdt.padding,
		    dimension);

	    // Write out volume averaged sheetness
	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);

	    return blockInformation;
	});

	List<BlockInformation> updatedBlockInformationList = blockInformationListWithPadding.collect();
	int[] maxPaddingForBlock = new int[updatedBlockInformationList.size()];
	for (int i = 0; i < updatedBlockInformationList.size(); i++) {
	    maxPaddingForBlock[i] = updatedBlockInformationList.get(i).paddingForMedialSurface;
	}

	for (int i = 0; i < updatedBlockInformationList.size(); i++) {
	    BlockInformation blockInformationOne = updatedBlockInformationList.get(i);
	    int paddingForMedialSurfaceOne = blockInformationOne.paddingForMedialSurface;
	    int numberOfBlocksToExpandBy = (int) Math.ceil(paddingForMedialSurfaceOne * 1.0 / blockSize[0]) + 1;

	    long[] gridLocationOne = blockInformationOne.gridBlock[2];
	    for (int j = 0; j < updatedBlockInformationList.size(); j++) {

		BlockInformation blockInformationTwo = updatedBlockInformationList.get(j);
		long[] gridLocationTwo = blockInformationTwo.gridBlock[2];
		long dx = gridLocationOne[0] - gridLocationTwo[0];
		long dy = gridLocationOne[1] - gridLocationTwo[1];
		long dz = gridLocationOne[2] - gridLocationTwo[2];

		double gridDistance = Math.sqrt(dx * dx + dy * dy + dz * dz);
		if (gridDistance < numberOfBlocksToExpandBy) {
		    if (paddingForMedialSurfaceOne > maxPaddingForBlock[j]) {
			// in case it goes beyond the neighboring, box, need to expand it by the box
			// dimension
			maxPaddingForBlock[j] = paddingForMedialSurfaceOne;
		    }
		}
	    }

	}
	for (int i = 0; i < updatedBlockInformationList.size(); i++) {
	    updatedBlockInformationList.get(i).paddingForMedialSurface = maxPaddingForBlock[i];
	    System.out.println(updatedBlockInformationList.get(i).paddingForMedialSurface);
	}

	return updatedBlockInformationList;
    }

    /**
     * 
     * Create a sheetness volume from the medial surface by expanding the medial
     * surface with a sphere centered at each voxel in the medial surface, with a
     * radius equal to the distance transform at that voxel. The value of voxels
     * within the sphere is the sheetness value at the center voxel, with all voxels
     * in the the resultant volume set to the average value of all spheres
     * containing that voxel.
     * 
     * @param sc                   Spark context
     * @param n5Path               Path to n5
     * @param datasetName          N5 dataset name
     * @param n5OutputPath         Path to output n5
     * @param blockInformationList Block inforation list
     * @return Histogram maps class with sheetness histograms
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> Map<List<Integer>, Long> projectCurvatureToSurface(final JavaSparkContext sc,
	    final String n5Path, final String datasetName, final String n5OutputPath,
	    final List<BlockInformation> blockInformationList) throws IOException {

	// General information
	final N5Reader n5Reader = new N5FSReader(n5Path);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
	final int[] blockSize = attributes.getBlockSize();
	double[] pixelResolution = IOHelper.getResolution(n5Reader, datasetName);
	double voxelVolume = pixelResolution[0] * pixelResolution[1] * pixelResolution[2];
	double voxelFaceArea = pixelResolution[0] * pixelResolution[1];

	// Create output that will contain volume averaged sheetness
	//TODO: check to make sure that for sphere to influence volume averaged sheetness of voxel, it can't pass through background
	String outputDatasetName = datasetName + "_sheetnessVolumeAveraged";
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, datasetName, n5OutputPath, outputDatasetName,
		DataType.UINT8);

	// Parallelize analysis over blocks
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<Map<List<Integer>, Long>> javaRDDHistogramMaps = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final long padding = blockInformation.paddingForMedialSurface;
	    final long[] paddedOffset = blockInformation.getPaddedOffset(padding);
	    final long[] dimension = gridBlock[1];
	    final long[] paddedDimension = blockInformation.getPaddedDimension(padding);

	    // Create images required to calculate the average sheetness at a voxel: the sum
	    // and the counts at a given voxel. Add an extra 2 because need to check for
	    // surface voxels so need extra border of 1
	    final Img<FloatType> sheetnessSum = new ArrayImgFactory<FloatType>(new FloatType())
		    .create(new long[] { blockSize[0] + 2, blockSize[1] + 2, blockSize[2] + 2 });
	    final Img<UnsignedIntType> counts = new ArrayImgFactory<UnsignedIntType>(new UnsignedIntType())
		    .create(new long[] { blockSize[0] + 2, blockSize[1] + 2, blockSize[2] + 2 });
	    RandomAccess<FloatType> sheetnessSumRA = sheetnessSum.randomAccess();
	    RandomAccess<UnsignedIntType> countsRA = counts.randomAccess();

	    // Update sheetness and thickness histogram, as well as sum and counts used to
	    // create volume averaged sheetness
	    Map<List<Integer>, Long> sheetnessAndThicknessHistogram = new HashMap<List<Integer>, Long>();

	    { // Necessary? scoping to limit memory
	      // Get corresponding medial surface and sheetness
		Cursor<T> medialSurfaceCursor = ProcessingHelper.getOffsetIntervalExtendZeroC(n5Path,
			datasetName + "_medialSurface", paddedOffset, paddedDimension);
		RandomAccess<DoubleType> sheetnessRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(n5Path,
			datasetName + "_sheetness", paddedOffset, paddedDimension);
		RandomAccess<FloatType> distanceTransformRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(
			n5OutputPath, datasetName + "_medialSurfaceDistanceTransform", paddedOffset, paddedDimension);

		createSumAndCountsAndUpdateHistogram(medialSurfaceCursor, distanceTransformRA, sheetnessRA,
			sheetnessSumRA, countsRA, pixelResolution, padding, paddedDimension,
			sheetnessAndThicknessHistogram);
	    }

	    // Take average
	    final Img<UnsignedByteType> output = createVolumeAveragedSheetnessAndUpdateHistograms(dimension,
		    sheetnessSumRA, countsRA, voxelVolume, voxelFaceArea);

	    // Write out volume averaged sheetness
	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
	    return sheetnessAndThicknessHistogram;
	});

	Map<List<Integer>, Long> sheetnessAndThicknessHistogram = javaRDDHistogramMaps.reduce((a, b) -> {
	    for (Entry<List<Integer>, Long> entry : b.entrySet()) {
		a.put(entry.getKey(), a.getOrDefault(entry.getKey(), 0L) + entry.getValue());
	    }
	    return a;
	});

	return sheetnessAndThicknessHistogram;

    }

    /**
     * 
     * Calculate sheetness and volume histograms
     * 
     * @param sc                   Spark context
     * @param n5Path               Path to n5
     * @param datasetName          N5 dataset name
     * @param n5OutputPath         Path to output n5
     * @param blockInformationList Block inforation list
     * @return Histogram maps class with sheetness histograms
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final HistogramMaps calculateSheetnessAndVolume(final JavaSparkContext sc, final String datasetName,
	    final String n5OutputPath, final List<BlockInformation> blockInformationList) throws IOException {

	// General information
	final N5Reader n5Reader = new N5FSReader(n5OutputPath);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
	final long[] dimensions = attributes.getDimensions();
	double[] pixelResolution = IOHelper.getResolution(n5Reader, datasetName);
	double voxelVolume = pixelResolution[0] * pixelResolution[1] * pixelResolution[2];
	double voxelFaceArea = pixelResolution[0] * pixelResolution[1];

	// Create output that will contain volume averaged sheetness

	// Parallelize analysis over blocks
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<HistogramMaps> javaRDDHistogramMaps = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final long[] dimension = gridBlock[1];
	    final long[] paddedOffset = blockInformation.getPaddedOffset(1);
	    final long[] paddedDimension = blockInformation.getPaddedDimension(1);

	    final N5Reader n5BlockReader = new N5FSReader(n5OutputPath);

	    // Get corresponding medial surface and sheetness
	    RandomAccess<UnsignedByteType> sheetnessRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(n5OutputPath,
		    datasetName, paddedOffset, paddedDimension);

	    Map<List<Integer>, Long> sheetnessAndThicknessHistogram = new HashMap<List<Integer>, Long>();
	    Map<Integer, Double> sheetnessAndSurfaceAreaHistogram = new HashMap<Integer, Double>();
	    Map<Integer, Double> sheetnessAndVolumeHistogram = new HashMap<Integer, Double>();

	    for (long x = 1; x < dimension[0] + 1; x++) {
		for (long y = 1; y < dimension[1] + 1; y++) {
		    for (long z = 1; z < dimension[2] + 1; z++) {
			long[] pos = new long[] { x, y, z };
			sheetnessRA.setPosition(pos);
			int sheetnessMeasureBin = sheetnessRA.get().get();
			if (sheetnessMeasureBin > 0) {
			    sheetnessAndVolumeHistogram.put(sheetnessMeasureBin,
				    sheetnessAndVolumeHistogram.getOrDefault(sheetnessMeasureBin, 0.0) + voxelVolume);
			    int faces = ProcessingHelper.getSurfaceAreaContributionOfVoxelInFaces(sheetnessRA,
				    paddedOffset, dimensions);
			    if (faces > 0) {
				sheetnessAndSurfaceAreaHistogram.put(sheetnessMeasureBin,
					sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessMeasureBin, 0.0)
						+ faces * voxelFaceArea);
			    }
			}
		    }
		}
	    }
	    return new HistogramMaps(sheetnessAndThicknessHistogram, sheetnessAndSurfaceAreaHistogram,
		    sheetnessAndVolumeHistogram);
	});

	HistogramMaps histogramMaps = javaRDDHistogramMaps.reduce((a, b) -> {
	    a.merge(b);
	    return a;
	});

	return histogramMaps;

    }

    /**
     * Update sheetness sum and count images which are used to calculate the volume
     * averaged sheetness. The averaged sheetness is calculated by taking the radius
     * (thickness) at a medial surface voxel, and filing in all voxels within that
     * sphere with the corresponding sheetness. Sum and counts are used to take the
     * average in cases where multiple spheres contain a single voxel.
     * 
     * @param pos               Position
     * @param radiusPlusPadding Radius with padding
     * @param radiusSquared     Radius squared
     * @param cpdt              Correctly padded distance transform instance
     * @param sheetnessMeasure  Sheetness value
     * @param sheetnessSumRA    Random access for sheetness sum
     * @param countsRA          Random access for counts
     */
    private static void updateSheetnessSumAndCount(int[] pos, int radiusPlusPadding, float radiusSquared,
	    float sheetnessMeasure, RandomAccess<FloatType> sheetnessSumRA, RandomAccess<UnsignedIntType> countsRA,
	    long padding, long[] paddedDimension) {

	for (int x = pos[0] - radiusPlusPadding; x <= pos[0] + radiusPlusPadding; x++) {
	    for (int y = pos[1] - radiusPlusPadding; y <= pos[1] + radiusPlusPadding; y++) {
		for (int z = pos[2] - radiusPlusPadding; z <= pos[2] + radiusPlusPadding; z++) {
		    int dx = x - pos[0];
		    int dy = y - pos[1];
		    int dz = z - pos[2];
		    // need to check extra padding of 1 because in next step we need this halo for
		    // checking surfaces
		    if ((x >= padding - 1 && x <= paddedDimension[0] - padding && y >= padding - 1
			    && y <= paddedDimension[1] - padding && z >= padding - 1
			    && z <= paddedDimension[2] - padding) && dx * dx + dy * dy + dz * dz <= radiusSquared) {
			// then it is in sphere
			long[] spherePos = new long[] { x - (padding - 1), y - (padding - 1), z - (padding - 1) };

			sheetnessSumRA.setPosition(spherePos);
			FloatType outputVoxel = sheetnessSumRA.get();
			outputVoxel.set(outputVoxel.get() + sheetnessMeasure);

			countsRA.setPosition(spherePos);
			UnsignedIntType countsVoxel = countsRA.get();
			countsVoxel.set(countsVoxel.get() + 1);

		    }
		}
	    }
	}
    }

    /**
     * Use distance transform to get thickness at medial surface. Use that thickness
     * as a radius for spheres to calculate sum and counts for volume averaged
     * sheetness.
     * 
     * @param medialSurfaceCursor            Cursor for medial surface
     * @param cpdt                           Correctly padded distance transform
     *                                       class instance
     * @param sheetnessRA                    Sheetness random access
     * @param sheetnessSumRA                 Sheetness sum random access
     * @param countsRA                       Counts random access
     * @param pixelResolution                Pixel resolution
     * @param sheetnessAndThicknessHistogram Histogram as a map containing sheetness
     *                                       and thickness
     */
    private static <T extends IntegerType<T> & NativeType<T>> void  createSumAndCountsAndUpdateHistogram(Cursor<T> medialSurfaceCursor,
	    RandomAccess<FloatType> distanceTransformRA, RandomAccess<DoubleType> sheetnessRA,
	    RandomAccess<FloatType> sheetnessSumRA, RandomAccess<UnsignedIntType> countsRA, double[] pixelResolution,
	    long padding, long[] paddedDimension, Map<List<Integer>, Long> sheetnessAndThicknessHistogram) {
	while (medialSurfaceCursor.hasNext()) {
	    final long medialSurfaceValue = medialSurfaceCursor.next().getIntegerLong();
	    if (medialSurfaceValue > 0) { // then it is on medial surface

		int[] pos = { medialSurfaceCursor.getIntPosition(0), medialSurfaceCursor.getIntPosition(1),
			medialSurfaceCursor.getIntPosition(2) };
		distanceTransformRA.setPosition(pos);
		sheetnessRA.setPosition(pos);

		float radiusSquared = distanceTransformRA.get().getRealFloat();
		double radius = Math.sqrt(radiusSquared);
		int radiusPlusPadding = (int) Math.ceil(radius);

		float sheetnessMeasure = sheetnessRA.get().getRealFloat();
		int sheetnessMeasureBin = (int) Math.ceil(sheetnessMeasure * 254) + 1;

		if (pos[0] >= padding && pos[0] < paddedDimension[0] - padding && pos[1] >= padding
			&& pos[1] < paddedDimension[1] - padding && pos[2] >= padding
			&& pos[2] < paddedDimension[2] - padding) {

		    double thickness = radius * 2;// convert later
		    // bin thickness in 8 nm bins
		    int thicknessBin = (int) Math.min(Math.floor(thickness * pixelResolution[0] / 8), 99);

		    List<Integer> histogramBinList = Arrays.asList(sheetnessMeasureBin, thicknessBin);
		    sheetnessAndThicknessHistogram.put(histogramBinList,
			    sheetnessAndThicknessHistogram.getOrDefault(histogramBinList, 0L) + 1L);
		}

		updateSheetnessSumAndCount(pos, radiusPlusPadding, radiusSquared, sheetnessMeasure, sheetnessSumRA,
			countsRA, padding, paddedDimension);
	    }
	}
    }

    /**
     * Create output volume averaged sheetness by dividing Sum image by Counts
     * image, and binning from 0-255.
     * 
     * @param dimension                        Block dimension
     * @param sheetnessSumRA                   Sheetness sum random access
     * @param countsRA                         Counts random access
     * @param voxelVolume                      Volume of a voxel
     * @param voxelFaceArea                    Area of a voxel face
     * @param sheetnessAndSurfaceAreaHistogram Histogram for sheetness and surface
     *                                         area
     * @param sheetnessAndVolumeHistogram      Histogram for sheetness and volume
     * @return
     */
    private static Img<UnsignedByteType> createVolumeAveragedSheetnessAndUpdateHistograms(long[] dimension,
	    RandomAccess<FloatType> sheetnessSumRA, RandomAccess<UnsignedIntType> countsRA, double voxelVolume,
	    double voxelFaceArea) {

	final Img<UnsignedByteType> output = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
		.create(dimension);
	RandomAccess<UnsignedByteType> outputRandomAccess = output.randomAccess();
	for (long x = 1; x < dimension[0] + 1; x++) {
	    for (long y = 1; y < dimension[1] + 1; y++) {
		for (long z = 1; z < dimension[2] + 1; z++) {

		    long[] pos = new long[] { x, y, z };
		    countsRA.setPosition(pos);
		    if (countsRA.get().get() > 0) {
			sheetnessSumRA.setPosition(pos);
			float sheetnessMeasure = sheetnessSumRA.get().get() / countsRA.get().get();
			sheetnessSumRA.get().set(sheetnessMeasure);// take average
			int sheetnessMeasureBin = (int) Math.ceil(sheetnessMeasure * 254) + 1;// add one in case any are
											      // 0

			outputRandomAccess.setPosition(new long[] { x - 1, y - 1, z - 1 });

			// rescale to 0-255
			outputRandomAccess.get().set(sheetnessMeasureBin);
		    }
		}
	    }
	}
	return output;
    }

    /**
     * Function to write out histograms.
     * 
     * @param histogramMaps   Maps containing histograms
     * @param outputDirectory Output directory to write files to
     * @param datasetName     Dataset name that is being analyzed
     * @throws IOException
     */
    public static void writeData(HistogramMaps histogramMaps, String outputDirectory, String datasetName,
	    boolean writeThickness) throws IOException {
	if (!new File(outputDirectory).exists()) {
	    new File(outputDirectory).mkdirs();
	}

	FileWriter sheetnessVolumeAndAreaHistograms = new FileWriter(
		outputDirectory + "/" + datasetName + "_sheetnessVolumeAndAreaHistograms.csv");
	sheetnessVolumeAndAreaHistograms.append("Sheetness,Volume (nm^3),Surface Area(nm^2)\n");

	FileWriter sheetnessVsThicknessHistogram = null;
	String rowString;
	if (writeThickness) {
	    sheetnessVsThicknessHistogram = new FileWriter(
		    outputDirectory + "/" + datasetName + "_sheetnessVsThicknessHistogram.csv");
	    rowString = "Sheetness/Thickness (nm)";
	    for (int thicknessBin = 0; thicknessBin < 100; thicknessBin++) {
		rowString += "," + Integer.toString(thicknessBin * 8 + 4);
	    }
	    sheetnessVsThicknessHistogram.append(rowString + "\n");
	}

	for (int sheetnessBin = 1; sheetnessBin < 256; sheetnessBin++) {
	    double volume = histogramMaps.sheetnessAndVolumeHistogram.getOrDefault(sheetnessBin, 0.0);
	    double surfaceArea = histogramMaps.sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessBin, 0.0);

	    String sheetnessBinString = Double.toString((sheetnessBin - 1) / 255.0 + 0.5 / 255.0);
	    sheetnessVolumeAndAreaHistograms.append(
		    sheetnessBinString + "," + Double.toString(volume) + "," + Double.toString(surfaceArea) + "\n");

	    if (writeThickness) {
		rowString = sheetnessBinString;
		for (int thicknessBin = 0; thicknessBin < 100; thicknessBin++) {
		    double thicknessCount = histogramMaps.sheetnessAndThicknessHistogram
			    .getOrDefault(Arrays.asList(sheetnessBin, thicknessBin), 0L);
		    rowString += "," + Double.toString(thicknessCount);
		}
		sheetnessVsThicknessHistogram.append(rowString + "\n");
	    }
	}
	sheetnessVolumeAndAreaHistograms.flush();
	sheetnessVolumeAndAreaHistograms.close();

	if (writeThickness) {
	    sheetnessVsThicknessHistogram.flush();
	    sheetnessVsThicknessHistogram.close();
	}

    }

    public static void setupSparkAndCalculatePropertiesFromMedialSurface(String inputN5Path, String inputN5DatasetName,
	    String outputN5Path, String outputDirectory, boolean calculateAreaAndVolumeFromExistingDataset)
	    throws IOException {
	
	final SparkConf conf = new SparkConf().setAppName("SparkCalculatePropertiesOfMedialSurface");

	for (String organelle : inputN5DatasetName.split(",")) {
	    List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path,
		    organelle);

	    JavaSparkContext sc = new JavaSparkContext(conf);
	    if (calculateAreaAndVolumeFromExistingDataset) {
		HistogramMaps histogramMaps = calculateSheetnessAndVolume(sc, organelle, outputN5Path,
			blockInformationList);
		writeData(histogramMaps, outputDirectory, organelle, false);
	    } else {
		blockInformationList = calculateDistanceTransformAtMedialSurface(sc, inputN5Path, organelle,
			outputN5Path, blockInformationList);
		Map<List<Integer>, Long> sheetnessAndThicknessHistogram = projectCurvatureToSurface(sc, inputN5Path,
			organelle, outputN5Path, blockInformationList);
		HistogramMaps histogramMaps = calculateSheetnessAndVolume(sc, organelle + "_sheetnessVolumeAveraged",
			outputN5Path, blockInformationList);
		histogramMaps.sheetnessAndThicknessHistogram = sheetnessAndThicknessHistogram;
		writeData(histogramMaps, outputDirectory, organelle, true);
	    }

	    sc.close();
	}
    }

    /**
     * Take input args and perform the calculation
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

	String inputN5Path = options.getInputN5Path();
	String inputN5DatasetName = options.getInputN5DatasetName();
	String outputN5Path = options.getOutputN5Path();
	String outputDirectory = options.getOutputDirectory();
	boolean calculateAreaAndVolumeFromExistingDataset = options.getCalculateAreaAndVolumeFromExistingDataset();

	setupSparkAndCalculatePropertiesFromMedialSurface(inputN5Path, inputN5DatasetName, outputN5Path, outputDirectory,
		calculateAreaAndVolumeFromExistingDataset);

    }

}
