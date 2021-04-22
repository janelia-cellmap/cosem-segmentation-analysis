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
import java.io.FilenameFilter;
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
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.Views;

/**
 * Calculate sheetness at contact sites. Outputs file with histogram of
 * sheetness vs surface area at contact sites based on sheetness in
 * inputN5SheetnessDatasetName
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCalculateSheetnessOfContactSites {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "Input N5 path")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "Output directory")
		private String outputDirectory = null;

		@Option(name = "--inputN5SheetnessDatasetName", required = false, usage = "Volume averaged sheetness N5 dataset")
		private String inputN5SheetnessDatasetName = null;

		@Option(name = "--inputN5ContactSiteDatasetName", required = false, usage = "Contact site N5 dataset")
		private String inputN5ContactSiteDatasetName = null;

		public Options(final String[] args) {
			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				parser.printUsage(System.err);
			}
		}

		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5SheetnessDatasetName() {
			return inputN5SheetnessDatasetName;
		}

		public String getInputN5ContactSiteDatasetName() {
			return inputN5ContactSiteDatasetName;
		}

		public String getOutputDirectory() {
			if (outputDirectory == null) {
				outputDirectory = inputN5Path.split(".n5")[0] + "_results";
			}
			return outputDirectory;
		}

	}

	/**
	 * Calculates histograms of the sheetness of the desired contact site surface
	 * voxels.
	 * 
	 * @param sc                                 Spark context
	 * @param n5Path                             Input N5 path
	 * @param volumeAveragedSheetnessDatasetName Dataset name for volume averaged
	 *                                           sheetness
	 * @param contactSiteName                    Dataset name corresponding to
	 *                                           desired contact sites
	 * @param referenceOrganelleName             Organelle for which sheetness was calculated                   
	 * @param blockInformationList               Block information list
	 * @return Map of histogram data
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final SheetnessMaps getContactSiteAndOrganelleSheetness(final JavaSparkContext sc,
			final String n5Path, final String volumeAveragedSheetnessDatasetName, final String contactSiteName,
			final String referenceOrganelleName, final List<BlockInformation> blockInformationList) throws IOException {

		// Set up reader and get information about dataset
		final N5Reader n5Reader = new N5FSReader(n5Path);
		double[] pixelResolution = IOHelper.getResolution(n5Reader, volumeAveragedSheetnessDatasetName);
		double voxelFaceArea = pixelResolution[0] * pixelResolution[1];

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(volumeAveragedSheetnessDatasetName);
		final long[] dimensions = attributes.getDimensions();

		// Acquire histograms in a blockwise manner
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<SheetnessMaps> javaRDDSheetnessMaps = rdd.map(blockInformation -> {
			long[] paddedOffset = blockInformation.getPaddedOffset(1);
			long[] paddedDimension = blockInformation.getPaddedDimension(1);

			// Set up random access for datasets
			RandomAccess<UnsignedByteType> volumeAveragedSheetnessRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(
					n5Path, volumeAveragedSheetnessDatasetName, paddedOffset, paddedDimension);
			RandomAccess<UnsignedLongType> contactSitesRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(n5Path,
					contactSiteName, paddedOffset, paddedDimension);
			RandomAccess<UnsignedLongType> referenceOrganelleCBRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(
					n5Path, referenceOrganelleName + "_contact_boundary_temp_to_delete", paddedOffset, paddedDimension);

			// Build histogram
			SheetnessMaps sheetnessMaps = buildSheetnessMaps(paddedOffset, paddedDimension, dimensions,
					volumeAveragedSheetnessRA, contactSitesRA, referenceOrganelleCBRA, voxelFaceArea);
			return sheetnessMaps;
		});

		// Collect histograms
		SheetnessMaps sheetnessMaps = javaRDDSheetnessMaps.reduce((a, b) -> {
			a.merge(b);
			return a;
		});

		return sheetnessMaps;
	}

	@SuppressWarnings("serial")
	static class SheetnessMaps implements Serializable {
		public Map<Integer, Double> sheetnessAndSurfaceAreaHistogram;
		public Map<Long, List<Integer>> organelleSheetnessMap;
		public Map<Long, List<Integer>> contactSiteSheetnessMap;

		SheetnessMaps() {
			sheetnessAndSurfaceAreaHistogram = new HashMap<Integer, Double>();
			organelleSheetnessMap = new HashMap<Long, List<Integer>>();
			contactSiteSheetnessMap = new HashMap<Long, List<Integer>>();
		}

		SheetnessMaps(Map<Integer, Double> sheetnessAndSurfaceAreaHistogram,
				Map<Long, List<Integer>> organelleSheetnessMap, Map<Long, List<Integer>> contactSiteSheetnessMap) {
			this.sheetnessAndSurfaceAreaHistogram = sheetnessAndSurfaceAreaHistogram;
			this.organelleSheetnessMap = organelleSheetnessMap;
			this.contactSiteSheetnessMap = contactSiteSheetnessMap;
		}

		public void merge(SheetnessMaps newSheetnessMaps) {
			for (Entry<Integer, Double> entry : newSheetnessMaps.sheetnessAndSurfaceAreaHistogram.entrySet())
				sheetnessAndSurfaceAreaHistogram.put(entry.getKey(),
						sheetnessAndSurfaceAreaHistogram.getOrDefault(entry.getKey(), 0.0) + entry.getValue());

			for (Entry<Long, List<Integer>> entry : newSheetnessMaps.organelleSheetnessMap.entrySet()) {
				Long organelleID = entry.getKey();
				List<Integer> newSumAndCount = entry.getValue();

				List<Integer> currentSumAndCount = organelleSheetnessMap.getOrDefault(organelleID, Arrays.asList(0, 0));
				currentSumAndCount.set(0, currentSumAndCount.get(0) + newSumAndCount.get(0));
				currentSumAndCount.set(1, currentSumAndCount.get(1) + newSumAndCount.get(1));
				organelleSheetnessMap.put(organelleID, currentSumAndCount);
			}

			for (Entry<Long, List<Integer>> entry : newSheetnessMaps.contactSiteSheetnessMap.entrySet()) {
				Long contactSiteID = entry.getKey();
				List<Integer> newSumAndCount = entry.getValue();

				List<Integer> currentSumAndCount = contactSiteSheetnessMap.getOrDefault(contactSiteID,
						Arrays.asList(0, 0));
				currentSumAndCount.set(0, currentSumAndCount.get(0) + newSumAndCount.get(0));
				currentSumAndCount.set(1, currentSumAndCount.get(1) + newSumAndCount.get(1));
				contactSiteSheetnessMap.put(contactSiteID, currentSumAndCount);
			}
		}
	}

	/**
	 * Loops over voxels to build up a histogram of the sheetness of the volume
	 * averaged sheetness at contact sites
	 * 
	 * @param paddedDimension           Padded dimensions of block
	 * @param volumeAveragedSheetnessRA Random access for volume averaged sheetness
	 *                                  dataset
	 * @param contactSitesRA            Random access for contact site dataset
	 * @param referenceOrganelleCBRA	Reference organelle contact boundary random access
	 * @param voxelFaceArea             Surface area of voxel face
	 * @return Map containing the histogram data
	 */
	public static <T extends IntegerType<T>> SheetnessMaps buildSheetnessMaps(long[] paddedOffset, long[] paddedDimension, long[] dimensions,
			RandomAccess<UnsignedByteType> volumeAveragedSheetnessRA, RandomAccess<T> contactSitesRA,
			RandomAccess<T> referenceOrganelleCBRA, double voxelFaceArea) {
		Map<Integer, Double> sheetnessAndSurfaceAreaHistogram = new HashMap<Integer, Double>();
		Map<Long, List<Integer>> organelleSheetnessMap = new HashMap<Long, List<Integer>>();
		Map<Long, List<Integer>> contactSiteSheetnessMap = new HashMap<Long, List<Integer>>();

		for (long x = 1; x < paddedDimension[0] - 1; x++) {
			for (long y = 1; y < paddedDimension[1] - 1; y++) {
				for (long z = 1; z < paddedDimension[2] - 1; z++) {
					long[] pos = new long[] { x, y, z };
					volumeAveragedSheetnessRA.setPosition(pos);
					contactSitesRA.setPosition(pos);
					int sheetnessMeasureBin = volumeAveragedSheetnessRA.get().get();
					long contactSiteID = contactSitesRA.get().getIntegerLong();

					if (sheetnessMeasureBin > 0 && contactSiteID > 0) {// Then is on surface and contact site
						int faces = ProcessingHelper.getSurfaceAreaContributionOfVoxelInFaces(volumeAveragedSheetnessRA,
								paddedOffset, dimensions);
						if (faces > 0) {
							sheetnessAndSurfaceAreaHistogram.put(sheetnessMeasureBin,
									sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessMeasureBin, 0.0)
											+ faces * voxelFaceArea);
						}

						referenceOrganelleCBRA.setPosition(pos);
						long organelleID = referenceOrganelleCBRA.get().getIntegerLong();

						List<Integer> sumAndCount = organelleSheetnessMap.getOrDefault(organelleID,
								Arrays.asList(0, 0));
						sumAndCount.set(0, sumAndCount.get(0) + sheetnessMeasureBin);
						sumAndCount.set(1, sumAndCount.get(1) + 1);
						organelleSheetnessMap.put(organelleID, sumAndCount);

						sumAndCount = contactSiteSheetnessMap.getOrDefault(contactSiteID, Arrays.asList(0, 0));
						sumAndCount.set(0, sumAndCount.get(0) + sheetnessMeasureBin);
						sumAndCount.set(1, sumAndCount.get(1) + 1);
						contactSiteSheetnessMap.put(contactSiteID, sumAndCount);
					}
				}
			}
		}
		return new SheetnessMaps(sheetnessAndSurfaceAreaHistogram, organelleSheetnessMap, contactSiteSheetnessMap);

	}

	/**
	 * 
	 * @param sheetnessAndSurfaceAreaHistogram Histogram of sheetness vs surface
	 *                                         area as map
	 * @param outputDirectory                  Directory to write results to
	 * @param filePrefix                       Prefix for file name
	 * @throws IOException
	 */
	public static void writeData(SheetnessMaps sheetnessMaps, String outputDirectory, String filePrefix,
			String referenceOrganelleName) throws IOException {
		if (!new File(outputDirectory).exists()) {
			new File(outputDirectory).mkdirs();
		}

		Map<Integer, Double> sheetnessAndSurfaceAreaHistogram = sheetnessMaps.sheetnessAndSurfaceAreaHistogram;
		FileWriter sheetnessVolumeAndAreaHistogramFW = new FileWriter(
				outputDirectory + "/" + filePrefix + "_sheetnessSurfaceAreaHistograms.csv");
		sheetnessVolumeAndAreaHistogramFW.append("Sheetness,Surface Area(nm^2)\n");

		for (int sheetnessBin = 1; sheetnessBin < 256; sheetnessBin++) {
			double surfaceArea = sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessBin, 0.0);
			String sheetnessBinString = Double.toString((sheetnessBin - 1) / 255.0 + 0.5 / 255.0);
			sheetnessVolumeAndAreaHistogramFW.append(sheetnessBinString + "," + Double.toString(surfaceArea) + "\n");
		}
		sheetnessVolumeAndAreaHistogramFW.flush();
		sheetnessVolumeAndAreaHistogramFW.close();

		Map<Long, List<Integer>> organelleSheetnessMap = sheetnessMaps.organelleSheetnessMap;
		FileWriter organelleSheetnessWriter = new FileWriter(
				outputDirectory + "/" + filePrefix + "__" + referenceOrganelleName + "_sheetness.csv");
		organelleSheetnessWriter.append("Object ID,Sheetness\n");

		for (Entry<Long, List<Integer>> entry : organelleSheetnessMap.entrySet()) {
			Long organelleID = entry.getKey();
			List<Integer> sumAndCount = entry.getValue();
			double averageSheetnessBin = 1.0 * sumAndCount.get(0) / sumAndCount.get(1);
			String averageSheetnessString = Double.toString((averageSheetnessBin - 1) / 255.0 + 0.5 / 255.0);
			organelleSheetnessWriter.append(Long.toString(organelleID) + "," + averageSheetnessString + "\n");
		}

		organelleSheetnessWriter.flush();
		organelleSheetnessWriter.close();

		Map<Long, List<Integer>> contactSiteSheetnessMap = sheetnessMaps.contactSiteSheetnessMap;
		FileWriter contactSiteSheetnessWriter = new FileWriter(outputDirectory + "/" + filePrefix + "_sheetness.csv");
		contactSiteSheetnessWriter.append("Object ID,Sheetness\n");

		for (Entry<Long, List<Integer>> entry : contactSiteSheetnessMap.entrySet()) {
			Long contactSiteID = entry.getKey();
			List<Integer> sumAndCount = entry.getValue();
			double averageSheetnessBin = 1.0 * sumAndCount.get(0) / sumAndCount.get(1);
			String averageSheetnessString = Double.toString((averageSheetnessBin - 1) / 255.0 + 0.5 / 255.0);
			contactSiteSheetnessWriter.append(Long.toString(contactSiteID) + "," + averageSheetnessString + "\n");
		}

		contactSiteSheetnessWriter.flush();
		contactSiteSheetnessWriter.close();

	}

	public static void setupSparkAndCalculateSheetnessOfContactSites(String inputN5Path, String inputN5SheetnessDatasetName, String outputDirectory, String inputN5ContactSiteDatasetName) throws IOException {
	 // Get all organelles
	 		String[] contactSiteDatasets = { "" };
	 		if (inputN5ContactSiteDatasetName != null) {
	 			contactSiteDatasets = inputN5ContactSiteDatasetName.split(",");
	 		} else {
	 			File file = new File(inputN5Path);
	 			contactSiteDatasets = file.list(new FilenameFilter() {
	 				@Override
	 				public boolean accept(File current, String name) {
	 					return new File(current, name).isDirectory();
	 				}
	 			});
	 		}

	 		final SparkConf conf = new SparkConf().setAppName("SparkCalculateSheetnessOfContactSites");

	 		// Create block information list
	 		List<BlockInformation> blockInformationList = BlockInformation
	 				.buildBlockInformationList(inputN5Path, inputN5SheetnessDatasetName);
	 		for (String contactSiteDataset : contactSiteDatasets) {
	 			String[] contactSiteDatasetSplit = contactSiteDataset.split("_to_");
	 			String referenceOrganelleName = contactSiteDatasetSplit[contactSiteDatasetSplit.length - 1];
	 			referenceOrganelleName = referenceOrganelleName.substring(0, referenceOrganelleName.length() - 3); 
	 			// -3 to get rid of _cc
	 			JavaSparkContext sc = new JavaSparkContext(conf);
	 			SheetnessMaps sheetnessMaps = getContactSiteAndOrganelleSheetness(sc, inputN5Path,
	 					inputN5SheetnessDatasetName, contactSiteDataset, referenceOrganelleName,
	 					blockInformationList);
	 			writeData(sheetnessMaps, outputDirectory, contactSiteDataset, referenceOrganelleName);
	 			sc.close();
	 		}
	}

	/**
	 * Take input args and perform analysis.
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
		String inputN5SheetnessDatasetName = options.getInputN5SheetnessDatasetName();
		String outputDirectory = options.getOutputDirectory();
		String inputN5ContactSiteDatasetName =  options.getInputN5ContactSiteDatasetName();

		setupSparkAndCalculateSheetnessOfContactSites(inputN5Path, inputN5SheetnessDatasetName, outputDirectory, inputN5ContactSiteDatasetName);

	}

}
