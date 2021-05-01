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

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.Views;

/**
 * Expand a given dataset to mask out predictions for improving downstream analysis.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCreateValidationDatasets {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--n5PathValidationData", required = true, usage = "N5 path to groundtruth")
		private String n5PathValidationData = null;
		
		@Option(name = "--n5PathRawPredictions", required = true, usage = "N5 path to Raw predictions")
		private String n5PathRawPredictions = null;
		
		@Option(name = "--n5PathRefinedPredictions", required = true, usage = "N5 path to refined predictions, aka refined segmentations")
		private String n5PathRefinedPredictions = null;

		@Option(name = "--outputPath", required = true, usage = "Output N5 path")
		private String outputPath = null;
		
		@Option(name = "--doMicrotubules", required = false, usage = "Whether or not to do microtubules")
		private Boolean doMicrotubules = false;


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

		public String getN5PathValidationData() {
			return n5PathValidationData;
		}
		
		public String getN5PathRawPredictions() {
			return n5PathRawPredictions;
		}
		
		public String getN5PathRefinedPredictions() {
			return n5PathRefinedPredictions;
		}

		public String getOutputPath() {
			return outputPath;
		}
		
		public boolean getDoMicrotubules() {
			return doMicrotubules;
		}

	}

	@SuppressWarnings("unchecked")
	public static final <T extends IntegerType<T>> void createValidationDatasets(
			final JavaSparkContext sc,
			final String n5PathTrainingData,
			final String n5PathRawPredictions,
			final String n5PathRefinedPredictions,
			final String outputPath,
			final boolean doMicrotubules) throws IOException {

		Map<String, List<Integer>> organelleToIDs = new HashMap<String,List<Integer>>();
		organelleToIDs.put("plasma_membrane",Arrays.asList(2));
		organelleToIDs.put("mito_membrane",Arrays.asList(3));
		organelleToIDs.put("mito",Arrays.asList(3,4,5));
		organelleToIDs.put("golgi",Arrays.asList(6,7));
		organelleToIDs.put("vesicle", Arrays.asList(8, 9));
		organelleToIDs.put("MVB", Arrays.asList(10, 11));
		organelleToIDs.put("er",Arrays.asList(16, 17, 18, 19, 20, 21, 22, 23));
		organelleToIDs.put("nucleus",Arrays.asList(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 37));
		organelleToIDs.put("microtubules_out",Arrays.asList(30));
		organelleToIDs.put("microtubules",Arrays.asList(30, 36));
		organelleToIDs.put("ribosomes",Arrays.asList(34));
		 
		final N5Reader n5ReaderTraining = new N5FSReader(n5PathTrainingData);
		List<String> cropNames = Arrays.asList("cropRight","cropLeft","cropUp","cropDown","cropFront","cropBack","whole");

		for(int i=0; i<cropNames.size(); i++) {

			String n5OutputTraining = outputPath+"/validation/"+cropNames.get(i)+".n5";
			final N5Writer n5WriterTraining = new N5FSWriter(n5OutputTraining);
			
			String n5OutputRefinedPredictions = outputPath+"/refinedPredictions/"+cropNames.get(i)+".n5";
			final N5Writer n5WriterRefinedPredictions = new N5FSWriter(n5OutputRefinedPredictions);
			
			String n5OutputRawPredictions = outputPath+"/rawPredictions/"+cropNames.get(i)+".n5";
			final N5Writer n5WriterRawPredictions = new N5FSWriter(n5OutputRawPredictions);
			double[] pixelResolutionTraining = null;
			int[] blockSizeTraining = null;
			int[] offsetTraining =null;
			long[] dimensionsTraining=null;
			for( Entry<String, List<Integer>> entry : organelleToIDs.entrySet()) {
				final String organelle = entry.getKey();
				List<Integer> ids = entry.getValue();
				
				String organellePathInN5 = organelle=="ribosomes" ? "/"+organelle : "/all";
			
				DatasetAttributes attributesTraining = n5ReaderTraining.getDatasetAttributes(organellePathInN5);
				dimensionsTraining = attributesTraining.getDimensions();
				blockSizeTraining = attributesTraining.getBlockSize();
				
				pixelResolutionTraining = IOHelper.getResolution(n5ReaderTraining, organellePathInN5);
				offsetTraining = IOHelper.getOffset(n5ReaderTraining, organellePathInN5);

				dimensionsTraining[0]=(long) (Math.floor(dimensionsTraining[0]/500)*500); //round to nearest 500//hardcode because one of them is 1004x1002x500
				dimensionsTraining[1]=(long) (Math.floor(dimensionsTraining[1]/500)*500); //round to nearest 500//hardcode because one of them is 1004x1002x500
				dimensionsTraining[2]=(long) (Math.floor(dimensionsTraining[2]/500)*500); //round to nearest 500//hardcode because one of them is 1004x1002x500
				
				final int [] offsetTrainingInVoxels = new int [] {0,0,0};
				if(i<6) {
					int dim = (int) Math.floor(i/2);
					dimensionsTraining[dim]/=2;
					if(i%2==0) {
						offsetTraining[dim]+=dimensionsTraining[dim]*pixelResolutionTraining[dim];
						offsetTrainingInVoxels[dim]+=dimensionsTraining[dim];//origin 0
					}
				}
				
				final String organelleRibosomeAdjustedNameTraining = organelle=="ribosomes"? "ribosomes_centers":organelle;
				
				n5WriterTraining.createDataset(
						organelleRibosomeAdjustedNameTraining,
						dimensionsTraining,
						blockSizeTraining,
						DataType.UINT8,
						new GzipCompression());
				n5WriterTraining.setAttribute(organelleRibosomeAdjustedNameTraining, "offset", offsetTraining);
				n5WriterTraining.setAttribute(organelleRibosomeAdjustedNameTraining, "pixelResolution",new IOHelper.PixelResolution(pixelResolutionTraining));
		
				List<BlockInformation> blockInformationListTraining = BlockInformation.buildBlockInformationList(dimensionsTraining, blockSizeTraining);
				JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationListTraining);
				rdd.foreach(blockInformation -> {
					long offset [] = new long [] {blockInformation.gridBlock[0][0]+offsetTrainingInVoxels[0],
							blockInformation.gridBlock[0][1]+offsetTrainingInVoxels[1],
							blockInformation.gridBlock[0][2]+offsetTrainingInVoxels[2]
					};
					final long [] dimension = blockInformation.gridBlock[1];
					final N5Reader n5ReaderLocal = new N5FSReader(n5PathTrainingData);
					
					final RandomAccessibleInterval<UnsignedLongType> source = Views.offsetInterval((RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5ReaderLocal, organellePathInN5), offset, dimension);
					final RandomAccessibleInterval<UnsignedByteType> sourceConverted = Converters.convert(
							source,
							(a, b) -> {
								Integer id = (int) a.get();
								b.set(ids.contains(id) ? 255 : 0 );
							},
							new UnsignedByteType());
					final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputTraining);
					
					N5Utils.saveBlock(sourceConverted, n5BlockWriter, organelleRibosomeAdjustedNameTraining, blockInformation.gridBlock[2]);
							
				});
			}
			
			Set<String> organelleSet = new HashSet<String>();
			organelleSet.addAll(organelleToIDs.keySet());
			//refined and raw predictions
			for(String n5PredictionsPath : Arrays.asList(n5PathRawPredictions, n5PathRefinedPredictions)) {
				organelleSet.remove("microtubules_out");
				organelleSet.remove("microtubules");
				if(doMicrotubules) {
					organelleSet.add("microtubules");
				}
				if(n5PredictionsPath==n5PathRefinedPredictions) {
					organelleSet.add("er_reconstructed");
					organelleSet.add("er_maskedWith_nucleus_expanded");
					organelleSet.add("mito_maskedWith_er");
					organelleSet.add("er_maskedWith_nucleus_expanded_maskedWith_ribosomes");
					organelleSet.add("er_reconstructed_maskedWith_nucleus_expanded");
					organelleSet.add("mito_maskedWith_er_reconstructed");
					organelleSet.add("er_reconstructed_maskedWith_nucleus_expanded_maskedWith_ribosomes");
					if(doMicrotubules) {
						organelleSet.add("er_maskedWith_microtubules");
						organelleSet.add("er_maskedWith_nucleus_expanded_maskedWith_microtubules");
						organelleSet.add("er_reconstructed_maskedWith_microtubules");
						organelleSet.add("er_reconstructed_maskedWith_nucleus_expanded_maskedWith_microtubules");
						organelleSet.add("nucleus_maskedWith_microtubules");
						organelleSet.add("mito_maskedWith_microtubules");
						organelleSet.add("MVB_maskedWith_microtubules");
						organelleSet.add("vesicle_maskedWith_microtubules");
						organelleSet.add("golgi_maskedWith_microtubules");
						organelleSet.add("plasma_membrane_maskedWith_microtubules");
					}
				}
				for(String organelle : organelleSet) {
	
					final String organelleRibosomeAdjustedName = (organelle=="ribosomes" && n5PredictionsPath==n5PathRefinedPredictions)? "ribosomes_centers":organelle;
	
					final N5Reader n5ReaderPredictions = new N5FSReader(n5PredictionsPath);		
					double[] pixelResolutionPredictions = IOHelper.getResolution(n5ReaderPredictions, organelleRibosomeAdjustedName);
					double resolutionRatio = pixelResolutionTraining[0]/pixelResolutionPredictions[0];
					long [] dimensionsPredictions = new long [] {(long) (dimensionsTraining[0]*resolutionRatio),(long) (dimensionsTraining[1]*resolutionRatio),(long) (dimensionsTraining[2]*resolutionRatio)};
					long [] offsetInVoxels = new long [] {(long) (offsetTraining[0]/pixelResolutionPredictions[0]),(long) (offsetTraining[1]/pixelResolutionPredictions[1]),(long) (offsetTraining[2]/pixelResolutionPredictions[2])};
					int[] blockSizeRefinedPredictions = blockSizeTraining;
					N5Writer n5WriterPredictions = n5PredictionsPath==n5PathRawPredictions ? n5WriterRawPredictions : n5WriterRefinedPredictions;
					n5WriterPredictions.createDataset(
							organelleRibosomeAdjustedName,
							dimensionsPredictions,
							blockSizeTraining,
							DataType.UINT8,
							new GzipCompression());
					n5WriterPredictions.setAttribute(organelleRibosomeAdjustedName, "offset", offsetTraining);
					n5WriterPredictions.setAttribute(organelleRibosomeAdjustedName, "pixelResolution", new IOHelper.PixelResolution(pixelResolutionPredictions));
			
					List<BlockInformation> blockInformationListRefinedPredictions = BlockInformation.buildBlockInformationList(dimensionsPredictions, blockSizeRefinedPredictions);
					JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationListRefinedPredictions);			
					rdd.foreach(blockInformation -> {
						final long [] offset= new long [] {blockInformation.gridBlock[0][0]+offsetInVoxels[0],
								blockInformation.gridBlock[0][1]+offsetInVoxels[1],
								blockInformation.gridBlock[0][2]+offsetInVoxels[2]
						};
						final long [] dimension = blockInformation.gridBlock[1];
						final N5Reader n5ReaderLocal = new N5FSReader(n5PredictionsPath);
						final long threshold = n5PredictionsPath==n5PathRawPredictions ? 127 : 1 ;
						final RandomAccessibleInterval<T> source = Views.offsetInterval((RandomAccessibleInterval<T>)N5Utils.open(n5ReaderLocal, organelleRibosomeAdjustedName), offset, dimension);
						final RandomAccessibleInterval<UnsignedByteType> sourceConverted = Converters.convert(
								source,
								(a, b) -> {
									b.set(a.getIntegerLong()>=threshold ? 255 : 0 );
								},
								new UnsignedByteType());
						final N5FSWriter n5BlockWriter = new N5FSWriter(n5PredictionsPath == n5PathRawPredictions ? n5OutputRawPredictions : n5OutputRefinedPredictions);
						N5Utils.saveBlock(sourceConverted, n5BlockWriter, organelleRibosomeAdjustedName, blockInformation.gridBlock[2]);
										
					});
				}
			}
		}
		
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
		
		SparkConf conf = new SparkConf().setAppName("SparkCreateValidationDatasets");
		JavaSparkContext sc = new JavaSparkContext(conf);
		createValidationDatasets(sc, options.getN5PathValidationData(), options.getN5PathRawPredictions(),options.getN5PathRefinedPredictions(), options.getOutputPath(), options.getDoMicrotubules());
		sc.close();
		
				
	}
}
