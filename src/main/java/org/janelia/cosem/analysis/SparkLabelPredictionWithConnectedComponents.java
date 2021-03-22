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
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
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
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;


/**
 * Threshold a prediction but label it using another segmented volume's ids.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkLabelPredictionWithConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--predictionN5Path", required = true, usage = "Path to prediction N5")
		private String predictionN5Path = null;

		@Option(name = "--predictionDatasetName", required = false, usage = "Name of prediction dataset")
		private String predictionDatasetName = null;
		
		@Option(name = "--connectedComponentsN5Path", required = false, usage = "Path to connected components dataset used for relabeling")
		private String connectedComponentsN5Path = null;
		
		@Option(name = "--connectedComponentsDatasetName", required = true, usage = "Name of connected components dataset")
		private String connectedComponentsDatasetName = null;
		
		@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
		private String outputN5Path = null;
		
		@Option(name = "--thresholdDistance", required = false, usage = "Distance for thresholding (positive inside, negative outside) (nm)")
		private double thresholdDistance = 0;
		
		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);

				if (outputN5Path == null)
					outputN5Path = connectedComponentsN5Path;

				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getPredictionN5Path() {
			return predictionN5Path;
		}

		public String getPredictionDatasetName() {
			return predictionDatasetName;
		}

		public String getConnectedComponentsN5Path() {
			return connectedComponentsN5Path;
		}
		
		public String getConnectedComponentsDatasetName() {
			return connectedComponentsDatasetName;
		}
		
		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public double getThresholdDistance() {
			return thresholdDistance;
		}
		
	}

	
	/**
	 * Method that relabels predictions above a certain threshold with the connected component object ID they are within.
	 * 
	 * @param sc								Spark context
	 * @param predictionN5Path					N5 path to predictions
	 * @param predictionDatasetName				Name of predictions
	 * @param connectedComponentsN5Path			N5 path to connected components
	 * @param connectedComponentsDatasetName	Name of connected components
	 * @param outputN5Path						N5 path to output
	 * @param thresholdIntensityCutoff			Threshold intensity cutoff
	 * @param blockInformationList				List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final void labelPredictionsWithConnectedComponents(
			final JavaSparkContext sc, final String predictionN5Path,final String predictionDatasetName, final String connectedComponentsN5Path,
			final String connectedComponentsDatasetName, final String outputN5Path, double thresholdIntensityCutoff, List<BlockInformation> blockInformationList) throws IOException {

		// Get attributes of input data sets.
		final N5Reader predictionN5Reader = new N5FSReader(predictionN5Path);
		final DatasetAttributes attributes = predictionN5Reader.getDatasetAttributes(predictionDatasetName);
		final int[] blockSize = attributes.getBlockSize();
		final long[] outputDimensions = attributes.getDimensions();
		final double [] pixelResolution = IOHelper.getResolution(predictionN5Reader, predictionDatasetName);
				
		// Create output dataset
		final String outputN5DatasetName = predictionDatasetName+"_labeledWith_"+connectedComponentsDatasetName;
		final N5Writer n5Writer = new N5FSWriter(outputN5Path);
		n5Writer.createGroup(outputN5DatasetName);
		n5Writer.createDataset(outputN5DatasetName, outputDimensions, blockSize,
				org.janelia.saalfeldlab.n5.DataType.UINT64, attributes.getCompression());
		n5Writer.setAttribute(outputN5DatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
		
		// Do the labeling, parallelized over blocks
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] dimension = gridBlock[1];
			
			final N5Reader predictionN5ReaderLocal = new N5FSReader(predictionN5Path);
			IntervalView<UnsignedByteType> prediction = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(predictionN5ReaderLocal, predictionDatasetName)
					),offset, dimension);
			
					
			final N5Reader connectedComponentsReaderLocal = new N5FSReader(connectedComponentsN5Path);
			IntervalView<UnsignedLongType> connectedComponents = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<UnsignedLongType>) N5Utils.open(connectedComponentsReaderLocal, connectedComponentsDatasetName)
						),offset, dimension);
								
			Cursor<UnsignedByteType> predictionCursor = prediction.cursor();
			Cursor<UnsignedLongType> connectedComponentsCursor = connectedComponents.cursor();
			while(predictionCursor.hasNext()) {
				predictionCursor.next();
				connectedComponentsCursor.next();
				long objectID = connectedComponentsCursor.get().get();
				int predictionValue = predictionCursor.get().get();
				if(objectID>0 && predictionValue<thresholdIntensityCutoff) {
						connectedComponentsCursor.get().set(0);
				}
			}
			
			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = new N5FSWriter(outputN5Path);
			N5Utils.saveBlock(connectedComponents, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

		});
	}
	
	/**
	 * Take input arguments and label prediction with connected components
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		double thresholdIntensityCutoff = 	128 * Math.tanh(options.getThresholdDistance() / 50) + 127;

		final SparkConf conf = new SparkConf().setAppName("SparkLabelPredictionWithConnectedComponents");
		
		//Create block information list
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getPredictionN5Path(), options.getPredictionDatasetName());
	
		//Run connected components
		JavaSparkContext sc = new JavaSparkContext(conf);
		labelPredictionsWithConnectedComponents(sc, options.getPredictionN5Path(), options.getPredictionDatasetName(), options.getConnectedComponentsN5Path(), options.getConnectedComponentsDatasetName(), options.getOutputN5Path(), thresholdIntensityCutoff, blockInformationList);	
		sc.close();
		
	}
}

