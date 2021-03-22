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

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

/**
 * Mask one dataset with another
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkApplyMaskToCleanData {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--datasetToMaskN5Path", required = true, usage = "N5 path with dataset to mask out")
	private String datasetToMaskN5Path = null;

	@Option(name = "--datasetToUseAsMaskN5Path", required = true, usage = "N5 path to dataset to use as mask")
	private String datasetToUseAsMaskN5Path = null;

	@Option(name = "--outputN5Path", required = true, usage = "Output N5 path")
	private String outputN5Path = null;

	@Option(name = "--datasetNameToMask", required = false, usage = "Dataset name to mask out")
	private String datasetNameToMask = null;

	@Option(name = "--datasetNameToUseAsMask", required = false, usage = "Dataset name to use as mask")
	private String datasetNameToUseAsMask = null;

	@Option(name = "--keepWithinMask", required = false, usage = "If true, keep data within the mask region; otherwise mask out data in mask")
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

	public boolean getKeepWithinMask() {
	    return keepWithinMask;
	}

    }

    /**
     * Mask data within a block
     * 
     * @param <T>
     * @param maskDataRA 	Random access for mask
     * @param dataToMaskRA	Random access for data to mask
     * @param dimension		Dimensions
     * @param keepWithinMask	Keep within mask
     */
    public static final <T extends IntegerType<T> & NativeType<T>> void maskWithinBlock(RandomAccess<T> maskDataRA,
	    RandomAccess<T> dataToMaskRA, long[] dimension, boolean keepWithinMask) {
	for (int x = 0; x < dimension[0]; x++) {
	    for (int y = 0; y < dimension[1]; y++) {
		for (int z = 0; z < dimension[2]; z++) {
		    long[] pos = new long[] { x, y, z };
		    maskDataRA.setPosition(pos);
		    if (maskDataRA.get().getIntegerLong() > 0) {

			if (!keepWithinMask) {// then use mask as regions to set to 0
			    dataToMaskRA.setPosition(pos);
			    dataToMaskRA.get().setZero();
			}

		    } else { // set region outside mask to 0

			if (keepWithinMask) {
			    dataToMaskRA.setPosition(pos);
			    dataToMaskRA.get().setZero();
			}

		    }

		}
	    }
	}
    }

    /**
     * Use as segmented dataset to mask out a prediction dataset, where the mask can
     * either be inclusive or exclusive.
     * 
     * @param sc                       Spark context
     * @param datasetToMaskN5Path      N5 path for dataset that will be masked
     * @param datasetNameToMask        Dataset name that will be masked
     * @param datasetToUseAsMaskN5Path N5 path for mask dataset
     * @param datasetNameToUseAsMask   Dataset name to use as mask
     * @param n5OutputPath             Output N5 path
     * @param keepWithinMask           If true, keep data that is within mask; else
     *                                 exclude data within mask
     * @param blockInformationList     List of block information
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> void applyMask(final JavaSparkContext sc,
	    final String datasetToMaskN5Path, final String datasetNameToMask, final String datasetToUseAsMaskN5Path,
	    final String datasetNameToUseAsMask, final String n5OutputPath, final boolean keepWithinMask,
	    final List<BlockInformation> blockInformationList) throws IOException {

	String maskedDatasetName = datasetNameToMask + "_maskedWith_" + datasetNameToUseAsMask;
	ProcessingHelper.createDatasetUsingTemplateDataset(datasetToMaskN5Path, datasetNameToMask, n5OutputPath,
		maskedDatasetName);

	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	rdd.foreach(blockInformation -> {
	    final long[] offset = blockInformation.gridBlock[0];
	    final long[] dimension = blockInformation.gridBlock[1];

	    final RandomAccessibleInterval<T> dataToMask = ProcessingHelper
		    .getOffsetIntervalExtendZeroRAI(datasetToMaskN5Path, datasetNameToMask, offset, dimension);
	    RandomAccess<T> dataToMaskRA = dataToMask.randomAccess();
	    RandomAccess<T> maskDataRA = ProcessingHelper
		    .getOffsetIntervalExtendZeroRA(datasetToUseAsMaskN5Path, datasetNameToUseAsMask, offset, dimension);

	    maskWithinBlock(maskDataRA, dataToMaskRA, dimension, keepWithinMask);

	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    
	    N5Utils.saveBlock(dataToMask, n5BlockWriter, maskedDatasetName,
		blockInformation.gridBlock[2]);
	  
	});
    }
    
    public static void setupSparkAndApplyMaskToCleanData(String datasetToUseAsMaskN5Path,
	    String datasetNameToUseAsMask, String datasetToMaskN5Path, String organellesToMaskString, String outputN5Path,
	    boolean keepWithinMask) throws IOException {
	
	final SparkConf conf = new SparkConf().setAppName("SparkApplyMaskToCleanData");
	
	String[] organellesToMask = organellesToMaskString.split(",");

	for (String mask : datasetNameToUseAsMask.split(",")) {
	    for (String organelleToMask : organellesToMask) {

		List<BlockInformation> blockInformationList = BlockInformation
			.buildBlockInformationList(datasetToMaskN5Path, organelleToMask);
		JavaSparkContext sc = new JavaSparkContext(conf);
		applyMask(sc, datasetToMaskN5Path, organelleToMask, datasetToUseAsMaskN5Path, mask,
			outputN5Path, keepWithinMask, blockInformationList);

		sc.close();
	    }
	}
	
    }

    /**
     * Actually do the masking of one dataset with another
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

	String datasetToUseAsMaskN5Path = options.getDatasetToUseAsMaskN5Path();
	String datasetNameToUseAsMask = options.getDatasetNameToUseAsMask();
	String datasetToMaskN5Path = options.getDatasetToMaskN5Path();
	String organellesToMaskString = options.getDatasetNameToMask();
	String outputN5Path = options.getOutputN5Path();
	boolean keepWithinMask = options.getKeepWithinMask();

	setupSparkAndApplyMaskToCleanData(datasetToUseAsMaskN5Path, datasetNameToUseAsMask, datasetToMaskN5Path, outputN5Path, organellesToMaskString,keepWithinMask);
    }

}
