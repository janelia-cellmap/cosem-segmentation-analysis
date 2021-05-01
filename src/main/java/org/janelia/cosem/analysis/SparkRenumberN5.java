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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkRenumberN5 {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
	private String inputN5Path = null;

	@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
	private String outputN5Path = null;

	@Option(name = "--inputN5DatasetName", required = true, usage = "N5 dataset, e.g. /mito")
	private String inputN5DatasetName = null;

	@Option(name = "--inputDirectory", required = true, usage = "directory containing renumbering data")
	private String inputDirectory = null;
	
	@Option(name = "--renumberingCSV", required = false, usage = "renumbering data")
	private String renumberingCSV = null;

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

	public String getInputDirectory() {
	    return inputDirectory;
	}

	public String getInputN5DatasetName() {
	    return inputN5DatasetName;
	}

	public String getOutputN5Path() {
	    return outputN5Path;
	}
	
	public String getRenumberingCSV() {
	    if (renumberingCSV == null)
		return getInputN5DatasetName()+"_renumbered";
	    return renumberingCSV;
	}

    }

    public static DataType getDataType(Long numberOfObjects) {
	if (numberOfObjects <= Math.pow(2, 8)) {
	    return DataType.UINT8;
	} else if (numberOfObjects <= Math.pow(2, 16)) {
	    return DataType.UINT16;
	} else if (numberOfObjects <= Math.pow(2, 32)) {
	    return DataType.UINT32;
	} else {
	    return DataType.UINT64;
	}
    }
    public static final <T extends IntegerType<T> & NativeType<T>> void renumberN5(final JavaSparkContext sc,
	    final String n5Path, final String datasetName, final String n5OutputPath,
	    Broadcast<Map<Long, Long>> broadcastedRenumberingMap, final List<BlockInformation> blockInformationList) throws IOException
    {
	renumberN5(sc, n5Path, datasetName, n5OutputPath, broadcastedRenumberingMap, blockInformationList, false);
    }
    
    public static final <T extends IntegerType<T> & NativeType<T>> void renumberN5(final JavaSparkContext sc,
	    final String n5Path, final String datasetName, final String n5OutputPath,
	    Broadcast<Map<Long, Long>> broadcastedRenumberingMap, final List<BlockInformation> blockInformationList, boolean addSuffixRegardless)
	    throws IOException {

	Long numberOfObjects = (long) (broadcastedRenumberingMap.value().size() + 1);// add 1 for zero
	DataType dataType = getDataType(numberOfObjects);
	String tempDatasetName = datasetName;
	if(n5OutputPath==n5Path || addSuffixRegardless) {
	    tempDatasetName+="_renumbered";
	}
	final String outputDatasetName = tempDatasetName;
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, datasetName, n5OutputPath, outputDatasetName, dataType);

	/*
	 * grid block size for parallelization to minimize double loading of blocks
	 */
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	rdd.foreach(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];

	    Cursor<T> sourceCursor = ProcessingHelper.getOffsetIntervalExtendZeroC(n5Path, datasetName,
		    offset, dimension);
	    RandomAccessibleInterval<T> output = ProcessingHelper.getZerosIntegerImageRAI(dimension, dataType);
	    RandomAccess<T> outputRA = output.randomAccess();

	    Map<Long, Long> renumberingMap = broadcastedRenumberingMap.value();
	    long[] pos;
	    while (sourceCursor.hasNext()) {
		sourceCursor.next();
		long objectID = sourceCursor.get().getIntegerLong();
		if (objectID > 0) {
		    Long renumberedID = renumberingMap.get(objectID);
		    pos = new long[] { sourceCursor.getLongPosition(0), sourceCursor.getLongPosition(1),
			    sourceCursor.getLongPosition(2) };
		    outputRA.setPosition(pos);
		    outputRA.get().setInteger(renumberedID);
		}
	    }
	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
	});
	
    }

    public static final Map<Long, Long> readInData(String inputDirectory, String renumberingName)
	    throws NumberFormatException, IOException {
	Map<Long, Long> renumberingMap = new HashMap<Long, Long>();

	boolean firstLine = true;
	String row;
	BufferedReader csvReader = new BufferedReader(
		new FileReader(inputDirectory + "/" + renumberingName));
	while ((row = csvReader.readLine()) != null) {
	    String[] data = row.split(",");
	    if (!firstLine) {
		long originalID = Long.parseLong(data[0]);
		long renumberedID = Long.parseLong(data[1]);
		renumberingMap.put(originalID, renumberedID);
	    }
	    firstLine = false;
	}
	csvReader.close();
	return renumberingMap;
    }
    public static void setupSparkAndRenumberN5(String inputDirectory, String inputN5DatasetName, String inputN5Path,
	    String outputN5Path, String renumberingCSV) throws Exception{
	setupSparkAndRenumberN5(inputDirectory, inputN5DatasetName, inputN5Path, outputN5Path, renumberingCSV, false);
    }
    public static void setupSparkAndRenumberN5(String inputDirectory, String inputN5DatasetName, String inputN5Path,
	    String outputN5Path, String renumberingCSV, boolean addSuffixRegardless) throws Exception {
	final SparkConf conf = new SparkConf().setAppName("SparkRenumberN5");	

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
	
	String [] renumberingCSVList = renumberingCSV.split(",");
	if(renumberingCSVList.length != organelles.length) {
	    throw new Exception("Organelle list and renumbering csv list are of different lengths"); 
	}
	for(int i=0; i < organelles.length; i++) {
	    Map<Long, Long> renumberingMap = readInData(inputDirectory, renumberingCSVList[i]+"_renumbering.csv");

	    // Create block information list
	    List<BlockInformation> blockInformationList = BlockInformation
		    .buildBlockInformationList(inputN5Path, organelles[i]);
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    Broadcast<Map<Long, Long>> broadcastedRenumberingMap = sc.broadcast(renumberingMap);

	    renumberN5(sc, inputN5Path, organelles[i], outputN5Path,
		    broadcastedRenumberingMap, blockInformationList, addSuffixRegardless);
	
	    sc.close();
	}
    }
    
    public static final void main(final String... args) throws Exception {

	final Options options = new Options(args);

	if (!options.parsedSuccessfully)
	    return;
	setupSparkAndRenumberN5(options.getInputDirectory(),
	options.getInputN5DatasetName(),
	options.getInputN5Path(),
	options.getOutputN5Path(),
	options.getRenumberingCSV());
	

    }

    
}
