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
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Get renumbering so that can change bit depth and save memory
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkGetRenumbering {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
	private String inputN5Path = null;

	@Option(name = "--outputDirectory", required = true, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
	private String outputDirectory = null;

	@Option(name = "--inputN5DatasetName", required = true, usage = "N5 dataset, e.g. /mito")
	private String inputN5DatasetName = null;
	
	@Option(name = "--datasetToUseForRenumbering", required = false, usage = "Optional N5 dataset to use for renumbering")
	private String datasetToUseForRenumbering = null;
	
	@Option(name = "--pathToUseForRenumbering", required = false, usage = "Optional N5 path for renumbering")
	private String pathToUseForRenumbering = null;

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

	public String getInputN5Path() {
	    return inputN5Path;
	}

	public String getInputN5DatasetName() {
	    return inputN5DatasetName;
	}

	public String getOutputDirectory() {
	    return outputDirectory;
	}
	
	public String getDatasetToUseForRenumbering() {
	    return datasetToUseForRenumbering;
	}
	
	public String getPathToUseForRenumbering() {
	    return pathToUseForRenumbering;
	}

    }

    public static final <T extends IntegerType<T> & NativeType<T>> void getRenumberingUsingOtherDataset(final JavaSparkContext sc, final String inputN5Path, final String pathToUseForRenumbering, 
	    final String datasetToRenumber, final String datasetNumberingToUse, final String outputDirectory,
	    final List<BlockInformation> blockInformationList) throws Exception {
	
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<HashMap<Long, HashMap<Long, Long>> > objectIDToRenumberingObjectIDCountsRDD = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final N5Reader n5BlockReader = new N5FSReader(inputN5Path);
	    IntervalView<T> datasetToRenumberView = Views.offsetInterval(
		    (RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, datasetToRenumber), gridBlock[0],
		    gridBlock[1]);
	    
	    final N5Reader n5BlockReaderToUseForRenumbering = new N5FSReader(pathToUseForRenumbering);
	    IntervalView<T> datasetNumberingToUseView = Views.offsetInterval(
		    (RandomAccessibleInterval<T>) N5Utils.open(n5BlockReaderToUseForRenumbering, datasetNumberingToUse), gridBlock[0],
		    gridBlock[1]);
	    Cursor<T> datasetToRenumberCursor = datasetToRenumberView.cursor();
	    Cursor<T> datasetNumberingToUseCursor = datasetNumberingToUseView.cursor();

	    HashMap<Long, HashMap<Long, Long>> objectIDToRenumberingObjectIDCounts = new HashMap<Long,HashMap<Long,Long>>();
	    
	    
	    while (datasetToRenumberCursor.hasNext()) {
		datasetToRenumberCursor.next();
		datasetNumberingToUseCursor.next();
		long datasetToRenumberObjectID = datasetToRenumberCursor.get().getIntegerLong();
		long datasetNumberingToUseObjectID = datasetNumberingToUseCursor.get().getIntegerLong();

		if (datasetToRenumberObjectID > 0 && datasetNumberingToUseObjectID>0) {
		    HashMap<Long, Long> objectIDCounts = objectIDToRenumberingObjectIDCounts.getOrDefault(datasetToRenumberObjectID, new HashMap<Long,Long>());
		    long currentObjectCount = objectIDCounts.getOrDefault(datasetNumberingToUseObjectID,0L);
		    currentObjectCount++;
		    objectIDCounts.put(datasetNumberingToUseObjectID, currentObjectCount);
		    objectIDToRenumberingObjectIDCounts.put(datasetToRenumberObjectID,objectIDCounts);		    
		}
	    }
	    return objectIDToRenumberingObjectIDCounts;
	});

	HashMap<Long, HashMap<Long, Long>> objectIDToRenumberingObjectIDCounts = objectIDToRenumberingObjectIDCountsRDD.reduce(
		(a, b) -> {
		    for (Entry<Long, HashMap<Long, Long>> e : a.entrySet() ) {
			long datasetToRenumberObjectID = e.getKey();
			HashMap<Long, Long> renumberingObjectIDCounts_a = e.getValue();
			
			if(b.containsKey(datasetToRenumberObjectID)) {
			    HashMap<Long, Long> renumberingObjectIDCounts_b = b.get(datasetToRenumberObjectID);
			    for(Long renumberingObjectID : renumberingObjectIDCounts_a.keySet()) {
				Long currentCount = renumberingObjectIDCounts_b.getOrDefault(renumberingObjectID,0L);
				currentCount += renumberingObjectIDCounts_a.get(renumberingObjectID);
				renumberingObjectIDCounts_b.put(renumberingObjectID,currentCount);
			    }	
			}
			else {
			    b.put(datasetToRenumberObjectID, renumberingObjectIDCounts_a);
			}
		    }
	    return b;
	});
	
	FileWriter csvWriter = new FileWriter(outputDirectory + "/" + datasetToRenumber + "_renumbering.csv");
	csvWriter.append("Original ID,Renumbered ID\n");

	//HashMap<Long, Long> renumberingMapBasedOnMajorityVoting = new HashMap<Long,Long>();
	for(Entry<Long, HashMap<Long, Long>> e : objectIDToRenumberingObjectIDCounts.entrySet()) {
	    Long originalObjectID = e.getKey();
	    HashMap<Long,Long> renumberingObjectIDCounts = objectIDToRenumberingObjectIDCounts.get(originalObjectID);
	    
	    long maxCount = 0;
	    Long renumberingObjectID=0L;
	    boolean isThereATie = false;
	    for(Entry<Long,Long> e2 : renumberingObjectIDCounts.entrySet()) {
		long count = e2.getValue();
		if (count>maxCount) {
		    maxCount = count;
		    renumberingObjectID = e2.getKey();
		    isThereATie=false;
		}
		else if(count==maxCount) {
		    isThereATie = true;
		}
	    }
	  /*  if(isThereATie) {
		csvWriter.flush();
		csvWriter.close();
		throw new Exception("Object is overlapped equally by multiple objects");
	    }
	    if(renumberingObjectID==0) {
		csvWriter.flush();
		csvWriter.close();
		throw new Exception("No overlapping objects");
	    }
	    */
	    if(renumberingObjectID!=0) {
		    csvWriter.append(originalObjectID + "," + renumberingObjectID + "\n");
	    }
	    //renumberingMapBasedOnMajorityVoting.put(originalObjectID, renumberingObjectID);
	    
	}
	csvWriter.flush();
	csvWriter.close();

    }

    public static final <T extends IntegerType<T>> void getRenumbering(final JavaSparkContext sc, final String inputN5Path,
	    final String datasetName, final String outputDirectory, final List<BlockInformation> blockInformationList)
	    throws IOException {

	/*
	 * grid block size for parallelization to minimize double loading of blocks
	 */
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<Set<Long>> objectIDsRDD = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final N5Reader n5BlockReader = new N5FSReader(inputN5Path);
	    IntervalView<T> source = Views.offsetInterval(
		    (RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, datasetName), gridBlock[0],
		    gridBlock[1]);

	    Set<Long> objectIDs = new HashSet<Long>();
	    Cursor<T> sourceCursor = source.cursor();
	    while (sourceCursor.hasNext()) {
		sourceCursor.next();
		long objectID = sourceCursor.get().getIntegerLong();
		if (objectID > 0) {
		    objectIDs.add(sourceCursor.get().getIntegerLong());
		}
	    }
	    return objectIDs;
	});

	Set<Long> allObjectIDs = objectIDsRDD.reduce((a, b) -> {
	    a.addAll(b);
	    return a;
	});
	FileWriter csvWriter = new FileWriter(outputDirectory + "/" + datasetName + "_renumbering.csv");
	csvWriter.append("Original ID,Renumbered ID\n");

	long renumberedID = 1;
	for (Long objectID : allObjectIDs) {
	    csvWriter.append(objectID + "," + renumberedID + "\n");
	    renumberedID++;
	}
	csvWriter.flush();
	csvWriter.close();

    }

    public static final void setupSparkAndGetRenumbering(String inputN5Path, String pathToUseForRenumbering, String outputDirectory,
	    String inputN5DatasetName, String datasetToUseForRenumbering) throws Exception {
	final SparkConf conf = new SparkConf().setAppName("SparkGetRenumbering");

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

	File directory = new File(outputDirectory);
	if (!directory.exists()) {
	    directory.mkdirs();
	}
	
	String [] datasetToUseForRenumberingArray = null;
	if (datasetToUseForRenumbering != null) {
		datasetToUseForRenumberingArray = datasetToUseForRenumbering.split(",");
	}
	
	for (int i=0;i<organelles.length;i++) {
	    String currentOrganelle = organelles[i];
	    
	    // Create block information list
	    List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path,
		    currentOrganelle);
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    if (datasetToUseForRenumbering == null) {
		getRenumbering(sc, inputN5Path, currentOrganelle, outputDirectory, blockInformationList);
	    }
	    else {
		getRenumberingUsingOtherDataset(sc, inputN5Path, pathToUseForRenumbering, currentOrganelle, datasetToUseForRenumberingArray[i], outputDirectory, blockInformationList);
	    }
	    sc.close();
	}
    }

    public static final void main(final String... args) throws Exception {

	final Options options = new Options(args);

	if (!options.parsedSuccessfully)
	    return;
	String inputN5Path = options.getInputN5Path();
	String outputDirectory = options.getOutputDirectory();
	String inputN5DatasetName = options.getInputN5DatasetName();
	String pathToUseForRenumbering= options.getPathToUseForRenumbering();
	String datasetToUseForRenumbering= options.getDatasetToUseForRenumbering();
	setupSparkAndGetRenumbering(inputN5Path, pathToUseForRenumbering, outputDirectory, inputN5DatasetName,datasetToUseForRenumbering);

    }
}
