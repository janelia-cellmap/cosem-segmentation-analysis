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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.RandomAccess;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;


/**
 * Expand dataset
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkProtrusionArea {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = true, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputDirectory = null;

		@Option(name = "--protrusionDataset", required = true, usage = "Protrusion dataset")
		private String protrusionDataset = null;
		
		@Option(name = "--protrusionCellDataset", required = true, usage = "Dataset of cell from which protrusion was determined")
		private String protrusionCellDataset = null;
		
		@Option(name = "--expandedCellDataset", required = true, usage = "Expanded cell dataset used to determine how much protrusion/nonprotrusion surface area of one cell is within distance of neighboring cell")
		private String expandedCellDataset = null;
		

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
		
		public String getOutputDirectory() {
			return outputDirectory;
		}
		
		public String getProtrusionDataset() {
			return protrusionDataset;
		}
		

		public String getProtrusionCellDataset() {
			return protrusionCellDataset;
		}
		
		public String getExpandedCellDataset() {
			return expandedCellDataset;
		}
		

	}


	private static <T extends IntegerType<T> & NativeType<T>> void getProtrusionArea(JavaSparkContext sc, String inputN5Path, String protrusionCellDataset,
		String protrusionDataset, String expandedCellDataset, String outputDirectory,
		List<BlockInformation> blockInformationList) throws IOException {
	    // TODO Auto-generated method stub
	    
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		double [] pixelResolution = IOHelper.getResolution(n5Reader, protrusionCellDataset);
		double pixelArea = pixelResolution[0]*pixelResolution[1];
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		
		JavaRDD<Map<Long,long[]>> surfaceAreaMapRDD = rdd.map(blockInformation -> {
			final long[][] gridBlock = blockInformation.gridBlock;
			long[] offset = gridBlock[0];
			long[] paddedOffset = new long[] {offset[0]-1, offset[1]-1, offset[2]-1};
			long[] dimension = gridBlock[1];
			long[] paddedDimension = new long[] {dimension[0]+2, dimension[1]+2, dimension[2]+2};			

			RandomAccess<T> protrusionCellRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(inputN5Path, protrusionCellDataset, paddedOffset, paddedDimension);
			RandomAccess<T> protrusionRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(inputN5Path, protrusionDataset, paddedOffset, paddedDimension);
			RandomAccess<T> expandedCellRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(inputN5Path, expandedCellDataset, paddedOffset, paddedDimension);
			
			Map<Long,long[]> surfaceAreaMapper = new HashMap<Long,long[]>();
			for(int x=1; x<paddedDimension[0]-1; x++) {
			    for(int y=1; y<paddedDimension[1]-1; y++) {
				for(int z=1; z<paddedDimension[2]-1; z++) {
				    long [] pos = new long[] {x,y,z};
				    protrusionRA.setPosition(pos);
				    long ID = protrusionRA.get().getIntegerLong(); //0 if non-protrusion, otherwise id of protrusion
				    long[] currentMapping = surfaceAreaMapper.getOrDefault(ID, new long[]{0,0});

				    protrusionCellRA.setPosition(pos);
				    int surfaceAreaContributionInVoxels = getSurfaceAreaContributionOfVoxelInFaces(protrusionCellRA);
				    if(surfaceAreaContributionInVoxels>0) {
					//then determine if it was part of protrusion or main body. and if it was within synapse of other cell
					expandedCellRA.setPosition(pos);
					

					int isPartOfSynapse = expandedCellRA.get().getIntegerLong()>0 ? 1 : 0; //0 if it was outside 1 um, otherwise has the number of the cell
					currentMapping[isPartOfSynapse]+=surfaceAreaContributionInVoxels;
				    }
				    surfaceAreaMapper.put(ID, currentMapping);
				}
				
			    }
			}	
			return surfaceAreaMapper;
		});
		
		Map<Long, long[]> collectedSurfaceAreaMap = surfaceAreaMapRDD.reduce((a,b) -> {
			for(Long ID : b.keySet()){
			    if(a.containsKey(ID)) {
				long[] surfaceArea_a = a.get(ID);
				long[] surfaceArea_b = b.get(ID);
				surfaceArea_a[0]+=surfaceArea_b[0];
				surfaceArea_a[1]+=surfaceArea_b[1];
				a.put(ID, surfaceArea_a);
			    }
			    else {
				a.put(ID, b.get(ID));
			    }
			}
			return a;
		});
		
		writeSurfaceArea(collectedSurfaceAreaMap, outputDirectory, protrusionCellDataset, pixelArea);
	}
	
	private static void writeSurfaceArea(Map<Long, long[]> collectedSurfaceAreaMap, String outputDirectory,
		String protrusionCellDataset, double pixelArea) throws IOException {
	    
        	if (! new File(outputDirectory).exists()){
        		new File(outputDirectory).mkdirs();
        	}
	
	    	FileWriter csvWriter = new FileWriter(outputDirectory+"/"+protrusionCellDataset+"_protrusionSurfaceArea.csv");
		csvWriter.append("ID,Non-synapse Surface Area (nm^2),Synapse Surface Area (nm^2)\n");
		
		for(long ID : collectedSurfaceAreaMap.keySet()) {
		    long[] surfaceAreas = collectedSurfaceAreaMap.get(ID);
		    csvWriter.append( Long.toString(ID)+","+Double.toString(surfaceAreas[0]*pixelArea)+","+Double.toString(surfaceAreas[1]*pixelArea)+"\n");
		}
		csvWriter.flush();
		csvWriter.close();
	}

	public static <T extends IntegerType<T> & NativeType<T>> int getSurfaceAreaContributionOfVoxelInFaces(final RandomAccess<T> sourceRandomAccess) {
		
	    	//For surface area
    	  	List<long[]> voxelsToCheck = new ArrayList(); 
    	  	voxelsToCheck.add(new long[] {-1, 0, 0});
    	  	voxelsToCheck.add(new long[] {1, 0, 0});
    	  	voxelsToCheck.add(new long[] {0, -1, 0});
    	  	voxelsToCheck.add(new long[] {0, 1, 0});
    	  	voxelsToCheck.add(new long[] {0, 0, -1});
    	  	voxelsToCheck.add(new long[] {0, 0, 1});
	    	long referenceVoxelValue = sourceRandomAccess.get().getIntegerLong();
		final long sourceRandomAccessPosition[] = {sourceRandomAccess.getLongPosition(0), sourceRandomAccess.getLongPosition(1), sourceRandomAccess.getLongPosition(2)};
		int surfaceAreaContributionOfVoxelInFaces = 0;
		
		if(referenceVoxelValue>0) {
			for(long[] currentVoxel : voxelsToCheck) {
				final long currentPosition[] = {sourceRandomAccessPosition[0]+currentVoxel[0], sourceRandomAccessPosition[1]+currentVoxel[1], sourceRandomAccessPosition[2]+currentVoxel[2]};
				sourceRandomAccess.setPosition(currentPosition);
				if(sourceRandomAccess.get().getIntegerLong() != referenceVoxelValue) {
					surfaceAreaContributionOfVoxelInFaces ++;
				}
			}
		}
		return surfaceAreaContributionOfVoxelInFaces;	
	
	}
	
	private static void setupSparkAndGetProtrusionArea(String inputN5Path, String protrusionCellDataset,
		String protrusionDataset, String expandedCellDataset, String outputDirectory) throws IOException {
		final SparkConf conf = new SparkConf().setAppName("SparkProtrusionArea");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path, protrusionCellDataset);
		getProtrusionArea(sc, inputN5Path, protrusionCellDataset, protrusionDataset, expandedCellDataset, outputDirectory, blockInformationList);
		sc.close();
	}

	/**
	 * Expand skeleton for more visible meshes
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
		
		setupSparkAndGetProtrusionArea(options.getInputN5Path(), options.getProtrusionCellDataset(), options.getProtrusionDataset(), options.getExpandedCellDataset(), options.getOutputDirectory());
		
	}

}
