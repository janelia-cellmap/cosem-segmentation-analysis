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
package org.janelia.saalfeldlab.hotknife;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkRenumberConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputN5Path = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;
		
		@Option(name = "--convertToUINT16", required = false, usage = "N5 dataset, e.g. /mito")
		private boolean convertToUINT16 = false;

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

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}
		
		public boolean getConvertToUINT16() {
			return convertToUINT16;
		}

		public String getOutputN5Path() {
			return outputN5Path;
		}

	}

	public static final void renumber(
			final JavaSparkContext sc,
			final String n5Path,
			final String datasetName,
			final String n5OutputPath,
			boolean convertToUINT16,
			final List<BlockInformation> blockInformationList) throws IOException {

		final N5Reader n5Reader = new N5FSReader(n5Path);

		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
		final long[] dimensions = attributes.getDimensions();
		final int[] blockSize = attributes.getBlockSize();
		
		/*
		 * grid block size for parallelization to minimize double loading of
		 * blocks
		 */
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Set<Long>> objectIDsRDD = rdd.map(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			 IntervalView<UnsignedLongType> source = Views.offsetInterval(
						(RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, datasetName),gridBlock[0],gridBlock[1]);
				
				Set<Long> objectIDs = new HashSet<Long>();
				Cursor<UnsignedLongType> sourceCursor = source.cursor();
				while (sourceCursor.hasNext()) {
					sourceCursor.next();
					long objectID = sourceCursor.get().get();
					if (objectID>0) {
						objectIDs.add(sourceCursor.get().get());
					}
				}	
				return objectIDs;
		});
		
		Set<Long> allObjectIDs = objectIDsRDD.reduce((a,b) -> {a.addAll(b); return a; });
		Map<Long,Long> objectIDtoRenumberedID = new HashMap<Long,Long>();
		long currentID = 1;
		for( Long objectID : allObjectIDs ) {
			objectIDtoRenumberedID.put(objectID,currentID);
			currentID++;
		}
		
		final N5Writer n5Writer = new N5FSWriter(n5OutputPath);
		n5Writer.createDataset(
				datasetName + "_renumbered",
				dimensions,
				blockSize,
				convertToUINT16 ? DataType.UINT16 : DataType.UINT64,
				new GzipCompression());	
		n5Writer.setAttribute(datasetName + "_renumbered", "pixelResolution", new IOHelper.PixelResolution(IOHelper.getResolution(n5Reader, datasetName)));
		n5Writer.setAttribute(datasetName + "_renumbered", "offset", IOHelper.getOffset(n5Reader, datasetName));

		
		rdd.foreach(blockInformation -> {
			final long [][] gridBlock = blockInformation.gridBlock;
			final N5Reader n5BlockReader = new N5FSReader(n5Path);
			 final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
			if(convertToUINT16) {
				RandomAccessibleInterval<UnsignedLongType> source = Views.offsetInterval(
						(RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, datasetName),gridBlock[0],gridBlock[1]);
	
				final RandomAccessibleInterval<UnsignedShortType> sourceConverted = Converters.convert(
						source,
						(a, b) -> {
							long objectID = a.getIntegerLong();
							if(objectID>0) {
								b.set(objectIDtoRenumberedID.get(objectID).shortValue());
							}
							else {
								b.set(0);
							}
						},
						new UnsignedShortType());
				N5Utils.saveBlock(sourceConverted, n5BlockWriter, datasetName + "_renumbered", gridBlock[2]);

			}
			else {
				IntervalView<UnsignedLongType> source = Views.offsetInterval(
						(RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(n5BlockReader, datasetName),gridBlock[0],gridBlock[1]);
	
				Cursor<UnsignedLongType> sourceCursor = source.cursor();
				while (sourceCursor.hasNext()) {
					sourceCursor.next();
					long objectID = sourceCursor.get().get();
					if (objectID>0) {
						sourceCursor.get().set(objectIDtoRenumberedID.get(objectID));
					}
				}	

				N5Utils.saveBlock(source, n5BlockWriter, datasetName + "_renumbered", gridBlock[2]);
			}
		});
		
		
	}


	

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		final SparkConf conf = new SparkConf().setAppName("SparkRenumberConnectedComponents");

		// Get all organelles
		String[] organelles = { "" };
		if (options.getInputN5DatasetName() != null) {
			organelles = options.getInputN5DatasetName().split(",");
		} else {
			File file = new File(options.getInputN5Path());
			organelles = file.list(new FilenameFilter() {
				@Override
				public boolean accept(File current, String name) {
					return new File(current, name).isDirectory();
				}
			});
		}

		System.out.println(Arrays.toString(organelles));

		
		List<String> directoriesToDelete = new ArrayList<String>();
		for (String currentOrganelle : organelles) {
			
			//Create block information list
			List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getInputN5Path(),
				currentOrganelle);
			JavaSparkContext sc = new JavaSparkContext(conf);
			
			renumber(
					sc,
					options.getInputN5Path(),
					currentOrganelle,
					options.getOutputN5Path(),
					options.getConvertToUINT16(),
					blockInformationList);
			
			sc.close();
		}
		//Remove temporary files

	}
}
