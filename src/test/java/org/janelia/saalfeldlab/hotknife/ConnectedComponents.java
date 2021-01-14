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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.labeling.ConnectedComponentAnalysis;
import net.imglib2.algorithm.neighborhood.RectangleShape;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.logic.BoolType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
/**
 *
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class ConnectedComponents {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		Set<long[][]> temp = new HashSet<>();
		Map<Long,Long> junk = new HashMap<>();
		junk.put(5L,1L);
		junk.put(3L,2L);
		junk.put(0L,3L);
		junk.put(6L,4L);
		junk.put(2L,5L);

		for(Entry<Long,Long> j : junk.entrySet()) {
System.out.println(j.getKey()+" "+j.getValue());
}
/*	//	ImagePlus imp = new Opener().openImage( "/groups/cosem/cosem/ackermand/other_skeleton_failed.tif");//failed_skeleton.tif");//jan_skeleton.tif");//HeLa_Cell3_4x4x4nm_it450000_crop_analysis.n5/skeletonShortestPathAsRadii.tif" );
		//ImagePlus imp = new Opener().openImage("/groups/cosem/cosem/ackermand/tempRibosomes.n5/ribosomes_cc_medialSurface.tif");
		//ImagePlus imp = new Opener().openImage("/groups/cosem/cosem/weigela/hela3/original_HeLa_Cell3_4x4x4nm_it650000_plasma_membrane_ccSkipSmoothing_ds8_8bit.tif");
		//ImagePlus imp = new Opener().openImage("/groups/cosem/cosem/weigela/hela3/mito_HeLa_Cell3_4x4x4nm_setup01_it825000_results_25_0.875_smoothed_filled_8bit.tif");
		//RandomAccessible<UnsignedByteType> wrapImg = ImageJFunctions.wrapByte(imp);
		//ra = RandomAccessible<>wrapImg;
		

		final RandomAccessible<BoolType> thresholded = Converters.convert(wrapImg, (a, b) -> b.set( a.getInteger() > 0), new BoolType());
		final ArrayImg<UnsignedLongType, LongArray> components = ArrayImgs.unsignedLongs(new long[] {1518,132,1474});//{501,501,501});
		ConnectedComponentAnalysis.connectedComponents(Views.offsetInterval(thresholded,new long[]{0,0,0},new long[] {1518,132,1474}), components,new RectangleShape(1,false));//{501,501,501}), components,new RectangleShape(1,false));
		new ImageJ();
		ImageJFunctions.show(components);
		*/
		
		final N5FSReader n5 = new N5FSReader("/groups/cosem/cosem/ackermand/supplementContactSites.n5");
		final RandomAccessibleInterval<UnsignedLongType> datasetA = N5Utils.open(n5, "mito_cc_contact_boundary_temp_to_delete");
		final RandomAccessibleInterval<UnsignedLongType> datasetB = N5Utils.open(n5, "er_cc_contact_boundary_temp_to_delete");

		net.imglib2.RandomAccess<UnsignedLongType> aRA = datasetA.randomAccess();
		net.imglib2.RandomAccess<UnsignedLongType> bRA = datasetB.randomAccess();
		Set<Long> aIDs = new HashSet<Long>();
		Set<Long> bIDs = new HashSet<Long>();
		for(long x=0; x<datasetA.dimension(0);x++) {
			for(long y=0;y<datasetA.dimension(1);y++) {
				for(long z=0; z<datasetA.dimension(2);z++) {
					
					aRA.setPosition(new long[] {x,y,z});
					bRA.setPosition(new long[] {x,y,z});
					long aID = aRA.get().get();
					long bID = bRA.get().get();
					if(aID>0 && bID>0) {
						aRA.get().set(1);
					}
					else {
						aRA.get().set(0);
					}
					
				}
			}
		}
		System.out.println(aIDs.size()+" "+bIDs.size());
		//final N5FSReader n5 = new N5FSReader("/nrs/saalfeld/FAFB00/v14_align_tps_20170818_dmg.n5");
		//final RandomAccessibleInterval<UnsignedByteType> img = N5Utils.open(n5, "/volumes/predictions/synapses_dt_reblocked/s0");

		//final N5FSReader n5 = new N5FSReader("/groups/cosem/cosem/ackermand/HeLa_Cell3_4x4x4nm_it450000_crop_analysis.n5");
		//final RandomAccessibleInterval<UnsignedLongType> crop = N5Utils.open(n5, "mito_cc_filled");

		//final RandomAccessibleInterval<UnsignedByteType> crop = Views.offsetInterval(img, new long[] {100000,65000,3500}, new long[] {64,64,64});
		long t0 = System.currentTimeMillis();

		final RandomAccessibleInterval<BoolType> thresholded = Converters.convert(datasetA, (a, b) -> b.set(a.getInteger() > 0), new BoolType());

		final ArrayImg<UnsignedLongType, LongArray> components = ArrayImgs.unsignedLongs(Intervals.dimensionsAsLongArray(thresholded));
		ConnectedComponentAnalysis.connectedComponents(thresholded, components,new RectangleShape(1,false));
		long maxID = -1;
		ArrayRandomAccess<UnsignedLongType> ra = components.randomAccess();
		for(long x=0; x<components.dimension(0);x++) {
			for(long y=0;y<components.dimension(1);y++) {
				for(long z=0; z<components.dimension(2);z++) {
					ra.setPosition(new long[] {x,y,z});
					long currentID = ra.get().get();
					if(currentID>maxID) {
						maxID=currentID;
					}
				}
			}
		}
		System.out.println(maxID);
		long t1 = System.currentTimeMillis();
		System.out.println(t1-t0);
		
		//new ImageJ();
		//ImageJFunctions.show(components);
		
		final DatasetAttributes attributes = n5.getDatasetAttributes("mito_cc_to_er_cc_cc");
		final N5Writer n5Writer = new N5FSWriter("/groups/cosem/cosem/ackermand/supplementContactSites.n5");
		n5Writer.setAttribute("oldMethod", "pixelResolution", new IOHelper.PixelResolution(new double[] {4,4,4}));
		//n5Writer.createDataset("oldMethod", attributes.getDimensions(),attributes.getBlockSize(),attributes.getDataType(),attributes.getCompression());
		N5Utils.save(components, n5Writer, "oldMethod", attributes.getBlockSize(), attributes.getCompression());
		//System.out.println(Arrays.toString(Intervals.dimensionsAsLongArray(img)));
	}

}

