/**
 *
 */
package org.janelia.saalfeldlab.hotknife;

import java.io.IOException;
import java.util.ArrayList;

import org.janelia.saalfeldlab.hotknife.ops.ContactSites;
import org.janelia.saalfeldlab.hotknife.ops.GradientCenter;
import org.janelia.saalfeldlab.hotknife.ops.TubenessCenter;
import org.janelia.saalfeldlab.hotknife.util.Lazy;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import bdv.util.volatiles.VolatileViews;
import ij.ImageJ;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.Volatile;
import net.imglib2.converter.Converters;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.Views;

/**
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 * Based on LazyBehavior.java by Stephen Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class LazyBehaviorDGA {

	private static int[] blockSize = new int[] {32, 32, 32};

	/**
	 * @param args
	 */
	public static void main(final String[] args) {

		new ImageJ();

		// set up N5 readers to read in raw EM data and predictions for COSEM
		N5FSReader n5_raw = null;
		N5FSReader n5_prediction = null;
		try {
			n5_prediction = new N5FSReader("/nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5/");
			n5_raw = new N5FSReader("/groups/cosem/cosem/data/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm.n5/");
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
		// store raw data and predictions for two organelles in array of UnsignedByteType random accessible intervals
		final ArrayList<RandomAccessibleInterval<UnsignedByteType>> img = new ArrayList<>();
		try {
			img.add(N5Utils.openVolatile(n5_raw, "volumes/raw"));
			img.add(N5Utils.openVolatile(n5_prediction, "plasma_membrane"));
			img.add(N5Utils.openVolatile(n5_prediction, "er"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		// convert UnsignedByteType random accessible intervals to DoubleType, extend them using a mirroring strategy.
		// add them to an extended random accessible array
		RandomAccessibleInterval<DoubleType> converted = null;
		final ArrayList<ExtendedRandomAccessibleInterval<DoubleType, RandomAccessibleInterval<DoubleType>>> source = new ArrayList<>();
		for (int i = 0; i < img.size(); i++) {
			converted =
					Converters.convert(
							img.get(i),
							(a, b) -> { b.set(a.getRealDouble()); },
							new DoubleType());
			source.add(Views.extendMirrorSingle(converted));
		}
		
		// display the raw data and predicted organelles
		BdvOptions options = BdvOptions.options();
		int imgCount = 0;
		for (final RandomAccessibleInterval<UnsignedByteType> currentImg : img)  {
			final BdvStackSource<Volatile<UnsignedByteType>> stackSource =
					BdvFunctions.show(
							VolatileViews.wrapAsVolatile(currentImg),
							"",
							options);
			stackSource.setDisplayRange(128, 128);
			if (imgCount == 1) {
				stackSource.setColor(new ARGBType( ARGBType.rgba(255,0,0,0)));
			}
			else if (imgCount == 2) {
				stackSource.setColor(new ARGBType( ARGBType.rgba(0,255,0,0)));
			}
			else {
				stackSource.setDisplayRange(0, 255);
			}
			options = options.addTo(stackSource);
			imgCount++;
		}
		
		final ArrayList<RandomAccessibleInterval<DoubleType>> analysisResults = new ArrayList<>();
		// calculate contact sites using ContactSites op and connected components using ConnectedComponentsOp
		
		// contact sites: here just doing it for one pair of organelles, but could loop over many
		for(int i=0; i<=0; i++) {
			final ContactSites<DoubleType> contactSitesOp = new ContactSites<>(source.get(1), source.get(2), 10);
			final RandomAccessibleInterval<DoubleType> currentContactSites = Lazy.process(
					img.get(0),
					blockSize,
					new DoubleType(),
					AccessFlags.setOf(AccessFlags.VOLATILE),
					contactSitesOp);
			analysisResults.add(currentContactSites);
		}
		
		
		// connected components: here just doing it for one organelle
		long [] sourceDimensions = {0,0,0};
		img.get(1).dimensions(sourceDimensions);

		// show contact sites in blue channel, and connected components in yellow
		imgCount=0;
		for (final RandomAccessibleInterval<DoubleType> currentAnalysisResults : analysisResults) {
			final BdvStackSource<Volatile<DoubleType>> stackSource =
					BdvFunctions.show(
							VolatileViews.wrapAsVolatile(currentAnalysisResults),
							"",
							options
							);
			if (imgCount==0) {
				stackSource.setDisplayRange(0, 255);
				stackSource.setColor(new ARGBType( ARGBType.rgba(0,0,255,0)));
			}
			else {
				stackSource.setDisplayRange(0, 65535);
				stackSource.setColor(new ARGBType( ARGBType.rgba(255,255,0,0)));
			}
			options = options.addTo(stackSource);
			imgCount++;
		}


	}
}
