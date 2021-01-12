/*
 * #%L
 * ImageJ software for multidimensional image processing and analysis.
 * %%
 * Copyright (C) 2014 - 2017 Board of Regents of the University of
 * Wisconsin-Madison, University of Konstanz and Brian Northan.
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

package org.janelia.saalfeldlab.hotknife.ops;

import java.util.function.Consumer;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

/**
 * ContactSites
 *
 * class to calculate contact sites between two predicted organelles from COSEM data, using a distance threshold:
 *      if a voxel is within the distance threshold of both organelles, it is considered a contact site
 * @author David Ackerman
 */
public class ContactSites<T extends RealType<T> & NumericType<T> & NativeType<T>> implements Consumer<RandomAccessibleInterval<T>> {

	// class attributes: the two source organelle images and a distance cutoff, provided as input 
	final private RandomAccessible<? extends T> sourceA;
	final private RandomAccessible<? extends T> sourceB;
	private double distanceCutoff;
	
	public ContactSites(final RandomAccessible<T> sourceA, final RandomAccessible<T> sourceB, final double distanceCutoff ) {
		// constructor that takes in distance cutoff
		this.sourceA = sourceA;
		this.sourceB = sourceB;
		this.distanceCutoff = distanceCutoff;	
	}
	
	public ContactSites(final RandomAccessible<T> sourceA, final RandomAccessible<T> sourceB) {
		// constructor that can do something other than using a hard cutoff
		this.sourceA = sourceA;
		this.sourceB = sourceB;
		this.distanceCutoff = Double.NaN;	
	}

	@Override
	public void accept(final RandomAccessibleInterval<T> output) {
		// performs the actual calculation of whether a voxel is a contact site, with the result being stored in output
		
		
		// gets an interval over the source data and creates a cursor to iterate over the voxels
		final Cursor<? extends T> a = Views.flatIterable(Views.interval(sourceA, output)).cursor();
		final Cursor<? extends T> b = Views.flatIterable(Views.interval(sourceB, output)).cursor();
		final Cursor<T> c = Views.flatIterable(output).cursor();
		
		// boolean that could be used if hard cutoff is not provided; currently not in use
		final boolean distanceCutoffProvided = !Double.isNaN(distanceCutoff);
		
		// loop over voxels and calculate if given voxel is contact site
		while (c.hasNext()) {
			// basically next voxel for inputs and outputs
			final T tA = a.next();
			final T tB = b.next();
			final T tC = c.next();
			
			// pixel values
			final double pixelValueA = tA.getRealDouble();
			final double pixelValueB = tB.getRealDouble();
			
			// convert pixel values to distances, and ensure any possible contact site is outside both organelles
			final double xA = (pixelValueA-127)/128;
			final double xB = (pixelValueB-127)/128;
			final double distanceA = -25*Math.log( (1+xA)/(1-xA) );
			final double distanceB = -25*Math.log( (1+xB)/(1-xB) );
			final boolean outsideBothOrganelles = distanceA>=0 && distanceB>=0;
			//final double totalDistance = distanceA+distanceB;
			
			// set output value to 255 if is a contact site, otherwise 0
			tC.setZero();
			if (distanceCutoffProvided && outsideBothOrganelles && distanceA<=distanceCutoff && distanceB<=distanceCutoff) {
				tC.setReal(255);
			}
			else if ( !distanceCutoffProvided && outsideBothOrganelles) {
				//tC.setReal( Math.max(255-totalDistance,0) );
			}
		}
	}
	
}
