package com.splicemachine.stats.util;

import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.stats.DoubleFunction;

/**
 * Linearly interpolates between fixed points.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public class LinearInterpolator implements DoubleFunction {
		private final double[] x;
		private final double[] slopes;
		private final double[] intersects;

		@ThreadSafe
		public static LinearInterpolator create(double[] x, double[] y){
				/*
				 * We make a copy of x to ensure that we disassociate what
				 * we store in x with what was passed in. This makes the class
				 * effectively stateless, and therefore thread-safe.
				 */
				double[] xCopy = new double[x.length];
				System.arraycopy(x,0,xCopy,0,x.length);
				return new LinearInterpolator(xCopy,y);
		}

		public static LinearInterpolator createRaw(double[] x, double[] y){
				return new LinearInterpolator(x,y);
		}

		private LinearInterpolator(double[] x, double[] y) {
				this.x = x;

				//compute the slopes and intersects between two adjacent points
				this.slopes = new double[x.length];
				this.intersects = new double[x.length];
				for(int i=0;i<x.length-1;i++){
						double xLow = x[i];
						double xHigh = x[i+1];
						double yLow = y[i];
						double yHigh = y[i+1];

						double slope = (yHigh-yLow)/(xHigh-xLow);
						double intersect = yLow - slope*xLow;
						slopes[i] = slope;
						intersects[i] = intersect;
				}
				//the last entry has the same slope and intersect as the entry before it
				slopes[slopes.length-1] = slopes[slopes.length-2];
				intersects[intersects.length-1] = intersects[intersects.length-2];
		}

		public double y(double x){
				double next;
				int pos=0;
				do{
						next = this.x[pos];
						pos++;
				}while(pos < this.x.length && next <= x);

				pos--;
				double slope = slopes[pos];
				double intersect = intersects[pos];

				return slope*x+intersect;
		}

		public static void main(String...args) throws Exception{
				double[] x = new double[]{0d,1d,2d,3d,4d,5.2d,6d,7d,8d,9d,10d};
				double[] y = new double[]{0d,1d,2d,3d,4d,12d,6d,7d,8d,9d,10d};

				LinearInterpolator interpolator = LinearInterpolator.create(x,y);
				System.out.println(interpolator.y(5.5d));
				System.out.println(interpolator.y(4.5d));
		}
}
