package com.splicemachine.logicalstats.histogram;

import com.splicemachine.utils.hash.BooleanHash;
import com.splicemachine.utils.hash.Hash32;
import com.splicemachine.utils.hash.HashFunctions;

import java.util.Arrays;

/**
 * GroupedCount Sketch for scalar-encoded values.
 *
 * @author Scott Fines
 * Date: 5/22/14
 */
public class ScalarGroupedCountSketch {
		private final int t;
		private final int b;
		private final int c;

		private final double[][][] s;

		private final Hash32[] h;
		private final Hash32[] f;
		private final BooleanHash[] eps;


		private final double[] possibleCoefficients;

		public ScalarGroupedCountSketch(int t, float epsilon) {
				assert epsilon>0 && epsilon<1: "Epsilon must be between 0 and 1";
				this.t = t;

				float size = 1/epsilon;
				int temp = 1;
				while(temp<size)
						temp<<=1;
				this.b = temp;
				size/=epsilon;
				while(temp<size)
						temp<<=1;
				this.c = temp;

				this.s = new double[t][][];
				this.h = new Hash32[t];
				this.f = new Hash32[t];
				this.eps = new BooleanHash[t];
				this.possibleCoefficients = new double[t];

				for(int i=0;i<t;i++){
						s[i] = new double[b][];
						for(int j=0;j<b;j++){
								s[i][j] = new double[c];
						}

						h[i] = HashFunctions.murmur3(2*i+1);
						f[i] = HashFunctions.murmur3(3*i+2);
//						eps[i] = HashFunctions.booleanHash(i<<1+t);
						eps[i] = HashFunctions.fourWiseBooleanHash(i);
				}
		}

		public void update(long i, double count){
				long id = id(i);
				for(int m =0;m<t;m++){
						int hPos = Math.abs(h[m].hash(id)) & (b-1);
						int fPos = Math.abs(f[m].hash(i)) & (c-1);

						if(eps[m].hash(i))
								count = -count;
						s[m][hPos][fPos] += count;
				}
		}

		public double estimateEnergy(long group){
				for(int m=0;m<t;m++){
						int hPos = Math.abs(h[m].hash(group)) & (b-1);
						double sum=0;
						for(int j=0;j<c;j++){
								sum+=Math.pow(s[m][hPos][j],2);
						}

						possibleCoefficients[m] = sum;
				}
				return median(possibleCoefficients);
//				Arrays.sort(possibleCoefficients);
//				return possibleCoefficients[possibleCoefficients.length/2];
		}

		public double getValue(long i) {
				long id = id(i);
				for(int m=0;m<t;m++){
						int hPos = Math.abs(h[m].hash(id)) & (b-1);
						int fPos = Math.abs(f[m].hash(i)) & (c-1);

						possibleCoefficients[m] = s[m][hPos][fPos];
				}
				return median(possibleCoefficients);
//				Arrays.sort(possibleCoefficients);
//				return possibleCoefficients[possibleCoefficients.length/2];
		}

		private long id(long i) {
				return i ^ Long.highestOneBit(i);
		}

		private double median(double[] elements){
//				if(true){
//						Arrays.sort(elements);
//						return elements[elements.length/2];
//				}
				if(t==1) return elements[0];

				int from = 0;
				int to = elements.length-1;
				int k = elements.length/2;
				while(from < to){
						int r = from;
						int w = to;
						double mid = elements[(r+w)>>1];

						while(r<w){
								if(elements[r] >=mid){
										double tmp = elements[w];
										elements[w] = elements[r];
										elements[r] = tmp;
										w--;
								}else
										r++;
						}
						if(elements[r]>mid)
								r--;

						if(k<=r)
								to = r;
						else
								from = r+1;
				}
				return elements[k];
		}
}
