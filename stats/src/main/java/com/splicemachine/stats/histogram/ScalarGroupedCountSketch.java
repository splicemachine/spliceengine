/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.stats.histogram;


import com.splicemachine.hash.BooleanHash;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.Hash64;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.primitives.MoreArrays;

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

						h[i] = HashFunctions.murmur3(i);
						f[i] = HashFunctions.murmur3(i+1);
						eps[i] = HashFunctions.booleanHash(i);
				}
		}

		public void update(long i, double count){
				long id = id(i);
				for(int m =0;m<t;m++){
						int hPos = h[m].hash(id) & (b-1);
						int fPos = f[m].hash(i) & (c-1);

						if(eps[m].hash(i))
                s[m][hPos][fPos] -= count;
            else
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
		}

		public double getValue(long i) {
				long id = id(i);
				for(int m=0;m<t;m++){
						int hPos = h[m].hash(id) & (b-1);
						int fPos = f[m].hash(i) & (c-1);

						possibleCoefficients[m] = s[m][hPos][fPos];
				}
				return median(possibleCoefficients);
		}

		private long id(long i) {
				return i ^ Long.highestOneBit(i);
		}

		private double median(double[] elements){
				if(t==1) return elements[0];
        return MoreArrays.median(elements);
    }
}
