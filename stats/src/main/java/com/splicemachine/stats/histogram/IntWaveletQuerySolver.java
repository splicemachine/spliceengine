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

import com.carrotsearch.hppc.IntDoubleOpenHashMap;

/**
 * @author Scott Fines
 *         Date: 4/30/14
 */
public class IntWaveletQuerySolver implements IntRangeQuerySolver{
		private final int minValue;
		private final int maxValue;
		private final long totalElements;

		private final WaveletCoefficient c1; //the root node of the tree
		private final double overallAverage;

		IntWaveletQuerySolver(int minValue,
													int maxValue,
												 long totalElements,
												 IntDoubleOpenHashMap coefficients,
												 int lgN,
												 double[] scale) {
				this.minValue = minValue;
				this.maxValue = maxValue;
				this.totalElements = totalElements;
				this.overallAverage = coefficients.get(0);

				this.c1 = new WaveletCoefficient(coefficients.get(1),1,lgN);
				buildCoefficientTree(c1,coefficients,lgN);
		}

		IntWaveletQuerySolver(int minValue,
													int maxValue,
													long totalElements,
													IntDoubleOpenHashMap coefficients,
													int lgN) {
				this.minValue = minValue;
				this.maxValue = maxValue;
				this.totalElements = totalElements;
				this.overallAverage = coefficients.get(0);

				this.c1 = new WaveletCoefficient(coefficients.get(1),1,lgN);
				buildCoefficientTree(c1,coefficients,lgN);
		}


		@Override public int min() { return minValue; }
		@Override public int max() { return maxValue; }

		@Override
		public long after(int value, boolean equals) {
				if(value>maxValue) return 0;
				else if(value == maxValue){
						if(equals) return equal(value);
						return 0;
				}else if(value<minValue){
						return totalElements;
				}else if(value==minValue){
						if(equals)
								return totalElements;
						else return totalElements-equal(value);
				}else
						return between(value,maxValue,equals,true);
		}

		@Override
		public long before(int value, boolean equals) {
				if(value<minValue) return 0;
				else if(value==minValue){
						if(equals)
								return equal(value);
						return 0;
				}else if(value>maxValue) return totalElements;
				else if(value==maxValue){
						if(equals) return totalElements;
						else return totalElements-equal(value);
				}else
						return between(minValue,value,true,equals);
		}

		@Override
		public long between(int startValue, int endValue, boolean inclusiveStart, boolean inclusiveEnd) {
				//if we ask for a range outside what we know we have convered, just return everything
				if((startValue<minValue || (startValue==minValue && inclusiveStart))
								&& (endValue>maxValue || (endValue==maxValue && inclusiveEnd)))
						return totalElements;

				double estimate = estimateBetween(startValue, endValue, c1, 0);
				estimate/=2;
				estimate += (overallAverage*(endValue-startValue));
				if(inclusiveEnd)
						estimate += estimateEquals(endValue);
				if(!inclusiveStart)
						estimate-=estimateEquals(startValue);

				return cleanupEstimate(estimate);
		}

		@Override
		public long equal(int value) {
				return cleanupEstimate(estimateEquals(value));
		}

		@Override
		public long getNumElements(Integer start, Integer end, boolean inclusiveStart, boolean inclusiveEnd) {
				if(start==null){
						if(end==null)
								return totalElements;
						else return before(end,inclusiveEnd);
				}else if(end==null)
						return after(start,inclusiveStart);
				else
						return between(start,end,inclusiveStart,inclusiveEnd);
		}

		@Override public Integer getMin() { return min(); }
		@Override public Integer getMax() { return max(); }


		int getNumWaveletCoefficients(){
				return countCoefs(c1)+1;
		}
/*************************************************************************************************************/
		/*private helper methods*/

		private int countCoefs(WaveletCoefficient coef){
				if(coef==null) return 0;
				return countCoefs(coef.left) + countCoefs(coef.right)+1;
		}

		private long cleanupEstimate(double estimate) {
				/*
				 * Coefficient math is fundamentally floating-point, which means that it can generate numbers
				 * which are way off. In this case, however, rounding is more accurate than truncation. We use
				 * abs() to deal with possibly negative values (although negative values will likely round to 0 anyway).
				 */
				return Math.abs((long)estimate);
		}

		long estimateEquals(int value){
				double estimate = overallAverage;
				WaveletCoefficient coef = c1;
				while(coef!=null){
						double coefficient = coef.coefficient;
						if(value>=coef.midPoint){
								estimate-=coefficient;
								coef = coef.right;
						}else{
								estimate+=coefficient;
								coef = coef.left;
						}
				}
				return (long)estimate;
		}


		private double estimateBetween(int start,
																	 int end,
																	 WaveletCoefficient coef,
																	 double currentSum) {
				/*
				 * If we have no coefficient, we assume it's zero
				 */
				if(coef==null) return currentSum;
				  /*
				   * There are 4 possibilities
				   * 1. start = coef.start && end == coef.end ==> return currentSum
				   * 2. [start,end) is wholly contained by [coef.left.start,coef.left.end) => subtract
				   * 3. [start,end) is wholly contained by [coef.right.start,coef.right.end) => add
				   * 4. [start,end) is in both left and right  => add AND subtract
				   */
				if(start==coef.start && end ==coef.stop){
						/*
						 * In situation 1, we have as many elements to the left as to the right, so
						 * our contributions will be totally cancelled out
						 */
						return currentSum;
				}

				if(end<=coef.midPoint){
						//we are wholly contained by the left side, so subtract out (end-start)*coefficient
						currentSum+=((end-start)*coef.coefficient);
						return estimateBetween(start,end,coef.left,currentSum);
				}else if(start>=coef.midPoint){
						//we are wholly contained by the right side, so add in (end-start)*coefficient
						currentSum-=((end-start)*coef.coefficient);
						return estimateBetween(start,end,coef.right,currentSum);
				}else{
						/*
						 * We straddle both sides, so we have to add in the proper proporitions
						 */
						currentSum+=(coef.midPoint-start)*coef.coefficient;
						currentSum-=(end-coef.midPoint)*coef.coefficient;
						//recurse both left and right
						currentSum=estimateBetween(start,coef.midPoint,coef.left,currentSum);
						currentSum=estimateBetween(coef.midPoint,end,coef.right,currentSum);
						return currentSum;
				}
		}

		private void buildCoefficientTree(WaveletCoefficient coef,
																			IntDoubleOpenHashMap coefficients,
																			int lgN) {
				int leftPos = 2*coef.position;
				int rightPos = leftPos+1;

				/*
				 * We add the sign check here to prevent multiplication overflow from
				 * causing an infinite loop (and thus stack overflow)
				 */
				if(leftPos>0 && coefficients.containsKey(leftPos)){
						WaveletCoefficient left = new WaveletCoefficient(coefficients.get(leftPos),leftPos,lgN);
						coef.left = left;
						buildCoefficientTree(left,coefficients,lgN);
				}
				if(rightPos>0 && coefficients.containsKey(rightPos)){
						WaveletCoefficient right = new WaveletCoefficient(coefficients.get(rightPos),rightPos,lgN);
						coef.right = right;
						buildCoefficientTree(right,coefficients,lgN);
				}
		}

		private class WaveletCoefficient {
				private final int start;
				private final int stop;
				private final int midPoint;
				private final double coefficient;
				private final int position;
				private final int size;
				private final double scale;

				private WaveletCoefficient left;
				private WaveletCoefficient right;

				private WaveletCoefficient(double coefficient,
																	 int position,
																	 int lgN) {
						int level = Integer.SIZE-Integer.numberOfLeadingZeros(position)-1;
						int k = position ^ Integer.highestOneBit(position);
						this.size = 1<<(lgN-level);
						this.start = k*size - (1<<(lgN-1));
						this.stop = start+size;
						this.scale = Math.sqrt(size);
						this.coefficient = coefficient/scale;
						this.position = position;
						this.midPoint = (stop-start)/2+start;
				}


		}
}
