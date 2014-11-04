package com.splicemachine.tools;

import com.google.common.util.concurrent.AtomicDoubleArray;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 11/26/13
 */
public class WindowedAverage implements MovingStatistics{
		private final AtomicDoubleArray window;

		private final AtomicInteger position = new AtomicInteger(0);

		//cached value for performance
		private final int lengthModulus;

		public WindowedAverage(int size) {
				int s = 1;
				while(s<size){
						s<<=1;
				}
				//making it a power of 2 makes computing modulus cheap
				lengthModulus = s-1;
				this.window = new AtomicDoubleArray(s);
		}

		@Override
		public double estimateMean() {
				double sum=0d;
				int length = window.length();
				for(int i=0;i< length;i++){
					sum+=window.get(i);
				}
				return sum/ length;
		}

		@Override
		public double estimateVariance() {
				double mean =0d;
				double variance = 0d;
				int length = window.length();
				for(int i=0;i<length;i++){
						double measure = window.get(i);
						double oldMean = mean;
						mean = mean +(measure-mean)/(i+1);
						variance = variance + (measure-oldMean)*(measure-mean);
				}

				return variance;
		}

		@Override
		public long numberOfMeasurements() {
				return position.get();
		}

		@Override
		public void update(double measurement) {
				int p = position.getAndIncrement() & lengthModulus;
				window.set(p,measurement);
		}

		public long getWindowSize(){
				return window.length();
		}

		public double estimateDeviation() {
				return Math.sqrt(estimateVariance()/((position.get() & lengthModulus)+1));
		}
}
