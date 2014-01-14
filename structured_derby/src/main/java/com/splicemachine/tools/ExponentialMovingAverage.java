package com.splicemachine.tools;

import com.yammer.metrics.stats.EWMA;
import org.cliffc.high_scale_lib.Counter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Represents an Exponential Weighted Moving Average, similar to
 * a Linux Load average.
 *
 * Times are detected by measuring the System clock passage between
 * intervals.
 *
 * This class is thread safe.
 *
 * @author Scott Fines
 * Date: 11/25/13
 */
public class ExponentialMovingAverage implements MovingStatistics{
		private final double interval;

		private volatile double mean;
		private volatile double variance;

		private volatile long lastTs;
		private final Counter counter = new Counter();

		public ExponentialMovingAverage(double interval) {
				lastTs = System.nanoTime();
				this.interval = interval;
		}

		@Override
		public double estimateMean() {
				return mean;
		}

		@Override
		public double estimateVariance() {
				return variance;
		}

		@Override
		public long numberOfMeasurements() {
				return counter.get();
		}

		@Override
		public void update(double measurement) {
				long ts = System.nanoTime();
				counter.increment();
				synchronized (this){
						double alpha = Math.exp((ts-lastTs)/interval);
						double diff = measurement-mean;
						mean = mean + alpha*diff;
						variance = (1-alpha)*(variance+alpha*diff*diff);
						lastTs = ts;
				}
		}

		public static void main(String...args) throws Exception{
				ExponentialMovingAverage ewma = new ExponentialMovingAverage(TimeUnit.SECONDS.toNanos(60l));
				Random random = new Random();
				for(int i=0;i<10000;i++){
						ewma.update(random.nextInt(100));
				}

				System.out.println(ewma.estimateMean());
				System.out.println(ewma.estimateVariance());
				System.out.println(ewma.numberOfMeasurements());
		}

}
