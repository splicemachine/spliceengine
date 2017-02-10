/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.metrics;


import com.splicemachine.utils.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.Arrays;
import java.util.Random;

/**
 * @author Scott Fines
 * Date: 2/11/14
 */
class SamplingCompositeTimer implements Timer,TimeView{
		private static final Unsafe unsafe = UnsafeUtil.unsafe();
		private static final long longArrayOffset = unsafe.arrayBaseOffset(long[].class);
		private final Random random;

		private final TimeMeasure wallClockTime;
		private final TimeMeasure cpuTime;
		private final TimeMeasure userTime;

		private final int sampleSize;

		private long[] sampleWallTimes;
		private long[] sampleCpuTimes;
		private long[] sampleUserTimes;

		private long numEvents;
		private int nextSamplePosition;
		private long numStarts;

		public SamplingCompositeTimer(TimeMeasure wallClockTime,
																	TimeMeasure userTime,
																	TimeMeasure cpuTime,
																	int sampleSize,
																	int initialSampleSize) {
				this.wallClockTime = wallClockTime;
				this.userTime = userTime;
				this.cpuTime = cpuTime;
				this.sampleSize = sampleSize;

				this.random = new Random(System.currentTimeMillis());
				this.sampleWallTimes = new long[initialSampleSize];
				this.sampleCpuTimes = new long[initialSampleSize];
				this.sampleUserTimes = new long[initialSampleSize];
		}

		@Override
		public void startTiming() {
				startMeasurements();
				int sampleSpot;
				if(numStarts<sampleSize)
						sampleSpot = (int)numStarts;
				else
						sampleSpot = random.nextInt((int)numStarts+1);

				numStarts++;

				if(sampleSpot <sampleSize){
						this.nextSamplePosition = sampleSpot;
				}else
						this.nextSamplePosition = -1;
		}

		protected void startMeasurements() {
				userTime.startTime();
				cpuTime.startTime();
				wallClockTime.startTime();
		}

		@Override
		public void stopTiming() {
				if(nextSamplePosition<0) return; //nothing to do, we aren't timing

				long wallTime = wallClockTime.stopTime();
				long elapsedUserTime = userTime.stopTime();
				long elapsedCpuTime  = cpuTime.stopTime();

				if(nextSamplePosition>sampleSize) return;

				if(nextSamplePosition>=sampleWallTimes.length){
						sampleWallTimes = Arrays.copyOf(sampleWallTimes,Math.min(sampleSize,2*sampleWallTimes.length));
						sampleCpuTimes = Arrays.copyOf(sampleCpuTimes,Math.min(sampleSize,2*sampleCpuTimes.length));
						sampleUserTimes = Arrays.copyOf(sampleUserTimes,Math.min(sampleSize,2*sampleUserTimes.length));
				}
				unsafe.putLong(sampleWallTimes, longArrayOffset+nextSamplePosition*8, wallTime);
				unsafe.putLong(sampleCpuTimes, longArrayOffset+nextSamplePosition*8, elapsedCpuTime);
				unsafe.putLong(sampleUserTimes, longArrayOffset+nextSamplePosition*8, elapsedUserTime);

				nextSamplePosition=-1;
		}

		@Override
		public void tick(long numEvents) {
				stopTiming();
				this.numEvents+=numEvents;
		}

		@Override
		public long getWallClockTime() {
				return estimateElapsedTime(sampleWallTimes,wallClockTime.getElapsedTime());
		}

		@Override
		public long getCpuTime() {
				return estimateElapsedTime(sampleCpuTimes,cpuTime.getElapsedTime());
		}

		@Override
		public long getUserTime() {
				return estimateElapsedTime(sampleUserTimes,userTime.getElapsedTime());
		}

		@Override public long getStopWallTimestamp() { return wallClockTime.getStartTimestamp(); }
		@Override public long getStartWallTimestamp() { return wallClockTime.getStopTimestamp(); }

		@Override public long getNumEvents() { return numEvents; }
		@Override public TimeView getTime() { return this; }

		protected long estimateElapsedTime(long [] samples, long measuredTime) {
				if(numEvents<=sampleSize){
						//we measured the full time usage
						return measuredTime;
				}

				double v = computeAverage(samples);
				double extrapolatedTime = measuredTime+(v *numStarts)/sampleSize;
				return (long)extrapolatedTime;
		}

		private double computeAverage(long[] samples) {
				long sum = 0l;
				int count = 0;
				//noinspection ForLoopReplaceableByForEach
				for(int i=0;i<samples.length;i++){
						long sampleTime = samples[i];
						if(sampleTime>0){
								sum+=sampleTime;
								count++;
						}
				}
				return ((double)sum)/count;
		}

		private long computeSum(long[] samples) {
				long sum = 0l;
				//noinspection ForLoopReplaceableByForEach
				for(int i=0;i<samples.length;i++){
						long sampleTime = samples[i];
						if(sampleTime>0)
								sum+=sampleTime;
				}
				return sum;
		}

		public static void main(String...args) throws Exception{
				int sampleSize = 10;
				Timer timer = Metrics.samplingMetricFactory(sampleSize,sampleSize).newTimer();
				timeAndPrint(timer);

				timer = Metrics.noOpTimer();
				timeAndPrint(timer);

				//now do the same thing with the straight Composite Timer and see if the values are close
//				timer = Metrics.newTimer();
//				timeAndPrint(timer);
		}

		protected static void timeAndPrint(Timer timer) {
				Timer totalTimer = Metrics.newTimer();
				Random random = new Random(0l);
				totalTimer.startTiming();

				long sum=0l;
				for(int i=0;i<1000;i++){
						timer.startTiming();
						sum+=random.nextLong();
						timer.tick(1);
				}

				totalTimer.stopTiming();
				System.out.printf("%nIgnore:%d%n", sum);

				double conversion = 1000*1000*1000d;
				TimeView totalTime = totalTimer.getTime();
				System.out.printf("Total wall time taken:%f%n",totalTime.getWallClockTime()/conversion);
				System.out.printf("Total cpu time taken:%f%n",totalTime.getCpuTime()/conversion);
				System.out.printf("Total user time taken:%f%n",totalTime.getUserTime()/conversion);
				TimeView time = timer.getTime();
				double wallTimeS = time.getWallClockTime()/conversion;
				System.out.printf("WallTime:%f%n",wallTimeS);
				double cpuTimeS = time.getCpuTime()/conversion;
				System.out.printf("CpuTime:%f%n",cpuTimeS);
				double userTimeS = time.getUserTime()/conversion;
				System.out.printf("UserTime:%f%n",userTimeS);
				System.out.printf("NumEvents:%d%n",timer.getNumEvents());
		}

}
