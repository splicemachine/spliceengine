package com.splicemachine.stats;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * @author Scott Fines
 * Date: 1/16/14
 */
public class Timers {

		private Timers(){}
		private static final ThreadMXBean threadMXBean;
		private static final boolean supportsCPUTime;

		static{
				threadMXBean = ManagementFactory.getThreadMXBean();
				supportsCPUTime = threadMXBean.isCurrentThreadCpuTimeSupported();
		}

		public static Timer newTimer(){
				if(!supportsCPUTime)
						return new CompositeTimer(new NanoTimeMeasure(),new NoOpTimeMeasure(),new NoOpTimeMeasure());
				else
						return new CompositeTimer(new NanoTimeMeasure(),new CpuTimeMeasure(),new UserTimeMeasure());
		}

		public static Timer noOpTimer(){ return NoOpTimer.INSTANCE;}


		private static class NoOpTimer implements Timer{
				private static final NoOpTimer INSTANCE = new NoOpTimer();
				@Override public void startTiming() {  }
				@Override public void stopTiming() {  }
				@Override public void tick(long numEvents) {  }
				@Override public long getNumEvents() { return 0; }
				@Override public long getWallClockTime() { return 0; }
				@Override public long getCpuTime() { return 0; }
				@Override public long getUserTime() { return 0; }
		}

		private static class NoOpTimeMeasure implements TimeMeasure{
				private static final NoOpTimeMeasure INSTANCE = new NoOpTimeMeasure();
				@Override public void startTime() {  }
				@Override public void stopTime() {  }
				@Override public long getElapsedTime() { return 0; }
		}

		private static class CpuTimeMeasure extends BaseTimeMeasure{
				@Override
				protected long getTimestamp() {
						return threadMXBean.getCurrentThreadCpuTime();
				}
		}

		private static class UserTimeMeasure extends BaseTimeMeasure{
				@Override
				protected long getTimestamp() {
						return threadMXBean.getCurrentThreadUserTime();
				}
		}

		private static class NanoTimeMeasure extends BaseTimeMeasure{
				@Override
				protected long getTimestamp() {
						return System.nanoTime();
				}
		}

}
