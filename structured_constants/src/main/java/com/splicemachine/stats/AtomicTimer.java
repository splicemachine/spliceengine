package com.splicemachine.stats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A Timer which can be shared among many threads.
 *
 * This differs from a {@link com.splicemachine.stats.Timer} implementation
 * mainly in that this hold central information, and acts as a factory for other,
 * non-thread-safe timers, which update the central information.
 *
 * @author Scott Fines
 * Date: 1/23/14
 */
public class AtomicTimer implements MetricFactory{
		private AtomicLong totalWallTime;
		private AtomicLong totalCpuTime;
		private AtomicLong totalUserTime;
		private AtomicLong totalEvents;
		private AtomicLong totalCountEvents;

		private final Counter counter = new Counter() {
								@Override public void add(long value) { totalCountEvents.addAndGet(value); }
								@Override public long getTotal() { return totalCountEvents.get(); }
								@Override public boolean isActive() { return true; }
						};

		private final TimeView view;

		public AtomicTimer() {
				this.totalWallTime = new AtomicLong(0l);
				this.totalEvents = new AtomicLong(0l);
				this.totalCountEvents = new AtomicLong(0l);
				if(Metrics.supportsCPUTime){
						totalCpuTime = new AtomicLong(0l);
						totalUserTime = new AtomicLong(0l);
						view = new TimeView() {
								@Override public long getWallClockTime() { return totalWallTime.get(); }
								@Override public long getCpuTime() { return totalCpuTime.get(); }
								@Override public long getUserTime() { return totalUserTime.get(); }
								@Override public long getStopWallTimestamp() { return -1l; }
								@Override public long getStartWallTimestamp() { return -1l; }
						};
				}else{
						view = new TimeView() {
								@Override public long getWallClockTime() { return totalWallTime.get(); }
								@Override public long getCpuTime() { return 0; }
								@Override public long getUserTime() { return 0; }
								@Override public long getStopWallTimestamp() { return -1l; }
								@Override public long getStartWallTimestamp() { return -1l; }
						};
				}

		}

		@Override
		public Timer newTimer(){
				TimeMeasure wallMeasure = new UpdatingWallTimeMeasure();
				TimeMeasure cpuMeasure,userMeasure;
				if(Metrics.supportsCPUTime){
						cpuMeasure = new UpdatingCpuTimeMeasure();
						userMeasure = new UpdatingUserTimeMeasure();
				}else{
						cpuMeasure = userMeasure = Metrics.noOpTimeMeasure();
				}
				return new UpdatingTimer(wallMeasure,cpuMeasure,userMeasure);
		}

		@Override public Gauge newMaxGauge() { return Metrics.noOpGauge(); }
		@Override public Gauge newMinGauge() { return Metrics.noOpGauge(); }

		public long getTotalEvents(){ return totalEvents.get();}
		public TimeView getTimeView(){ return view; }

		public long getTotalCountedValues(){ return counter.getTotal(); }

		@Override public Counter newCounter() { return counter; }

		private abstract class BaseUpdatingTimeMeasure extends BaseTimeMeasure{
				@Override
				public void stopTime() {
						super.stopTime();
						update((stop - start));
				}

				protected abstract void update(long time);
		}

		private class UpdatingWallTimeMeasure extends BaseUpdatingTimeMeasure{
				@Override protected long getTimestamp() { return System.nanoTime(); }

				@Override protected void update(long time) { totalWallTime.addAndGet(time); }
		}
		private class UpdatingCpuTimeMeasure extends BaseUpdatingTimeMeasure{
				@Override protected long getTimestamp() { return Metrics.threadMXBean.getCurrentThreadCpuTime(); }
				@Override protected void update(long time) { totalCpuTime.addAndGet(time); }
		}
		private class UpdatingUserTimeMeasure extends BaseUpdatingTimeMeasure{
				@Override protected long getTimestamp() { return Metrics.threadMXBean.getCurrentThreadUserTime(); }
				@Override protected void update(long time) { totalUserTime.addAndGet(time); }
		}

		private class UpdatingTimer extends CompositeTimer{
				public UpdatingTimer(TimeMeasure wallClockTime,
														 TimeMeasure userTime,
														 TimeMeasure cpuTime) {
						super(wallClockTime, userTime, cpuTime);
				}

				@Override
				public void tick(long numEvents) {
						super.tick(numEvents);
						totalEvents.addAndGet(numEvents);
				}
		}

}
