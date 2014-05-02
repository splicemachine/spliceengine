package com.splicemachine.stats;

import com.splicemachine.stats.util.DoubleFolder;
import com.splicemachine.stats.util.Folders;
import com.splicemachine.utils.ThreadSafe;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * Base class for Metrics collection tools. Includes Timers, Gauges, and Counters.
 *
 * There are no public implementations of any Metrics tools--access should always go
 * through a factory method in this class. Included are non-thread
 *
 * @author Scott Fines
 * Date: 1/17/14
 */
public class Metrics {
		static final ThreadMXBean threadMXBean;
		static final boolean supportsCPUTime;

		static {
				threadMXBean = ManagementFactory.getThreadMXBean();
				//supportsCPUTime = threadMXBean.isCurrentThreadCpuTimeSupported();
				supportsCPUTime = false;
		}

		private static final MetricFactory NOOP_FACTORY = new MetricFactory() {
				@Override public Counter newCounter() { return NOOP_COUNTER; }
				@Override public Timer newTimer() { return NOOP_TIMER; }
				@Override public Gauge newMaxGauge() { return NOOP_GAUGE; }
				@Override public Gauge newMinGauge() { return NOOP_GAUGE; }
				@Override public boolean isActive() { return false; }
		};

		private static final IOStats NOOP_IO = new IOStats() {
				@Override public TimeView getTime() { return NOOP_TIME_VIEW; }
				@Override public long getRows() { return 0; }
				@Override public long getBytes() { return 0; }
		};

		private static final TimeMeasure NOOP_TIME_MEASURE = new TimeMeasure() {
				@Override public void startTime() { }
				@Override public long stopTime() {return 0l; }
				@Override public long getElapsedTime() { return 0; }
				@Override public long getStopTimestamp() { return 0; }
				@Override public long getStartTimestamp() { return 0; }
		};

		private static final Gauge NOOP_GAUGE = new Gauge() {
				@Override public void update(double value) { }
				@Override public double getValue() { return 0; }
				@Override public boolean isActive() { return false; }
		};

		private static final TimeView NOOP_TIME_VIEW = new TimeView() {
				@Override public long getWallClockTime() { return 0; }
				@Override public long getCpuTime() { return 0; }
				@Override public long getUserTime() { return 0; }
				@Override public long getStopWallTimestamp() { return 0; }
				@Override public long getStartWallTimestamp() { return 0; }
		};

		private static final Timer NOOP_TIMER = new Timer() {
				@Override public void startTiming() { }

				@Override public void startTiming(boolean force) {  }

				@Override public void stopTiming() { }
				@Override public void tick(long numEvents) { }
				@Override public long getNumEvents() { return 0; }
				@Override public TimeView getTime() { return NOOP_TIME_VIEW; }
		};

		private static Counter NOOP_COUNTER = new Counter() {
				@Override public void add(long value) { }
				@Override public long getTotal() { return 0; }
				@Override public boolean isActive() { return false; }
				@Override public void increment() {  }
		};

		private static MultiTimeView NOOP_MULTI_TIME_VIEW = new MultiTimeView() {
				@Override public void update(TimeView timeView) {  }
				@Override public long getWallClockTime() { return 0; }
				@Override public long getCpuTime() { return 0; }
				@Override public long getUserTime() { return 0; }
				@Override public long getStopWallTimestamp() { return 0; }
				@Override public long getStartWallTimestamp() { return 0; }
		};

		private Metrics() { }

		public static MetricFactory basicMetricFactory() { return new CreatingMetricFactory(); }

		public static MetricFactory samplingMetricFactory(int sampleSize){
				int initialSize = sampleSize>100? sampleSize/100: sampleSize;
				return samplingMetricFactory(sampleSize,initialSize);
		}

		public static MetricFactory samplingMetricFactory(int sampleSize,int initialSize){
				return new SamplingMetricFactory(sampleSize,initialSize);
		}

		@ThreadSafe
		public static MetricFactory noOpMetricFactory() { return NOOP_FACTORY; }

		@ThreadSafe
		public static AtomicTimer atomicTimer() { return new AtomicTimer(); }

		@ThreadSafe
		public static IOStats noOpIOStats() { return NOOP_IO; }

		@ThreadSafe
		public static Counter noOpCounter() { return NOOP_COUNTER; }

		public static Counter basicCounter() { return new BasicCounter(); }

		public static Timer newTimer() {
				if (!supportsCPUTime)
						return new CompositeTimer(new NanoTimeMeasure(), NOOP_TIME_MEASURE, NOOP_TIME_MEASURE);
				else
						return new CompositeTimer(new NanoTimeMeasure(), new UserTimeMeasure(), new CpuTimeMeasure());
		}

		public static Timer samplingTimer(int sampleSize){
				int initialSize = sampleSize>100? sampleSize/100: sampleSize;
				return samplingTimer(sampleSize,initialSize);
		}

		public static Timer samplingTimer(int sampleSize,int initialSize){
				if (!supportsCPUTime)
						return new SamplingCompositeTimer(new NanoTimeMeasure(), NOOP_TIME_MEASURE, NOOP_TIME_MEASURE,sampleSize,initialSize);
				else
						return new SamplingCompositeTimer(new NanoTimeMeasure(), new UserTimeMeasure(), new CpuTimeMeasure(),sampleSize,initialSize);
		}

		@ThreadSafe
		public static Timer noOpTimer() { return NOOP_TIMER; }

		@ThreadSafe
		public static MultiTimeView noOpMultiTimeView(){ return NOOP_MULTI_TIME_VIEW;};

		@ThreadSafe
		static TimeMeasure noOpTimeMeasure() { return NOOP_TIME_MEASURE; }

		@ThreadSafe
		public static TimeView noOpTimeView() { return NOOP_TIME_VIEW; }

		public static Gauge maxGauge() { return new FoldGauge(Folders.maxDoubleFolder()); }

		public static Gauge minGauge() { return new FoldGauge(Folders.minDoubleFolder()); }

		@ThreadSafe
		public static Gauge noOpGauge() { return NOOP_GAUGE; }

		/*private helper classes*/
		private static class BasicCounter implements Counter {
				private long count;

				@Override public void increment() { add(1l);	 }
				@Override public void add(long value) { this.count += value; }
				@Override public long getTotal() { return count; }
				@Override public boolean isActive() { return true; }
		}

		private static class FoldGauge implements Gauge {
				private final DoubleFolder folder;
				private double current;

				private FoldGauge(DoubleFolder folder) { this.folder = folder; }
				@Override public void update(double value) { current = folder.fold(current, value); }
				@Override public double getValue() { return current; }
				@Override public boolean isActive() { return true; }
		}

		private static class CpuTimeMeasure extends BaseTimeMeasure {
				@Override protected long getTimestamp() { return threadMXBean.getCurrentThreadCpuTime(); }
		}

		private static class UserTimeMeasure extends BaseTimeMeasure {
				@Override protected long getTimestamp() { return threadMXBean.getCurrentThreadUserTime(); }
		}

		private static class NanoTimeMeasure extends BaseTimeMeasure {
				@Override protected long getTimestamp() { return System.nanoTime(); }
		}

		private static class SamplingMetricFactory implements MetricFactory{
				private final int sampleSize;
				private final int initialSize;

				private SamplingMetricFactory(int sampleSize, int initialSize) {
						this.sampleSize = sampleSize;
						this.initialSize = initialSize;
				}

				@Override public Counter newCounter() { return basicCounter(); }
				@Override public Timer newTimer() { return Metrics.samplingTimer(sampleSize,initialSize); }
				@Override public Gauge newMaxGauge() { return maxGauge(); }
				@Override public Gauge newMinGauge() { return minGauge(); }
				@Override public boolean isActive() { return true; }

		}
		private static class CreatingMetricFactory implements MetricFactory {
				@Override public Counter newCounter() { return basicCounter(); }
				@Override public Timer newTimer() { return Metrics.newTimer(); }
				@Override public Gauge newMaxGauge() { return maxGauge(); }
				@Override public Gauge newMinGauge() { return minGauge(); }
				@Override public boolean isActive() { return true; }
		}

		public static void main(String...args) throws Exception{
				Timer timer = newTimer();
				timer.startTiming();
				Thread.sleep(1000);
				timer.stopTiming();
				TimeView time = timer.getTime();
				System.out.printf("Wall: %d%n" +
								"Cpu: %d%n" +
								"User: %d%n",time.getWallClockTime(),time.getCpuTime(),time.getUserTime());
		}
}
