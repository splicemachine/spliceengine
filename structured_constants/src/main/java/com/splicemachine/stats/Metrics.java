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
				supportsCPUTime = threadMXBean.isCurrentThreadCpuTimeSupported();
		}

		private static final MetricFactory NOOP_FACTORY = new MetricFactory() {
				@Override public Counter newCounter() { return NOOP_COUNTER; }
				@Override public Timer newTimer() { return NOOP_TIMER; }
				@Override public Gauge newMaxGauge() { return NOOP_GAUGE; }
				@Override public Gauge newMinGauge() { return NOOP_GAUGE; }
		};

		private static final IOStats NOOP_IO = new IOStats() {
				@Override public TimeView getTime() { return NOOP_TIME_VIEW; }
				@Override public long getRows() { return 0; }
				@Override public long getBytes() { return 0; }
		};

		private static final TimeMeasure NOOP_TIME_MEASURE = new TimeMeasure() {
				@Override public void startTime() { }
				@Override public void stopTime() { }
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
				@Override public void stopTiming() { }
				@Override public void tick(long numEvents) { }
				@Override public long getNumEvents() { return 0; }
				@Override public TimeView getTime() { return NOOP_TIME_VIEW; }
		};

		private static Counter NOOP_COUNTER = new Counter() {
				@Override public void add(long value) { }
				@Override public long getTotal() { return 0; }
				@Override public boolean isActive() { return false; }
		};

		private Metrics() { }

		public static MetricFactory basicMetricFactory() { return new CreatingMetricFactory(); }

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

		@ThreadSafe
		public static Timer noOpTimer() { return NOOP_TIMER; }

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

		private static class CreatingMetricFactory implements MetricFactory {
				@Override public Counter newCounter() { return basicCounter(); }
				@Override public Timer newTimer() { return Metrics.newTimer(); }
				@Override public Gauge newMaxGauge() { return maxGauge(); }
				@Override public Gauge newMinGauge() { return minGauge(); }
		}
}
