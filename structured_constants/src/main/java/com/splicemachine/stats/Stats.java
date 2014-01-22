package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public class Stats {

		private static final MetricFactory NOOP_FACTORY= new MetricFactory() {
				@Override public Counter newCounter() { return Counters.noOpCounter(); }
				@Override public Timer newTimer() { return Timers.noOpTimer(); }
				@Override public Gauge newMaxGauge() { return Gauges.noOpGauge(); }
				@Override public Gauge newMinGauge() { return Gauges.noOpGauge(); }
		};
		private static final IOStats NOOP_IO = new IOStats() {
				@Override public TimeView getTime() { return Timers.noOpTimeView(); }
				@Override public long getRows() { return 0; }
				@Override public long getBytes() { return 0; }
		};

		private Stats(){}

		public static MetricFactory basicMetricFactory(){
				return new CreatingMetricFactory();
		}

		public static MetricFactory noOpMetricFactory(){
				return NOOP_FACTORY;
		}

		public static AtomicTimer atomicTimer(){
				return new AtomicTimer();
		}

		public static IOStats noOpIOStats(){
				return NOOP_IO;
		}


		private static class CreatingMetricFactory implements MetricFactory {
				@Override public Counter newCounter() { return Counters.basicCounter(); }
				@Override public Timer newTimer() { return Timers.newTimer(); }
				@Override public Gauge newMaxGauge() { return Gauges.maxGauge(); }
				@Override public Gauge newMinGauge() { return Gauges.minGauge(); }
		}
}
