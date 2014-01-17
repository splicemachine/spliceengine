package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public class Stats {

		private static final MetricFactory NOOP_FACTORY= new MetricFactory() {
				@Override public Counter newCounter() { return Counters.noOpCounter(); }
				@Override public Timer newTimer() { return Timers.noOpTimer(); }
		};

		private Stats(){}

		public static MetricFactory basicMetricFactory(){
				return new CreatingMetricFactory();
		}

		public static MetricFactory noOpMetricFactory(){
				return NOOP_FACTORY;
		}


		private static class CreatingMetricFactory implements MetricFactory {
				@Override public Counter newCounter() { return Counters.basicCounter(); }
				@Override public Timer newTimer() { return Timers.newTimer(); }
		}
}
