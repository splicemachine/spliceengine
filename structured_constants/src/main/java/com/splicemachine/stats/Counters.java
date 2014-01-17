package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public class Counters {

		public static Counter noOpCounter(){
				return NoOpCounter.INSTANCE;
		}

		public static Counter basicCounter(){
				return new BasicCounter();
		}

		private static class NoOpCounter implements Counter{
				private static final NoOpCounter INSTANCE = new NoOpCounter();
				@Override public void add(long value) {  }
				@Override public long getTotal() { return 0; }
				@Override public boolean isActive() { return false; }
		}

		private static class BasicCounter implements Counter{
				private long count;

				@Override
				public void add(long value) {
					this.count+=value;
				}

				@Override public long getTotal() { return count; }

				@Override public boolean isActive() { return true; }
		}
}
