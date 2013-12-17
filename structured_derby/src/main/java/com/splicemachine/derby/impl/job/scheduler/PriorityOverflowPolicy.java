package com.splicemachine.derby.impl.job.scheduler;

/**
 * @author Scott Fines
 * Date: 12/6/13
 */
public interface PriorityOverflowPolicy {

		int adjustPriority(int overflowedPriority);

		public static class DefaultPolicy implements PriorityOverflowPolicy{
				private final int defaultPriority;
				public DefaultPolicy(int defaultPriority) { this.defaultPriority = defaultPriority; }
				@Override public int adjustPriority(int overflowedPriority) { return defaultPriority; }
		}
}
