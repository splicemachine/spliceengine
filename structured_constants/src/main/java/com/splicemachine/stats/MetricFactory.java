package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface MetricFactory {

		Counter newCounter();

		Timer newTimer();
}
