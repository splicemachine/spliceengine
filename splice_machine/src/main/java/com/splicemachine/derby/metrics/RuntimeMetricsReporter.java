package com.splicemachine.derby.metrics;

/**
 * @author Scott Fines
 * Date: 1/16/14
 */
public interface RuntimeMetricsReporter {

		public void start();

		public void shutdown();

		public void reportMetrics(OperationRuntimeStats stats);
}
