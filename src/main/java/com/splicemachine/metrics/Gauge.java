package com.splicemachine.metrics;

/**
 * @author Scott Fines
 * Date: 1/23/14
 */
public interface Gauge {

		public void update(double value);

		public double getValue();

		public boolean isActive();
}
