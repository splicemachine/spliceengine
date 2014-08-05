package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface MetricFactory {

		Counter newCounter();

		Timer newTimer();

		Timer newWallTimer();

		//TODO -sf- clean this up?
		Gauge newMaxGauge();

		Gauge newMinGauge();

		boolean isActive();
}
