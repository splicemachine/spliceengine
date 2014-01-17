package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 1/17/14
 */
public interface TimeMeasure {

		void startTime();

		void stopTime();

		long getElapsedTime();
}
