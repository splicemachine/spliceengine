package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 1/23/14
 */
public interface TimeView {
		/**
		 * @return the "Wall clock time". Equivalent to System.nanoTime()-based measurements
		 */
		long getWallClockTime();

		/**
		 * @return the CPU time, or 0 if CPU time is not supported by the underlying JVM
		 */
		long getCpuTime();

		/**
		 * @return the User time, or 0 if User time is not supported by the underlying JVM
		 */
		long getUserTime();

		long getStopWallTimestamp();

		long getStartWallTimestamp();
}
