package com.splicemachine.metrics;

/**
 * @author Scott Fines
 * Date: 1/24/14
 */
public interface IOStats {

		TimeView getTime();

		long getRows();

		long getBytes();
}
