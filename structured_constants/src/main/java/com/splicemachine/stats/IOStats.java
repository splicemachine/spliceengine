package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/24/14
 */
public interface IOStats {

		TimeView getTime();

		long getRows();

		long getBytes();
}
