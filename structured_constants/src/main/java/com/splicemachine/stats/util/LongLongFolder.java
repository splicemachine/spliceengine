package com.splicemachine.stats.util;

/**
 * Functions similar to the foldLeft() command present in a functional
 * language, but specifically for longs.
 *
 * @author Scott Fines
 * Date: 1/24/14
 */
public interface LongLongFolder {

		long fold(long previous, long next);
}
