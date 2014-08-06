package com.splicemachine.derby.utils;

import com.splicemachine.metrics.IOStats;

/**
 * @author Scott Fines
 *         Date: 5/13/14
 */
public interface IOStandardIterator<T> extends StandardIterator<T> {

		IOStats getStats();
}
