package com.splicemachine.derby.utils;

import com.splicemachine.stats.IOStats;

/**
 * @author Scott Fines
 *         Date: 5/13/14
 */
public interface IOStandardIterator<T> extends StandardIterator<T> {

		IOStats getStats();
}
