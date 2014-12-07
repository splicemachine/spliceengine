package com.splicemachine.stats.estimation;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface IntColumnStatistics extends OrderedColumnStatistics<Integer>{

    int min();

    int max();

    long before(int n, boolean includeEnd);

    long after(int n, boolean includeStart);

    long between(int start, int stop, boolean includeStart, boolean includeEnd);

    long equals(int n);
}
