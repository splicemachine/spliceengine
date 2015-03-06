package com.splicemachine.stats.estimate;

/**
 * A distribution which contains no records. This is primarily useful when
 * a given partition <em>knows</em> that there is no data which matches a given column--e.g.
 * when statistics were not collected for a specific column.
 *
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class EmptyDistribution<T> implements Distribution<T>{
    private static final EmptyDistribution INSTANCE = new EmptyDistribution();

    @SuppressWarnings("unchecked")
    public static <T> Distribution<T> emptyDistribution(){
        //unchecked cast is fine here, because we don't do anything with the type info anyway
        return (EmptyDistribution<T>)INSTANCE;
    }

    @Override public long selectivity(T element) { return 0; }

    @Override public long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop) { return 0; }
}
