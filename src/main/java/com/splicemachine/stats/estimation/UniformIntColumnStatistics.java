package com.splicemachine.stats.estimation;

import com.splicemachine.stats.frequency.FrequentElements;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
class UniformIntColumnStatistics implements IntColumnStatistics{


    @Override
    public int min() {
        return 0;
    }

    @Override
    public int max() {
        return 0;
    }

    @Override
    public long before(int n, boolean includeEnd) {
        return 0;
    }

    @Override
    public long after(int n, boolean includeStart) {
        return 0;
    }

    @Override
    public long between(int start, int stop, boolean includeStart, boolean includeEnd) {
        return 0;
    }

    @Override
    public long equals(int n) {
        return 0;
    }

    @Override
    public Integer minValue() {
        return null;
    }

    @Override
    public long minCount() {
        return 0;
    }

    @Override
    public Integer maxValue() {
        return null;
    }

    @Override
    public long maxCount() {
        return 0;
    }

    @Override
    public long between(Integer start, Integer stop, boolean includeMin, boolean includeMax) {
        return 0;
    }

    @Override
    public long equals(Integer value) {
        return 0;
    }

    @Override
    public long nullCount() {
        return 0;
    }

    @Override
    public long nonNullCount() {
        return 0;
    }

    @Override
    public FrequentElements<Integer> mostFrequentElements() {
        return null;
    }

    @Override
    public long cardinality() {
        return 0;
    }

    @Override
    public float duplicateFactor() {
        return 0;
    }

    @Override
    public int averageSize() {
        return 0;
    }
}
