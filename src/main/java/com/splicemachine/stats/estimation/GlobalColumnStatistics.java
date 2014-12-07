package com.splicemachine.stats.estimation;

import com.splicemachine.stats.frequency.FrequentElements;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public class GlobalColumnStatistics<T> implements ColumnStatistics<T> {
    private final long nullCount;
    private final long nonNullCount;
    private final long cardinality;
    private final int averageColumnSize;

    private final FrequentElements<T> frequencyEstimates;

    public GlobalColumnStatistics(long nullCount,
                                  long nonNullCount,
                                  long cardinality,
                                  int averageColumnSize,
                                  FrequentElements<T> frequencyEstimates) {
        this.nullCount = nullCount;
        this.nonNullCount = nonNullCount;
        this.cardinality = cardinality;
        this.averageColumnSize = averageColumnSize;
        this.frequencyEstimates = frequencyEstimates;
    }

    @Override public long nullCount() { return nullCount; }
    @Override public long nonNullCount() { return nonNullCount; }
    @Override public FrequentElements<T> mostFrequentElements() { return frequencyEstimates; }
    @Override public long cardinality() { return cardinality; }
    @Override public float duplicateFactor() { return (float)cardinality/nonNullCount; }
    @Override public int averageSize() { return averageColumnSize; }
}
