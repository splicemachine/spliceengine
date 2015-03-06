package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.EmptyDistribution;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.Externalizable;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public abstract class BaseDvdStatistics implements ColumnStatistics<DataValueDescriptor>,Externalizable {
    protected ColumnStatistics baseStats;
    private Distribution<DataValueDescriptor> distribution;

    public BaseDvdStatistics() {
        this.distribution = EmptyDistribution.emptyDistribution();
    }

    public BaseDvdStatistics(ColumnStatistics baseStats) {
        this.baseStats = baseStats;
        this.distribution = newDistribution(baseStats);
    }

    @Override public long cardinality() { return baseStats.cardinality(); }
    @Override public float nullFraction() { return baseStats.nullFraction(); }
    @Override public long nullCount() { return baseStats.nullCount(); }
    @Override public long avgColumnWidth() { return baseStats.avgColumnWidth(); }
    @Override public long nonNullCount() { return baseStats.nonNullCount(); }
    @Override public long minCount() { return baseStats.minCount(); }
    @Override public int columnId() { return baseStats.columnId(); }

    @Override public Distribution<DataValueDescriptor> getDistribution() { return distribution; }

    @Override
    @SuppressWarnings("unchecked")
    public ColumnStatistics<DataValueDescriptor> merge(ColumnStatistics<DataValueDescriptor> other) {
        assert other instanceof BaseDvdStatistics: "Cannot merge statistics of type "+ other.getClass();
        baseStats = (ColumnStatistics)baseStats.merge(((BaseDvdStatistics) other).baseStats);
        return this;
    }

    protected abstract Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats);
}
