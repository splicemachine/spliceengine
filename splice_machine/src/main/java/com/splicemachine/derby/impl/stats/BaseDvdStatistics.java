package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.Externalizable;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public abstract class BaseDvdStatistics implements ColumnStatistics<DataValueDescriptor>,Externalizable {
    protected ColumnStatistics baseStats;

    public BaseDvdStatistics() { }

    public BaseDvdStatistics(ColumnStatistics baseStats) {
        this.baseStats = baseStats;
    }

    @Override public long cardinality() { return baseStats.cardinality(); }
    @Override public float nullFraction() { return baseStats.nullFraction(); }
    @Override public long nullCount() { return baseStats.nullCount(); }
    @Override public long avgColumnWidth() { return baseStats.avgColumnWidth(); }

    @Override
    @SuppressWarnings("unchecked")
    public ColumnStatistics<DataValueDescriptor> merge(ColumnStatistics<DataValueDescriptor> other) {
        assert other instanceof BaseDvdStatistics: "Cannot merge statistics of type "+ other.getClass();
        baseStats = (ColumnStatistics)baseStats.merge(((BaseDvdStatistics) other).baseStats);
        return this;
    }
}
