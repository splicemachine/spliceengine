package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;

import java.math.BigDecimal;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class NumericStatsCollector extends DvdStatsCollector {
    private ColumnStatsCollector<BigDecimal> baseStats;

    public NumericStatsCollector(ColumnStatsCollector<BigDecimal> baseStats) {
        super(baseStats);
        this.baseStats = baseStats;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        baseStats.update((BigDecimal)dataValueDescriptor.getObject(),count);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
        return new NumericStats((ColumnStatistics<BigDecimal>)build);
    }
}
