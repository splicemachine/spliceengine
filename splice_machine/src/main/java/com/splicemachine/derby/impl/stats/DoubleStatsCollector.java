package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.DoubleColumnStatistics;
import com.splicemachine.stats.collector.DoubleColumnStatsCollector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class DoubleStatsCollector extends DvdStatsCollector {
    private DoubleColumnStatsCollector baseCollector;

    public DoubleStatsCollector(DoubleColumnStatsCollector baseCollector) {
        super(baseCollector);
        this.baseCollector = baseCollector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        baseCollector.update(dataValueDescriptor.getDouble(),count);
    }

    @Override
    protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
        return new DoubleStats((DoubleColumnStatistics)build);
    }
}
