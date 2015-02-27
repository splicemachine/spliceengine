package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.BooleanColumnStatsCollector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class BooleanDvdStatsCollector extends DvdStatsCollector{
    private BooleanColumnStatsCollector baseCollector;

    public BooleanDvdStatsCollector(BooleanColumnStatsCollector baseCollector) {
        super(baseCollector);
        this.baseCollector = baseCollector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        baseCollector.update(dataValueDescriptor.getBoolean(),count);
    }

    @Override
    protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
        return new BooleanStats(baseCollector.build());
    }

    @Override public void updateSize(int size) { baseCollector.updateSize(size); }

}
