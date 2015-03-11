package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.ShortColumnStatistics;
import com.splicemachine.stats.collector.ShortColumnStatsCollector;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class SmallintStatsCollector extends DvdStatsCollector {
    private ShortColumnStatsCollector baseCollector;

    public SmallintStatsCollector(ShortColumnStatsCollector baseCollector) {
        super(baseCollector);
        this.baseCollector = baseCollector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        baseCollector.update(dataValueDescriptor.getShort(),count);
    }

    @Override
    protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
        return new SmallintStats((ShortColumnStatistics)build);
    }
}
