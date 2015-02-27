package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.FloatColumnStatistics;
import com.splicemachine.stats.collector.FloatColumnStatsCollector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class RealStatsCollector extends DvdStatsCollector {
    private FloatColumnStatsCollector baseCollector;

    public RealStatsCollector(FloatColumnStatsCollector baseCollector) {
        super(baseCollector);
        this.baseCollector = baseCollector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        baseCollector.update(dataValueDescriptor.getFloat(),count);
    }

    @Override
    protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
        return new RealStats((FloatColumnStatistics)build);
    }
}
