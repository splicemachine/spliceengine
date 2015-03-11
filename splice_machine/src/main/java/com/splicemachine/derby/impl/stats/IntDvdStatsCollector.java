package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.IntColumnStatistics;
import com.splicemachine.stats.collector.IntColumnStatsCollector;

/**
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class IntDvdStatsCollector extends DvdStatsCollector {
    private final IntColumnStatsCollector columnStatsCollector;

    public IntDvdStatsCollector(IntColumnStatsCollector columnStatsCollector) {
        super(columnStatsCollector);
        //keep a reference to the Int version to avoid excessive casting
        this.columnStatsCollector = columnStatsCollector;
    }

    @Override public void updateSize(int size) { columnStatsCollector.updateSize(size); }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
       columnStatsCollector.update(dataValueDescriptor.getInt(),count);
    }

    @Override
    protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
        return new IntStats((IntColumnStatistics)build);
    }

}
