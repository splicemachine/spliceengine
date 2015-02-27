package com.splicemachine.derby.impl.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.IntColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import com.splicemachine.stats.collector.IntColumnStatsCollector;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.IntFrequencyEstimate;
import com.splicemachine.stats.frequency.IntFrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

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
