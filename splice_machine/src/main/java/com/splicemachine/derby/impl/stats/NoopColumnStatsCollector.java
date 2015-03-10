package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class NoopColumnStatsCollector implements ColumnStatsCollector<DataValueDescriptor>{
    private static ColumnStatsCollector<DataValueDescriptor> INSTANCE = new NoopColumnStatsCollector();

    private NoopColumnStatsCollector(){}

    public static ColumnStatsCollector<DataValueDescriptor> collector(){
        return INSTANCE;
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> build() {
        return null;
    }

    @Override public void updateNull() {  }
    @Override public void updateNull(long count) {  }
    @Override public void updateSize(int size) {  }
    @Override public void update(DataValueDescriptor item) {  }
    @Override public void update(DataValueDescriptor item, long count) {  }
}
