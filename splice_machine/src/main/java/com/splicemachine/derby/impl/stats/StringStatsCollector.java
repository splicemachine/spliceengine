package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public abstract class StringStatsCollector extends DvdStatsCollector{
    private ColumnStatsCollector<String> baseCollector;

    protected StringStatsCollector(ColumnStatsCollector<String> collector) {
        super(collector);
        this.baseCollector = collector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        baseCollector.update(dataValueDescriptor.getString(),count);
    }

    public static StringStatsCollector charCollector(ColumnStatsCollector<String> collector,final int strLen){
        return new StringStatsCollector(collector) {
            @Override
            @SuppressWarnings("unchecked")
            protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
                return new CharStats((ColumnStatistics<String>)build,strLen);
            }
        };
    }

    public static StringStatsCollector varcharCollector(ColumnStatsCollector<String> collector,final int strLen){
        return new StringStatsCollector(collector) {
            @Override
            @SuppressWarnings("unchecked")
            protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
                return new VarcharStats((ColumnStatistics<String>)build,strLen);
            }
        };
    }
}
