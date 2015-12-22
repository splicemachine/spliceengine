package com.splicemachine.derby.hbase;

import com.splicemachine.pipeline.api.StatisticsWatcher;

/**
 * A No-op implementation of a Statistics Watcher
 * @author Scott Fines
 *         Date: 4/7/15
 */
public class NoOpStatisticsWatcher implements StatisticsWatcher{
    public static final StatisticsWatcher INSTANCE = new NoOpStatisticsWatcher();

    private NoOpStatisticsWatcher(){}

    @Override public void rowsWritten(long rowCount){  }
}
