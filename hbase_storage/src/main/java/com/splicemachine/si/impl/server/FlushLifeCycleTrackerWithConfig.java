package com.splicemachine.si.impl.server;

import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;

public class FlushLifeCycleTrackerWithConfig implements FlushLifeCycleTracker {
    private final FlushLifeCycleTracker flushLifeCycleTracker;
    private final PurgeConfig config;

    public FlushLifeCycleTrackerWithConfig(FlushLifeCycleTracker flushLifeCycleTracker, PurgeConfig config) {
       this.flushLifeCycleTracker = flushLifeCycleTracker;
       this.config = config;
    }

    public void notExecuted(String reason) {
        flushLifeCycleTracker.notExecuted(reason);
    }

    public void beforeExecution() {
        flushLifeCycleTracker.beforeExecution();
    }

    public void afterExecution() {
        flushLifeCycleTracker.afterExecution();
    }

    public PurgeConfig getConfig() {
        return config;
    }
}
