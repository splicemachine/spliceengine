/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
