/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.access.configuration;

import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class DDLConfiguration implements ConfigurationDefault {
    public static final String ERROR_TAG = "[ERROR]";

    public static final String MAX_DDL_WAIT = "splice.ddl.maxWaitSeconds";
    //-sf- tuned down to facilitate testing. Tune up if it's causing problems
    private static final long DEFAULT_MAX_DDL_WAIT=TimeUnit.SECONDS.toMillis(60);

    public static final String DDL_REFRESH_INTERVAL = "splice.ddl.refreshIntervalSeconds";
    private static final long DEFAULT_DDL_REFRESH_INTERVAL=TimeUnit.SECONDS.toMillis(10);

    /**
     * The initial wait in milliseconds when a DDL operation waits for all concurrent transactions to finish before
     * proceeding.
     *
     * The operation will wait progressively longer until the DDL_DRAINING_MAXIMUM_WAIT is reached, then it will
     * block concurrent transactions from writing to the affected tables.
     *
     * Defaults to 1000 (1 second)
     */
    public  static final String DDL_DRAINING_INITIAL_WAIT = "splice.ddl.drainingWait.initial";
    private static final long DEFAULT_DDL_DRAINING_INITIAL_WAIT = 1000;

    /**
     * The maximum wait in milliseconds a DDL operation will wait for concurrent transactions to finish before
     * blocking them from writing to the affected tables.
     *
     * Defaults to 100000 (100 seconds)
     */
    public static final String DDL_DRAINING_MAXIMUM_WAIT = "splice.ddl.drainingWait.maximum";
    private static final long DEFAULT_DDL_DRAINING_MAXIMUM_WAIT = 100000;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.maxDdlWait = configurationSource.getLong(MAX_DDL_WAIT, DEFAULT_MAX_DDL_WAIT);
        builder.ddlRefreshInterval = configurationSource.getLong(DDL_REFRESH_INTERVAL, DEFAULT_DDL_REFRESH_INTERVAL);
        builder.ddlDrainingInitialWait = configurationSource.getLong(DDL_DRAINING_INITIAL_WAIT, DEFAULT_DDL_DRAINING_INITIAL_WAIT);
        builder.ddlDrainingMaximumWait = configurationSource.getLong(DDL_DRAINING_MAXIMUM_WAIT, DEFAULT_DDL_DRAINING_MAXIMUM_WAIT);
    }

}
