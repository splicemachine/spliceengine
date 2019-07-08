/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

    public static final String MERGE_REGION_WAIT_TIMEOUT = "splice.merge.region.wait.timeout";
    private static final long DEFAULT_MERGE_REGION_WAIT_TIMEOUT = 120000;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.maxDdlWait = configurationSource.getLong(MAX_DDL_WAIT, DEFAULT_MAX_DDL_WAIT);
        builder.ddlRefreshInterval = configurationSource.getLong(DDL_REFRESH_INTERVAL, DEFAULT_DDL_REFRESH_INTERVAL);
        builder.ddlDrainingInitialWait = configurationSource.getLong(DDL_DRAINING_INITIAL_WAIT, DEFAULT_DDL_DRAINING_INITIAL_WAIT);
        builder.ddlDrainingMaximumWait = configurationSource.getLong(DDL_DRAINING_MAXIMUM_WAIT, DEFAULT_DDL_DRAINING_MAXIMUM_WAIT);
        builder.mergeRegionTimeout = configurationSource.getLong(MERGE_REGION_WAIT_TIMEOUT, DEFAULT_MERGE_REGION_WAIT_TIMEOUT);
    }

}
