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

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class StorageConfiguration implements ConfigurationDefault {

    /**
     * Amount of time(in milliseconds) taken to wait for a Region split to occur before checking on that
     * split's status during internal Split operations. It is generally not recommended
     * to adjust this setting unless Region splits take an incredibly short or long amount
     * of time to complete.
     *
     * Defaults to 500 ms.
     */
    public static final String TABLE_SPLIT_SLEEP_INTERVAL= "splice.splitWaitInterval";
    public static final long DEFAULT_SPLIT_WAIT_INTERVAL = 500L;

    public static final String REGION_MAX_FILE_SIZE = "hbase.hregion.max.filesize";

    public static final String SPLIT_BLOCK_SIZE = "splice.splitBlockSize";
    public static final int DEFAULT_SPLIT_BLOCK_SIZE=32*1024*1024;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.splitBlockSize = configurationSource.getInt(SPLIT_BLOCK_SIZE, DEFAULT_SPLIT_BLOCK_SIZE);

        builder.tableSplitSleepInterval = configurationSource.getLong(TABLE_SPLIT_SLEEP_INTERVAL, DEFAULT_SPLIT_WAIT_INTERVAL);
    }
}
