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

package com.splicemachine.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.log4j.Logger;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.ConfigurationBuilder;
import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.configuration.HConfigurationDefaultsList;
import com.splicemachine.constants.SpliceConfiguration;

/**
 * This Singleton creates the configuration and provides hbase-specific default properties for
 * the hbase_storage subsystem.
 * <p/>
 * The configuration is created by calling {@link #getConfiguration()}, which creates the "final"
 * {@link SConfiguration} for use by Splice subsystems. This is the preferred method of configuration
 * access.
 * <p/>
 * There is one case where we allow the configuration to be "reset", that is, configuration properties
 * to be overwritten after the initial configuration has been created.  However, {@link #reInit(Configuration)}
 * should be <b>used with extreme caution</b>: any subsystem code that has read the initial configuration
 * will never see the overridden property values but a configuration "dump" will show the new values.
 * That could be very confusing to debug.
 * <p/>
 * There is also a method to get the underlying {@link #unwrapDelegate() Hadoop Configuration} required
 * by everything below us.
 */
public class HConfiguration extends HBaseConfiguration {
    private static final Logger LOG = Logger.getLogger("splice.config");

    private static final String DEFAULT_COMPRESSION = "none";
    private static final String TRANSACTION_LOCK_STRIPES = "splice.transaction.lock.stripes";

    // Splice Default Table Definitions
    public static final Boolean DEFAULT_IN_MEMORY = HColumnDescriptor.DEFAULT_IN_MEMORY;
    public static final Boolean DEFAULT_BLOCKCACHE=HColumnDescriptor.DEFAULT_BLOCKCACHE;
    public static final int DEFAULT_TTL = HColumnDescriptor.DEFAULT_TTL;
    public static final String DEFAULT_BLOOMFILTER = HColumnDescriptor.DEFAULT_BLOOMFILTER;

//    private static final String DEFAULT_MAX_RESERVED_TIMESTAMP_PATH = "/transactions/maxReservedTimestamp";

    private static SConfiguration INSTANCE;
    private HConfiguration() {}

    /**
     * Get the Splice configuration properties.
     * @return the Splice configuration
     */
    public static SConfiguration getConfiguration() {
        return new HConfiguration().init();
    }

    /**
     * This method allows us a chance at overwriting the given backing hadoop configuration
     * after we've set new or overridden properties on the old.<br/>
     * It's expected that this method will not be called any later than primordial boot time.<br/>
     * <b>Note</b> that any component that has used the <code>SConfiguration</code> <i>before</i>
     * this method is called will have already been configured and <i>will not</i> see any
     * changes.
     * <p/>
     * The intent is to:
     * <ol>
     *     <li>get the configuration: <code>Configuration config = HConfiguration.unwrapDelegate();</code></li>
     *     <li>override what's needed: <code>config.setLong("hbase.regionserver.handler.count", 200);</code></li>
     *     <li>make the changes visible: <code>HConfiguration.reloadConfiguration(config);</code></li>
     * </ol>
     *
     * @param resetConfiguration the newly modified hadoop configuration.
     * @return the new splice configuration.
     */
    public static SConfiguration reloadConfiguration(Configuration resetConfiguration) {
        return new HConfiguration().reInit(resetConfiguration);
    }

    /**
     * Get the hadoop config object which will have been setup with the splice default properties.
     * @return the underlying hadoop configuration
     */
    public static Configuration unwrapDelegate() {
        return (Configuration) HConfiguration.getConfiguration().getConfigSource().unwrapDelegate();
    }

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        super.setDefaults(builder, configurationSource);
        // set subsystem specific config params
        builder.regionServerHandlerCount = configurationSource.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
        builder.transactionLockStripes = configurationSource.getInt(TRANSACTION_LOCK_STRIPES, builder.regionServerHandlerCount * 8);

        builder.regionMaxFileSize = configurationSource.getLong(REGION_MAX_FILE_SIZE, HConstants.DEFAULT_MAX_FILE_SIZE);

        builder.compressionAlgorithm = configurationSource.getString(COMPRESSION_ALGORITHM, DEFAULT_COMPRESSION);
    }

    private SConfiguration init() {
        SConfiguration config = INSTANCE;
        if (config == null) {
            synchronized (this) {
                config = INSTANCE;
                if (config == null) {
                    HBaseConfigurationSource configSource = new HBaseConfigurationSource(SpliceConfiguration.create());
                    ConfigurationBuilder builder = new ConfigurationBuilder();
                    // note we're adding "this" ConfigurationDefault to the config here
                    config = builder.build(new HConfigurationDefaultsList().addConfig(this), configSource);
                    INSTANCE = config;
                    LOG.info("Created Splice configuration.");
                    config.traceConfig();
                }
            }
        }
        return config;
    }

    private SConfiguration reInit(Configuration newConfiguration) {
        synchronized (this) {
            HBaseConfigurationSource configSource = new HBaseConfigurationSource(newConfiguration);
            ConfigurationBuilder builder = new ConfigurationBuilder();
            // note we're adding "this" ConfigurationDefault to the config here
            INSTANCE = builder.build(new HConfigurationDefaultsList().addConfig(this), configSource);
            LOG.info("Reloaded Splice configuration.");
            INSTANCE.traceConfig();
        }
        return INSTANCE;
    }

}
