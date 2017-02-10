/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.splicemachine.primitives.Bytes;

/**
 * The common (non-hbase-specific) properties that are used by Splice hbase subsystems.
 * <p/>
 * Note that this class is subclassed to provide hbase subsystem-specific properties for
 * which we don't want a compile-time dependency at this level.<br/>
 * Also note that the subclass must be added to the {@link ConfigurationDefaultsList}
 * manually before creating the configuration (see HConfiguration subclass).
 */
public class HBaseConfiguration implements ConfigurationDefault {
    public static final String NAMESPACE = "splice.namespace";
    public static final String DEFAULT_NAMESPACE = "splice";

    /**
     * Path in Zookeeper for storing ongoing backup
     */
    public static final String BACKUP_PATH = "splice.backup_node";
    public static final String DEFAULT_BACKUP_PATH = "/backup";
    public static final byte[] BACKUP_IN_PROGRESS = Bytes.toBytes(false);
    public static final byte[] BACKUP_DONE = Bytes.toBytes(true);

    public static final String HBASE_SECURITY_AUTHENTICATION = "hbase_security_authentication";
    public static final String HBASE_SECURITY_AUTHORIZATION = "hbase_security_authorization";
    public static final String DEFAULT_HBASE_SECURITY_AUTHORIZATION = "simple";


    /**
     * Path in Zookeeper for storing ongoing backup
     */
    public static final String SNOWFLAKE_PATH = "/splice_snowflake";

    /**
     * Path in Zookeeper for coordinating booking tasks in Spark
     */
    public static final String BOOKINGS_PATH = "/spark_bookings";

    /**
     * The Path in zookeeper for manipulating transactional information.
     * Set to [SpliceRootPath]/transactions
     */
    public static final String TRANSACTION_PATH = "/transactions";

    /**
     * The Path in zookeeper for storing the minimum active transaction.
     * Defaults to [TRANSACTION_PATH]/minimum
     */
    public static final String MINIMUM_ACTIVE_PATH = TRANSACTION_PATH+"/minimum";

    /**
     * Path in ZooKeeper for manipulating Conglomerate information.
     * Defaults to /conglomerates
     */
    public static final String CONGLOMERATE_SCHEMA_PATH = "/conglomerates";

    /**
     * Path in ZooKeeper for storing Derby properties information.
     * Defaults to /derbyPropertyPath
     */
    public static final String DERBY_PROPERTY_PATH = "/derbyPropertyPath";

    public static final String DDL_PATH="/ddl";
    public static final String DDL_CHANGE_PATH="/ddlChange";

    /**
     * Location of Startup node in ZooKeeper. The presence of this node
     * indicates whether or not Splice needs to attempt to recreate
     * System tables (i.e. whether or not Splice has been installed and
     * set up correctly).
     * Defaults to /startupPath
     */
    public static final String STARTUP_PATH = "/startupPath";

    /**
     * Location of Leader Election path in ZooKeeper.
     * Defaults to /leaderElection
     */
    public static final String LEADER_ELECTION = "/leaderElection";

    public static final String SPLICE_ROOT_PATH = "splice.root.path";
    public static final String DEFAULT_ROOT_PATH="/splice";

    /**
     * The number of timestamps to 'reserve' at a time in the Timestamp Server.
     * Defaults to 8192
     */
    public static final String TIMESTAMP_BLOCK_SIZE = "splice.timestamp_server.blocksize";
    protected static final int DEFAULT_TIMESTAMP_BLOCK_SIZE = 32768;

    public static final int DEFAULT_JMX_BIND_PORT = 10102;


    public static final String REGION_LOAD_UPDATE_INTERVAL = "splice.statistics.regionLoadUpdateInterval";
    public static final long DEFAULT_REGION_LOAD_UPDATE_INTERVAL = 5;

    protected static final String REGION_MAX_FILE_SIZE = StorageConfiguration.REGION_MAX_FILE_SIZE;
    protected static final String TRANSACTION_LOCK_STRIPES = SIConfigurations.TRANSACTION_LOCK_STRIPES;

    public static final String SPLICE_BACKUP_PARALLELISM = "splice.backup.parallelism";
    public static final int DEFAULT_SPLICE_BACKUP_PARALLELISM = 16;
    /**
     * The Path in zookeeper for storing the maximum reserved timestamp
     * from the ZkTimestampSource implementation.
     * Defaults to /transactions/maxReservedTimestamp
     */
    public static final String MAX_RESERVED_TIMESTAMP_PATH = "/transactions/maxReservedTimestamp";
    public static final List<String> zookeeperPaths = Collections.unmodifiableList(Arrays.asList(
        CONGLOMERATE_SCHEMA_PATH,
        CONGLOMERATE_SCHEMA_PATH+"/__CONGLOM_SEQUENCE",
        DERBY_PROPERTY_PATH,
        CONGLOMERATE_SCHEMA_PATH,
        CONGLOMERATE_SCHEMA_PATH,
        MINIMUM_ACTIVE_PATH,
        TRANSACTION_PATH,
        MAX_RESERVED_TIMESTAMP_PATH,
        DDL_CHANGE_PATH,
        DDL_PATH,
        SNOWFLAKE_PATH,
        BOOKINGS_PATH
    ));

    // Splice Internal Tables
    public static final String TEST_TABLE = "SPLICE_TEST";
    public static final String TRANSACTION_TABLE = "SPLICE_TXN";
    public static final String TENTATIVE_TABLE = "TENTATIVE_DDL";
    public static final String SYSSCHEMAS_CACHE = "SYSSCHEMAS_CACHE";
    public static final String SYSSCHEMAS_INDEX1_ID_CACHE = "SYSSCHEMAS_INDEX1_ID_CACHE";
    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    @SuppressFBWarnings(value = "MS_MUTABLE_ARRAY",justification = "Intentional")
    public static final byte[] TRANSACTION_TABLE_BYTES = Bytes.toBytes(TRANSACTION_TABLE);

    /**
     * The type of compression to use when compressing Splice Tables. This is set the same way
     * HBase sets table compression, and has the same codecs available to it (GZIP,Snappy, or
     * LZO depending on what is installed).
     *
     * Defaults to none
     */
    public static final String COMPRESSION_ALGORITHM = "splice.compression";


    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        // override in HConfiguration
        builder.timestampBlockSize = configurationSource.getInt(TIMESTAMP_BLOCK_SIZE, DEFAULT_TIMESTAMP_BLOCK_SIZE);

        builder.regionLoadUpdateInterval = configurationSource.getLong(REGION_LOAD_UPDATE_INTERVAL, DEFAULT_REGION_LOAD_UPDATE_INTERVAL);

        builder.spliceRootPath = configurationSource.getString(SPLICE_ROOT_PATH, DEFAULT_ROOT_PATH);
        builder.namespace = configurationSource.getString(NAMESPACE, DEFAULT_NAMESPACE);
        builder.backupPath = configurationSource.getString(BACKUP_PATH, DEFAULT_BACKUP_PATH);


        builder.hbaseSecurityAuthentication = configurationSource.getBoolean(HBASE_SECURITY_AUTHENTICATION, false);
        builder.hbaseSecurityAuthorization = configurationSource.getString(HBASE_SECURITY_AUTHORIZATION,
                                                                           DEFAULT_HBASE_SECURITY_AUTHORIZATION);

        builder.backupParallelism = configurationSource.getInt(SPLICE_BACKUP_PARALLELISM, DEFAULT_SPLICE_BACKUP_PARALLELISM);

    }
}
