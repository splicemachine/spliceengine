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
    public static final String DEFAULT_BACKUP_LOCK_PATH = "/backup_lock";

    public static final String REPLICATION_PATH = "splice.replication.path";
    public static final String DEFAULT_REPLICATION_PATH = "/replication";
    public static final String DEFAULT_REPLICATION_SOURCE_PATH = "/source";
    public static final String DEFAULT_REPLICATION_PEER_PATH = "/peerId";

    public static final byte[] REPLICATION_PRIMARY = Bytes.toBytes("PRIMARY");
    public static final byte[] REPLICATION_REPLICA = Bytes.toBytes("REPLICA");
    public static final byte[] REPLICATION_NONE = Bytes.toBytes("NONE");

    public static final byte[] BACKUP_IN_PROGRESS = Bytes.toBytes(false);
    public static final byte[] BACKUP_DONE = Bytes.toBytes(true);

    public static final String HBASE_SECURITY_AUTHENTICATION = "hbase_security_authentication";
    public static final String HBASE_SECURITY_AUTHORIZATION = "hbase_security_authorization";
    public static final String DEFAULT_HBASE_SECURITY_AUTHORIZATION = "simple";

    public static final byte[] BULKLOAD_TASK_KEY = Bytes.toBytes("SPLICE_BULKLOAD_SOURCE_TASK");

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
     * The Path in zookeeper for olap server coordination.
     */
    public static final String OLAP_SERVER_PATH = "/olapServer";

    /**
     * The Path in zookeeper for olap server leader election.
     */
    public static final String OLAP_SERVER_LEADER_ELECTION_PATH = "/leaderElection";

    /**
     * The Path in zookeeper for olap server queues.
     */
    public static final String OLAP_SERVER_QUEUE_PATH = "/queues";

    /**
     * The Path in zookeeper for olap server keep alive.
     */
    public static final String OLAP_SERVER_KEEP_ALIVE_PATH = "/keepAlive";

    /**
     * The Path in zookeeper for olap server diagnostics.
     */
    public static final String OLAP_SERVER_DIAGNOSTICS_PATH = "/diagnostics";

    /**
     * The Path in zookeeper for coordinating concurrent HMasters booting up
     */
    public static final String MASTER_INIT_PATH = "/masterInitialization";

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

    /**
     * Path in ZooKeeper for registering servers
     */
    public static final String SERVERS_PATH = "/servers";

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

    public static final String OLD_TRANSACTIONS_NODE = "/transactions/v1transactions";
    /**
     * The number of timestamps to 'reserve' at a time in the Timestamp Server.
     * Defaults to 32768
     */
    public static final String TIMESTAMP_BLOCK_SIZE = "splice.timestamp_server.blocksize";
    protected static final int DEFAULT_TIMESTAMP_BLOCK_SIZE = 32768;

    public static final int DEFAULT_JMX_BIND_PORT = 10102;


    public static final String REGION_LOAD_UPDATE_INTERVAL = "splice.statistics.regionLoadUpdateInterval";
    public static final long DEFAULT_REGION_LOAD_UPDATE_INTERVAL = 900;

    public static final String TRANSACTIONS_WATCHER_UPDATE_INTERVAL = "splice.txn.watcherUpdateInterval";
    public static final long DEFAULT_TRANSACTIONS_WATCHER_UPDATE_INTERVAL = 30;

    protected static final String REGION_MAX_FILE_SIZE = StorageConfiguration.REGION_MAX_FILE_SIZE;
    protected static final String TRANSACTION_LOCK_STRIPES = SIConfigurations.TRANSACTION_LOCK_STRIPES;

    public static final String SPLICE_BACKUP_PARALLELISM = "splice.backup.parallelism";
    public static final int DEFAULT_SPLICE_BACKUP_PARALLELISM = 16;

    public static final String SPLICE_BACKUP_KEEPALIVE_INTERVAL = "splice.backup.keepAliveIntervalMs";
    public static final long DEFAULT_SPLICE_BACKUP_KEEPALIVE_INTERVAL = 3000L;

    public static final String SPLICE_BACKUP_TIMEOUT = "splice.backup.timeout";
    public static final long DEFAULT_SPLICE_BACKUP_TIMEOUT = 10 * DEFAULT_SPLICE_BACKUP_KEEPALIVE_INTERVAL;

    public static final String SPLICE_BACKUP_MAX_BANDWIDTH_MB = "splice.backup.max.bandwidth.mb";
    public static final long DEFAULT_SPLICE_BACKUP_MAX_BANDWIDTH_MB = 100;

    public static final String SPLICE_BACKUP_USE_DISTCP = "splice.backup.use.distcp";
    public static final boolean DEFAULT_SPLICE_USE_DISTCP = false;

    public static final String SPLICE_BACKUP_IO_BUFFER_SIZE = "splice.backup.io.buffer.size";
    public static final int DEFAULT_SPLICE_BACKUP_IO_BUFFER_SIZE = 64*1024;

    public static final String SPLICE_REPLICATION_ENABLED = "splice.replication.enabled";
    public static final boolean DEFAULT_SPLICE_REPLICATION_ENABLED = false;

    public static final String SPLICE_REPLICATION_SNAPSHOT_INTERVAL = "splice.replication.snapshot.interval";
    public static final int DEFAULT_SPLICE_REPLICATION_SNAPSHOT_INTERVAL = 1000;

    public static final String SPLICE_REPLICATION_PROGRESS_UPDATE_INTERVAL = "splice.replication.progress.update.interval";
    public static final int DEFAULT_SPLICE_REPLICATION_PROGRESS_UPDATE_INTERVAL = 200;

    public static final String SPLICE_REPLICATION_MONITOR_QUORUM = "splice.replication.monitor.quorum";

    public static final String SPLICE_REPLICATION_MONITOR_PATH = "splice.replication.monitor.path";
    public static final String DEFAULT_SPLICE_REPLICATION_MONITOR_PATH = "/splice/replication";

    public static final String SPLICE_REPLICATION_MONITOR_INTERVAL = "splice.replication.monitor.interval";
    public static final int DEFAULT_SPLICE_REPLICATION_MONITOR_INTERVAL = 1000;

    public static final String SPLICE_REPLICATION_HEALTHCHECKSCRIPT = "splice.replication.healthcheck.script";

    public static final String KAFKA_BOOTSTRAP_SERVERS = "splice.kafka.bootstrapServers";
    public static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

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
            BOOKINGS_PATH,
            DEFAULT_BACKUP_PATH,
            DEFAULT_BACKUP_LOCK_PATH,
            DEFAULT_REPLICATION_PATH
    ));

    // Splice Internal Tables
    public static final String TEST_TABLE = "SPLICE_TEST";
    public static final String TRANSACTION_TABLE = "SPLICE_TXN";
    public static final String TENTATIVE_TABLE = "TENTATIVE_DDL";
    public static final String SYSSCHEMAS_CACHE = "SYSSCHEMAS_CACHE";
    public static final String SYSSCHEMAS_INDEX1_ID_CACHE = "SYSSCHEMAS_INDEX1_ID_CACHE";
    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    public static final String IGNORE_TXN_TABLE_NAME = "SPLICE_IGNORE_TXN";
    public static final String DROPPED_CONGLOMERATES_TABLE_NAME = "DROPPED_CONGLOMERATES";
    public static final String MASTER_SNAPSHOTS_TABLE_NAME = "SPLICE_MASTER_SNAPSHOTS";
    public static final String REPLICA_REPLICATION_PROGRESS_TABLE_NAME = "SPLICE_REPLICATION_PROGRESS";
    public static final String REPLICATION_PROGRESS_ROWKEY = "ReplicationProgress";
    public static final byte[] REPLICATION_PROGRESS_ROWKEY_BYTES = Bytes.toBytes("ReplicationProgress");
    public static final byte[] REPLICATION_PROGRESS_TSCOL_BYTES = Bytes.toBytes("Timestamp");
    public static final byte[] REPLICATION_SNAPSHOT_TSCOL_BYTES = Bytes.toBytes("Timestamp");

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
        builder.transactionsWatcherUpdateInterval = configurationSource.getLong(TRANSACTIONS_WATCHER_UPDATE_INTERVAL, DEFAULT_TRANSACTIONS_WATCHER_UPDATE_INTERVAL);

        builder.spliceRootPath = configurationSource.getString(SPLICE_ROOT_PATH, DEFAULT_ROOT_PATH);
        builder.namespace = configurationSource.getString(NAMESPACE, DEFAULT_NAMESPACE);
        builder.backupPath = configurationSource.getString(BACKUP_PATH, DEFAULT_BACKUP_PATH);
        builder.replicationPath = configurationSource.getString(REPLICATION_PATH, DEFAULT_REPLICATION_PATH);

        builder.hbaseSecurityAuthentication = configurationSource.getBoolean(HBASE_SECURITY_AUTHENTICATION, false);
        builder.hbaseSecurityAuthorization = configurationSource.getString(HBASE_SECURITY_AUTHORIZATION,
                                                                           DEFAULT_HBASE_SECURITY_AUTHORIZATION);

        builder.backupParallelism = configurationSource.getInt(SPLICE_BACKUP_PARALLELISM, DEFAULT_SPLICE_BACKUP_PARALLELISM);
        builder.backupKeepAliveInterval = configurationSource.getLong(SPLICE_BACKUP_KEEPALIVE_INTERVAL, DEFAULT_SPLICE_BACKUP_KEEPALIVE_INTERVAL);
        builder.backupTimeout = configurationSource.getLong(SPLICE_BACKUP_TIMEOUT, DEFAULT_SPLICE_BACKUP_TIMEOUT);
        builder.backupMaxBandwidthMB = configurationSource.getLong(SPLICE_BACKUP_MAX_BANDWIDTH_MB, DEFAULT_SPLICE_BACKUP_MAX_BANDWIDTH_MB);
        builder.backupUseDistcp = configurationSource.getBoolean(SPLICE_BACKUP_USE_DISTCP, DEFAULT_SPLICE_USE_DISTCP);
        builder.backupIOBufferSize = configurationSource.getInt(SPLICE_BACKUP_IO_BUFFER_SIZE, DEFAULT_SPLICE_BACKUP_IO_BUFFER_SIZE);
        builder.replicationSnapshotInterval = configurationSource.getInt(SPLICE_REPLICATION_SNAPSHOT_INTERVAL, DEFAULT_SPLICE_REPLICATION_SNAPSHOT_INTERVAL);
        builder.replicationProgressUpdateInterval = configurationSource.getInt(SPLICE_REPLICATION_PROGRESS_UPDATE_INTERVAL, DEFAULT_SPLICE_REPLICATION_PROGRESS_UPDATE_INTERVAL);
        builder.replicationMonitorPath =  configurationSource.getString(SPLICE_REPLICATION_MONITOR_PATH, DEFAULT_SPLICE_REPLICATION_MONITOR_PATH);
        builder.replicationMonitorQuorum = configurationSource.getString(SPLICE_REPLICATION_MONITOR_QUORUM, null);
        builder.replicationEnabled = configurationSource.getBoolean(SPLICE_REPLICATION_ENABLED, DEFAULT_SPLICE_REPLICATION_ENABLED);
        builder.replicationMonitorInterval = configurationSource.getInt(SPLICE_REPLICATION_MONITOR_INTERVAL, DEFAULT_SPLICE_REPLICATION_MONITOR_INTERVAL);
        builder.replicationHealthcheckScript = configurationSource.getString(SPLICE_REPLICATION_HEALTHCHECKSCRIPT, null);
        builder.kafkaBootstrapServers = configurationSource.getString(KAFKA_BOOTSTRAP_SERVERS, System.getProperty(KAFKA_BOOTSTRAP_SERVERS, DEFAULT_KAFKA_BOOTSTRAP_SERVERS));
    }
}
