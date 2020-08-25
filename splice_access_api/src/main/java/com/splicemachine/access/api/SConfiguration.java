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

package com.splicemachine.access.api;

import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;

import java.util.Map;

/**
 * The Splice configuration interface.
 * <p/>
 * Rather than provide generic methods to get configuration properties, we provide a method to
 * get each configuration property in which we're interested.
 * <p/>
 * We also retain a reference to the source of configuration because it may contain properties
 * required by systems on which we depend. It can be accessed by {@link #getConfigSource()}.
 * <p/>
 * For adding a new configuration property, see the documentation in the implementation.
 */
public interface SConfiguration {

    /**
     * Get the source of the configuration properties.
     * @return the source of properties, wrapped to eliminate the compile-time dependency.<br/>
     * <b>MUST BE UNWRAPPED TO GET THE ACTUAL SOURCE CONFIGURATION!</b>
     * @see ConfigurationSource#unwrapDelegate()
     */
    ConfigurationSource getConfigSource();

    /**
     * List all keys in the configuration which are set (or have a default) and start with the specified prefix.
     *
     * @param prefix the prefix to search for. An empty String or {@code null} will return all keys.
     * @return all keys which start with {@code prefix}
     */
    Map<String, String> prefixMatch(String prefix);

    // ===========
    // AuthenticationConfiguration
    boolean authenticationNativeCreateCredentialsDatabase();

    String getAuthentication();

    String getAuthenticationCustomProvider();

    String getAuthenticationLdapSearchauthdn();

    String getAuthenticationLdapSearchauthPassword();

    String getAuthenticationLdapSearchbase();

    String getAuthenticationLdapSearchfilter();

    String getAuthenticationLdapServer();

    String getAuthenticationMapGroupAttr();

    String getAuthenticationNativeAlgorithm();

    int getAuthenticationTokenLength();
    int getAuthenticationTokenMaxLifetime();
    int getAuthenticationTokenRenewInterval();
    boolean getAuthenticationImpersonationEnabled();
    String getAuthenticationImpersonationUsers();
    boolean getAuthenticationTokenEnabled();
    boolean getAuthenticationTokenDebugConnections();
    int getAuthenticationTokenMaxConnections();
    int getAuthenticationTokenPermissionCacheSize();

    // DDLConfiguration
    long getDdlDrainingInitialWait();

    long getDdlDrainingMaximumWait();

    long getDdlRefreshInterval();

    long getMaxDdlWait();

    long getMergeRegionTimeout();

    // HConfiguration
    int getRegionServerHandlerCount();

    int getTimestampBlockSize();

    long getRegionLoadUpdateInterval();

    long getTransactionsWatcherUpdateInterval();

    String getBackupPath();

    int getBackupParallelism();

    long getBackupKeepAliveInterval();

    long getBackupTimeout();

    long getBackupMaxBandwidthMB();

    boolean getBackupUseDistcp();

    int getBackupIOBufferSize();

    boolean replicationEnabled();

    public String getReplicationPath();

    int getReplicationSnapshotInterval();

    int getReplicationProgressUpdateInterval();

    String getReplicationMonitorQuorum();

    String getReplicationMonitorPath();

    int getReplicationMonitorInterval();

    String getReplicationHealthcheckScript();

    String getCompressionAlgorithm();

    String getNamespace();

    String getSpliceRootPath();

    String getHbaseSecurityAuthorization();

    boolean getHbaseSecurityAuthentication();

    String getKafkaBootstrapServers();

    // OperationConfiguration
    int getSequenceBlockSize();

    int getThreadPoolMaxSize();

    // PipelineConfiguration
    int getCoreWriterThreads();

    int getMaxDependentWriteThreads();

    int getMaxIndependentWriteThreads();

    int getMaxBufferEntries();

    int getMaxDependentWrites();

    int getMaxIndependentWrites();

    int getMaxRetries();

    int getMaxWriterThreads();

    int getPipelineKryoPoolSize();

    int getWriteMaxFlushesPerRegion();

    long getClientPause();

    long getMaxBufferHeapSize();

    long getStartupLockWaitPeriod();

    long getThreadKeepaliveTime();

    String getSparkIoCompressionCodec();

    int getSparkResultStreamingBatches();
    int getSparkResultStreamingBatchSize();

    int getSparkSlowResultStreamingBatches();
    int getSparkSlowResultStreamingBatchSize();

    double getBulkImportSampleFraction();

    String getAuthorizationScheme();

    String getRangerServiceName();

    int getSentryPollingInterval();

    int getBulkImportTasksPerRegion();

    int getRegionToLoadPerTask();

    // SIConfigurations
    int getActiveTransactionMaxCacheSize();
    int getActiveTransactionInitialCacheSize();

    int getCompletedTxnCacheSize();

    int getCompletedTxnConcurrency();

    int getReadResolverQueueSize();

    int getReadResolverThreads();

    int getRollforwardQueueSize();
    int getRollforwardFirstWait();
    int getRollforwardSecondWait();
    int getRollforwardFirstThreads();
    int getRollforwardSecondThreads();

    int getTimestampClientWaitTime();

    int getTimestampServerBindPort();

    int getTransactionKeepAliveThreads();

    int getTransactionLockStripes();

    long getTransactionKeepAliveInterval();

    long getTransactionTimeout();

    boolean getIgnoreMissingTxns();

    // Olap configurations

    int getOlapClientWaitTime();

    int getOlapClientTickTime();

    int getOlapServerBindPort();

    String getOlapServerStagingDirectory();

    boolean getOlapServerExternal();

    int getOlapServerMaxRetries();

    long getOlapServerKeepAliveTimeout();

    int getOlapServerThreads();

    int getOlapServerSubmitAttempts();

    int getOlapServerMemory();

    int getOlapServerMemoryOverhead();

    int getOlapVirtualCores();

    int getOlapClientRetries();

    int getOlapShufflePartitions();

    boolean getOlapCompactionAutomaticallyPurgeDeletedRows();

    boolean getOlapCompactionAutomaticallyPurgeOldUpdates();

    // SQLConfiguration
    boolean debugDumpBindTree();

    boolean debugDumpClassFile();

    boolean debugDumpOptimizedTree();

    boolean debugLogStatementContext();

    boolean ignoreSavePoints();

    boolean upgradeForced();

    int getImportMaxQuotedColumnLines();

    long getImportCsvScanThreshold();

    int getIndexBatchSize();

    int getIndexLookupBlocks();

    int getKryoPoolSize();

    int getNetworkBindPort();

    int getPartitionserverJmxPort();

    String getPartitionserverJmxUser();

    String getPartitionserverJmxPassword();

    int getPartitionserverPort();

    long getBroadcastRegionMbThreshold();

    long getBroadcastRegionRowThreshold();

    long getBroadcastDatasetCostThreshold();

    long getOptimizerPlanMaximumTimeout();

    long getOptimizerPlanMinimumTimeout();

    long getDetermineSparkRowThreshold();

    String getNetworkBindAddress();

    String getUpgradeForcedFrom();

    String getStorageFactoryHome();

    int getNestedLoopJoinBatchSize();

    // StatsConfiguration
    double getFallbackNullFraction();

    double getOptimizerExtraQualifierMultiplier();

    int getCardinalityPrecision();

    int getFallbackRowWidth();

    int getIndexFetchSampleSize();

    int getTopkSize();

    long getFallbackLocalLatency();

    long getFallbackMinimumRowCount();

    long getFallbackOpencloseLatency();

    long getFallbackRegionRowCount();

    long getFallbackRemoteLatencyRatio();

    long getPartitionCacheExpiration();

    int getCollectSchemaStatisticsMaximumConcurrent();

    // StorageConfiguration
    int getSplitBlockSize();

    long getRegionMaxFileSize();

    long getTableSplitSleepInterval();

    int getSplitsPerRegionMin();

    /**
     * Dump splice configuration, including hadoop config, to the log.
     */
    void traceConfig();

    /**
     * Get all, splice and hadoop, configuration properties
     * @return mapping of key -> value configuration props
     */
    Map<String,Object> getConfigMap();

    int getOlapCompactionMaximumWait();

    int getOlapCompactionMaximumConcurrent();

    double getOlapCompactionResolutionShare();

    double getFlushResolutionShare();

    int getOlapCompactionResolutionBufferSize();

    int getLocalCompactionResolutionBufferSize();

    boolean getOlapCompactionBlocking();

    boolean getResolutionOnFlushes();

    int getOlapServerTickLimit();

    long getControlExecutionRowLimit();

    String getOlapLog4jConfig();

    Map<String, String> getOlapServerIsolatedRoles();
    
    Map<String, String> getOlapServerYarnQueues();

    boolean getOlapServerIsolatedCompaction();

    String getOlapServerIsolatedCompactionQueueName();

    int getMaxCheckTableErrors();

    int getRecursiveQueryIterationLimit();

    void setNativeSparkAggregationMode(CompilerContext.NativeSparkModeType newValue);

    CompilerContext.NativeSparkModeType getNativeSparkAggregationMode();

    String getMetadataRestrictionEnabled();

    String getRangerUserSyncCaseConversion();

    String getOlapServerMode();

    boolean getSparkAccumulatorsEnabled();
}
