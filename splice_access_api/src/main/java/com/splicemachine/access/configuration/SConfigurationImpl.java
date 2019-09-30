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

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The implementation of SConfiguration.
 * <p/>
 * This class contains all splice configuration property values with their getters.<br/>
 * By making all configuration variables <code>final</code>, their access can be "in-lined"
 * by the HotSpot compiler resulting in negligible performance penalties for repeated access.
 * <p/>
 * We retain a reference to the source of configuration because it may contain properties
 * required by systems on which we depend.
 *
 * <h4>Adding Configuration Properties</h4>
 *
 * When adding a new configuration property, it must be added here as a <code>private final</code>
 * field along with a "getter".  Given these fields are <code>final</code>, they must be set in
 * the private {@link #SConfigurationImpl(ConfigurationBuilder, ConfigurationSource) constructor} by
 * the {@link ConfigurationBuilder}. That requires there be a <code>public</code> field of the same
 * type and name in the builder, which is set by a given subsystem {@link ConfigurationDefault} instance.
 * <p/>
 * The property getter should be exposed through the {@link SConfiguration} interface and called by
 * subsystem code to configure it.
 * @see ConfigurationBuilder
 * @see ConfigurationDefault
 */
public final class SConfigurationImpl implements SConfiguration {
    // AuthenticationConfiguration
    private final  boolean authenticationNativeCreateCredentialsDatabase;
    private final  String authentication;
    private final  String authenticationCustomProvider;
    private final  String authenticationLdapSearchauthdn;
    private final  String authenticationLdapSearchauthpw;
    private final  String authenticationLdapSearchbase;
    private final  String authenticationLdapSearchfilter;
    private final  String authenticationLdapServer;
    private final  String authenticationMapGroupAttr;
    private final  String authenticationNativeAlgorithm;
    public int authenticationTokenLength;
    public int authenticationTokenMaxLifetime;
    public int authenticationTokenRenewInterval;
    public boolean authenticationImpersonationEnabled;
    public String authenticationImpersonationUsers;
    public boolean authenticationTokenEnabled;
    public boolean authenticationTokenDebugConnections;
    public int authenticationTokenMaxConnections;
    public int authenticationTokenPermissionCacheSize;

    // Authorization Configuration
    private final String authorizationScheme;
    private final String rangerServiceName;
    private final int sentryPollingInterval;


    // DDLConfiguration
    private final  long ddlDrainingInitialWait;
    private final  long ddlDrainingMaximumWait;
    private final  long ddlRefreshInterval;
    private final  long maxDdlWait;
    private final long mergeRegionTimeout;

    // HConfiguration
    private final  int regionServerHandlerCount;
    private final  int timestampBlockSize;
    private final  long regionLoadUpdateInterval;
    private final  String backupPath;
    private final  String compressionAlgorithm;
    private final  String namespace;
    private final  String spliceRootPath;
    private final  String hbaseSecurityAuthorization;
    private final  boolean hbaseSecurityAuthentication;
    private final  int backupParallelism;
    private final  long backupKeepAliveInterval;
    private final  long backupTimeout;
    private final  long backupMaxBandwidthMB;
    private final  boolean backupUseDistcp;
    private final  int backupIOBufferSize;
    private final  int replicationSnapshotInterval;
    private final  int replicationSinkPort;
    private final int replicationProgressUpdateInterval;

    // OperationConfiguration
    private final  int sequenceBlockSize;
    private final  int threadPoolMaxSize;

    // PipelineConfiguration
    private final  int coreWriterThreads;
    private final  int maxDependentWriteThreads;
    private final  int maxIndependentWriteThreads;
    private final  int maxBufferEntries;
    private final  int maxDependentWrites;
    private final  int maxIndependentWrites;
    private final  int maxRetries;
    private final  int maxWriterThreads;
    private final  int pipelineKryoPoolSize;
    private final  int writeMaxFlushesPerRegion;
    private final  long clientPause;
    private final  long maxBufferHeapSize;
    private final  long startupLockWaitPeriod;
    private final  long threadKeepaliveTime;
    private final  String sparkIoCompressionCodec;
    private final int sparkResultStreamingBatches;
    private final int sparkResultStreamingBatchSize;
    private final int compactionReservedSlots;
    private final int olapCompactionMaximumWait;
    private final int olapCompactionMaximumConcurrent;
    private final double olapCompactionResolutionShare;
    private final double flushResolutionShare;
    private final int olapCompactionResolutionBufferSize;
    private final boolean olapCompactionBlocking;
    private final boolean resolutionOnFlushes;
    private final int reservedSlotsTimeout;
    private final double bulkImportSampleFraction;
    private final int bulkImportTasksPerRegion;
    private final int regionToLoadPerTask;
    private final int rollForwardQueueSize;
    private final int rollForwardFirstWait;
    private final int rollForwardSecondWait;
    private final int rollForwardFirstThreads;
    private final int rollForwardSecondThreads;

    // OLAP client/server configurations
    private final int olapClientWaitTime;
    private final int olapClientTickTime;
    private final int olapServerBindPort;
    private final String olapServerStagingDir;
    private final boolean olapServerExternal;
    private final int olapServerThreads;
    private final int olapServerTickLimit;
    private final int olapClientRetries;
    private final int olapShufflePartitions;
    private final int olapServerSubmitAttempts;
    private final int olapServerMemory;
    private final int olapServerMemoryOverhead;
    private final int olapServerVirtualCores;
    private final String olapLog4jConfig;
    private final Map<String, String> olapServerIsolatedRoles;
    private final Map<String, String> olapServerYarnQueues;
    private final boolean olapServerIsolatedCompaction;
    private final String olapServerIsolatedCompactionQueueName;

    // SIConfigurations
    private final  int activeTransactionCacheSize;
    private final  int completedTxnCacheSize;
    private final  int completedTxnConcurrency;
    private final  int readResolverQueueSize;
    private final  int readResolverThreads;
    private final  int timestampClientWaitTime;
    private final  int timestampServerBindPort;
    private final  int transactionKeepAliveThreads;
    private final  int transactionLockStripes;
    private final  long transactionKeepAliveInterval;
    private final  long transactionTimeout;
    private final boolean ignoreMissingTxns;

    // SQLConfiguration
    private final  boolean debugDumpBindTree;
    private final  boolean debugDumpClassFile;
    private final  boolean debugDumpOptimizedTree;
    private final  boolean debugLogStatementContext;
    private final  boolean ignoreSavePoints;
    private final  boolean upgradeForced;
    private final  int batchOnceBatchSize;
    private final  int importMaxQuotedColumnLines;
    private final  int indexBatchSize;
    private final  int indexLookupBlocks;
    private final  int kryoPoolSize;
    private final  int networkBindPort;
    private final  int partitionserverJmxPort;
    private final  String partitionserverJmxUser;
    private final  String partitionserverJmxPassword;
    private final  int partitionserverPort;
    private final  long broadcastRegionMbThreshold;
    private final  long broadcastRegionRowThreshold;
    private final  long broadcastDatasetCostThreshold;
    private final  long optimizerPlanMaximumTimeout;
    private final  long optimizerPlanMinimumTimeout;
    private final  long determineSparkRowThreshold;
    private final  String networkBindAddress;
    private final  String upgradeForcedFrom;
    private final String storageFactoryHome;
    private final int nestedLoopJoinBatchSize;
    private final long controlExecutionRowLimit;
    private final int maxCheckTableErrors;
    private final int recursiveQueryIterationLimit;
    private String metadataRestrictionEnabled;
    private CompilerContext.NativeSparkModeType nativeSparkAggregationMode;

    // StatsConfiguration
    private final  double fallbackNullFraction;
    private final  double optimizerExtraQualifierMultiplier;
    private final  int cardinalityPrecision;
    private final  int fallbackRowWidth;
    private final  int indexFetchSampleSize;
    private final  int topkSize;
    private final  long fallbackLocalLatency;
    private final  long fallbackMinimumRowCount;
    private final  long fallbackOpencloseLatency;
    private final  long fallbackRegionRowCount;
    private final  long fallbackRemoteLatencyRatio;
    private final  long partitionCacheExpiration;

    // StorageConfiguration
    private final  int splitBlockSize;
    private final  long regionMaxFileSize;
    private final  long tableSplitSleepInterval;
    private final  int splitsPerTableMin;

    // Gateway to hadoop config
    private final ConfigurationSource configSource;

    public ConfigurationSource getConfigSource() {
        return configSource;
    }

    /**
     * List all keys in the configuration which are set (or have a default) and start with the specified prefix.
     *
     * @param prefix the prefix to search for. An empty String or {@code null} will return all keys.
     * @return all keys which start with {@code prefix}
     */
    @Override
    public Map<String, String> prefixMatch(String prefix) {
        return configSource.prefixMatch(prefix);
    }

    // ===========
    // AuthenticationConfiguration
    @Override
    public boolean authenticationNativeCreateCredentialsDatabase() {
        return authenticationNativeCreateCredentialsDatabase;
    }
    @Override
    public String getAuthentication() {
        return authentication;
    }
    @Override
    public String getAuthenticationCustomProvider() {
        return authenticationCustomProvider;
    }
    @Override
    public String getAuthenticationLdapSearchauthdn() {
        return authenticationLdapSearchauthdn;
    }
    @Override
    public String getAuthenticationLdapSearchauthPassword() {
        return authenticationLdapSearchauthpw;
    }
    @Override
    public String getAuthenticationLdapSearchbase() {
        return authenticationLdapSearchbase;
    }
    @Override
    public String getAuthenticationLdapSearchfilter() {
        return authenticationLdapSearchfilter;
    }
    @Override
    public String getAuthenticationLdapServer() {
        return authenticationLdapServer;
    }
    @Override
    public String getAuthenticationMapGroupAttr() { return authenticationMapGroupAttr; }
    @Override
    public String getAuthenticationNativeAlgorithm() {
        return authenticationNativeAlgorithm;
    }
    @Override
    public int getAuthenticationTokenLength() {
        return authenticationTokenLength;
    }
    @Override
    public int getAuthenticationTokenMaxLifetime() {
        return authenticationTokenMaxLifetime;
    }
    @Override
    public int getAuthenticationTokenRenewInterval() {
        return authenticationTokenRenewInterval;
    }
    @Override
    public String getAuthenticationImpersonationUsers() {
        return authenticationImpersonationUsers;
    }
    @Override
    public boolean getAuthenticationImpersonationEnabled() {
        return authenticationImpersonationEnabled;
    }
    @Override
    public boolean getAuthenticationTokenEnabled() {
        return authenticationTokenEnabled;
    }
    @Override
    public boolean getAuthenticationTokenDebugConnections() {
        return authenticationTokenDebugConnections;
    }
    @Override
    public int getAuthenticationTokenMaxConnections() {
        return authenticationTokenMaxConnections;
    }
    @Override
    public int getAuthenticationTokenPermissionCacheSize() {
        return authenticationTokenPermissionCacheSize;
    }

    // DDLConfiguration
    @Override
    public long getDdlDrainingInitialWait() {
        return ddlDrainingInitialWait;
    }
    @Override
    public long getDdlDrainingMaximumWait() {
        return ddlDrainingMaximumWait;
    }
    @Override
    public long getDdlRefreshInterval() {
        return ddlRefreshInterval;
    }
    @Override
    public long getMaxDdlWait() {
        return maxDdlWait;
    }
    @Override
    public long getMergeRegionTimeout() {
        return mergeRegionTimeout;
    }

    // HConfiguration
    @Override
    public int getRegionServerHandlerCount() {
        return regionServerHandlerCount;
    }
    @Override
    public int getTimestampBlockSize() {
        return timestampBlockSize;
    }
    @Override
    public long getRegionLoadUpdateInterval() {
        return regionLoadUpdateInterval;
    }
    @Override
    public String getBackupPath() {
        return backupPath;
    }
    @Override
    public int getBackupParallelism() {
        return backupParallelism;
    }
    @Override
    public long getBackupKeepAliveInterval() {
        return backupKeepAliveInterval;
    }
    @Override
    public long getBackupTimeout() {
        return backupTimeout;
    }
    @Override
    public long getBackupMaxBandwidthMB() {
        return backupMaxBandwidthMB;
    }
    @Override
    public boolean getBackupUseDistcp() {
        return backupUseDistcp;
    }
    @Override
    public int getBackupIOBufferSize() {
        return backupIOBufferSize;
    }
    @Override
    public int getReplicationSnapshotInterval() {
        return replicationSnapshotInterval;
    }
    @Override
    public int getReplicationSinkPort() {
        return replicationSinkPort;
    }
    @Override
    public int getReplicationProgressUpdateInterval() {
        return replicationProgressUpdateInterval;
    }
    @Override
    public String getCompressionAlgorithm() {
        return compressionAlgorithm;
    }
    @Override
    public String getNamespace() {
        return namespace;
    }
    @Override
    public String getSpliceRootPath() {
        return spliceRootPath;
    }
    @Override
    public String getHbaseSecurityAuthorization() {
        return hbaseSecurityAuthorization;
    }
    @Override
    public boolean getHbaseSecurityAuthentication() {
        return hbaseSecurityAuthentication;
    }

    // OperationConfiguration
    @Override
    public int getSequenceBlockSize() {
        return sequenceBlockSize;
    }

    @Override
    public int getThreadPoolMaxSize() {
         return threadPoolMaxSize;
    }

    // PipelineConfiguration
    @Override
    public int getCoreWriterThreads() {
        return coreWriterThreads;
    }
    @Override
    public int getMaxDependentWriteThreads() {
        return maxDependentWriteThreads;
    }
    @Override
    public int getMaxIndependentWriteThreads() {
        return maxIndependentWriteThreads;
    }

    @Override
    public int getMaxBufferEntries() {
        return maxBufferEntries;
    }
    @Override
    public int getMaxDependentWrites() {
        return maxDependentWrites;
    }
    @Override
    public int getMaxIndependentWrites() {
        return maxIndependentWrites;
    }
    @Override
    public int getMaxRetries() {
        return maxRetries;
    }
    @Override
    public int getMaxWriterThreads() {
        return maxWriterThreads;
    }
    @Override
    public int getPipelineKryoPoolSize() {
        return pipelineKryoPoolSize;
    }
    @Override
    public int getWriteMaxFlushesPerRegion() {
        return writeMaxFlushesPerRegion;
    }
    @Override
    public long getClientPause() {
        return clientPause;
    }
    @Override
    public long getMaxBufferHeapSize() {
        return maxBufferHeapSize;
    }
    @Override
    public long getStartupLockWaitPeriod() {
        return startupLockWaitPeriod;
    }
    @Override
    public long getThreadKeepaliveTime() {
        return threadKeepaliveTime;
    }
    @Override
    public String getSparkIoCompressionCodec() {
        return sparkIoCompressionCodec;
    }

    @Override
    public double getBulkImportSampleFraction() {
        return bulkImportSampleFraction;
    }

    @Override
    public int getBulkImportTasksPerRegion() {
        return bulkImportTasksPerRegion;
    }

    @Override
    public int getRegionToLoadPerTask() {
        return regionToLoadPerTask;
    }

    @Override
    public int getSparkResultStreamingBatches() {
        return sparkResultStreamingBatches;
    }

    @Override
    public int getSparkResultStreamingBatchSize() {
        return sparkResultStreamingBatchSize;
    }

    // SIConfigurations
    @Override
    public int getActiveTransactionCacheSize() {
        return activeTransactionCacheSize;
    }
    @Override
    public int getCompletedTxnCacheSize() {
        return completedTxnCacheSize;
    }
    @Override
    public int getCompletedTxnConcurrency() {
        return completedTxnConcurrency;
    }
    @Override
    public int getReadResolverQueueSize() {
        return readResolverQueueSize;
    }
    @Override
    public int getReadResolverThreads() {
        return readResolverThreads;
    }

    @Override
    public int getRollforwardQueueSize() {
        return rollForwardQueueSize;
    }

    @Override
    public int getRollforwardFirstWait() {
        return rollForwardFirstWait;
    }

    @Override
    public int getRollforwardSecondWait() {
        return rollForwardSecondWait;
    }

    @Override
    public int getRollforwardFirstThreads() {
        return rollForwardFirstThreads;
    }

    @Override
    public int getRollforwardSecondThreads() {
        return rollForwardSecondThreads;
    }

    @Override
    public int getOlapClientWaitTime() {
        return olapClientWaitTime;
    }
    @Override
    public int getOlapClientTickTime() {
        return olapClientTickTime;
    }
    @Override
    public int getOlapServerBindPort() {
        return olapServerBindPort;
    }
    @Override
    public String getOlapServerStagingDirectory() {
        return olapServerStagingDir;
    }
    @Override
    public boolean getOlapServerExternal() {
        return olapServerExternal;
    }
    @Override
    public int getOlapServerSubmitAttempts() {
        return olapServerSubmitAttempts;
    }
    @Override
    public String getAuthorizationScheme() {
        return authorizationScheme;
    }
    @Override
    public String getRangerServiceName() {
        return rangerServiceName;
    }
    @Override
    public int getSentryPollingInterval() {
        return sentryPollingInterval;
    }


    @Override
    public int getOlapServerMemory() {
        return olapServerMemory;
    }
    @Override
    public int getOlapServerMemoryOverhead() {
        return olapServerMemoryOverhead;
    }
    @Override
    public int getOlapVirtualCores() {
        return olapServerVirtualCores;
    }
    @Override
    public int getOlapServerThreads() {
        return olapServerThreads;
    }
    @Override
    public int getOlapClientRetries() {
        return olapClientRetries;
    }
    @Override
    public int getOlapShufflePartitions() {
        return olapShufflePartitions;
    }
    @Override
    public int getTimestampClientWaitTime() {
        return timestampClientWaitTime;
    }
    @Override
    public int getTimestampServerBindPort() {
        return timestampServerBindPort;
    }
    @Override
    public int getTransactionKeepAliveThreads() {
        return transactionKeepAliveThreads;
    }
    @Override
    public int getTransactionLockStripes() {
        return transactionLockStripes;
    }
    @Override
    public long getTransactionKeepAliveInterval() {
        return transactionKeepAliveInterval;
    }
    @Override
    public long getTransactionTimeout() {
        return transactionTimeout;
    }
    @Override
    public boolean getIgnoreMissingTxns() {
        return ignoreMissingTxns;
    }

    // SQLConfiguration
    @Override
    public boolean debugDumpBindTree() {
        return debugDumpBindTree;
    }
    @Override
    public boolean debugDumpClassFile() {
        return debugDumpClassFile;
    }
    @Override
    public boolean debugDumpOptimizedTree() {
        return debugDumpOptimizedTree;
    }
    @Override
    public boolean debugLogStatementContext() {
        return debugLogStatementContext;
    }
    @Override
    public boolean ignoreSavePoints() {
        return ignoreSavePoints;
    }
    @Override
    public boolean upgradeForced() {
        return upgradeForced;
    }
    @Override
    public int getBatchOnceBatchSize() {
        return batchOnceBatchSize;
    }
    @Override
    public int getImportMaxQuotedColumnLines() {
        return importMaxQuotedColumnLines;
    }
    @Override
    public int getIndexBatchSize() {
        return indexBatchSize;
    }
    @Override
    public int getIndexLookupBlocks() {
        return indexLookupBlocks;
    }
    @Override
    public int getKryoPoolSize() {
        return kryoPoolSize;
    }
    @Override
    public int getNetworkBindPort() {
        return networkBindPort;
    }
    @Override
    public int getPartitionserverJmxPort() {
        return partitionserverJmxPort;
    }
    @Override
    public String getPartitionserverJmxUser() {
        return partitionserverJmxUser;
    }    @Override
    public String getPartitionserverJmxPassword() {
        return partitionserverJmxPassword;
    }
    @Override
    public int getPartitionserverPort() {
        return partitionserverPort;
    }
    @Override
    public long getBroadcastRegionMbThreshold() {
        return broadcastRegionMbThreshold;
    }
    @Override
    public long getBroadcastRegionRowThreshold() {
        return broadcastRegionRowThreshold;
    }
    @Override
    public long getBroadcastDatasetCostThreshold() {
        return broadcastDatasetCostThreshold;
    }
    @Override
    public long getOptimizerPlanMaximumTimeout() {
        return optimizerPlanMaximumTimeout;
    }
    @Override
    public long getOptimizerPlanMinimumTimeout() {
        return optimizerPlanMinimumTimeout;
    }
    @Override
    public long getDetermineSparkRowThreshold() {  return determineSparkRowThreshold; }
    @Override
    public String getNetworkBindAddress() {
        return networkBindAddress;
    }
    @Override
    public String getUpgradeForcedFrom() {
        return upgradeForcedFrom;
    }
    @Override
    public int getNestedLoopJoinBatchSize() {
        return nestedLoopJoinBatchSize;
    }
    @Override
    public int getRecursiveQueryIterationLimit() {
        return recursiveQueryIterationLimit;
    }
    @Override
    public String getMetadataRestrictionEnabled() {
        return metadataRestrictionEnabled;
    }

    // StatsConfiguration
    @Override
    public double getFallbackNullFraction() {
        return fallbackNullFraction;
    }
    @Override
    public double getOptimizerExtraQualifierMultiplier() {
        return optimizerExtraQualifierMultiplier;
    }
    @Override
    public int getCardinalityPrecision() {
        return cardinalityPrecision;
    }
    @Override
    public int getFallbackRowWidth() {
        return fallbackRowWidth;
    }
    @Override
    public int getIndexFetchSampleSize() {
        return indexFetchSampleSize;
    }
    @Override
    public int getTopkSize() {
        return topkSize;
    }
    @Override
    public long getFallbackLocalLatency() {
        return fallbackLocalLatency;
    }
    @Override
    public long getFallbackMinimumRowCount() {
        return fallbackMinimumRowCount;
    }
    @Override
    public long getFallbackOpencloseLatency() {
        return fallbackOpencloseLatency;
    }
    @Override
    public long getFallbackRegionRowCount() {
        return fallbackRegionRowCount;
    }
    @Override
    public long getFallbackRemoteLatencyRatio() {
        return fallbackRemoteLatencyRatio;
    }
    @Override
    public long getPartitionCacheExpiration() {
        return partitionCacheExpiration;
    }
    @Override
    public String getStorageFactoryHome() { return storageFactoryHome;}

    // StorageConfiguration
    @Override
    public int getSplitBlockSize() {
        return splitBlockSize;
    }
    @Override
    public long getRegionMaxFileSize() {
        return regionMaxFileSize;
    }
    @Override
    public long getTableSplitSleepInterval() {
        return tableSplitSleepInterval;
    }
    @Override
    public int getSplitsPerTableMin() {
        return splitsPerTableMin;
    }

    // ===========

    /**
     * Constructor
     * <p/>
     * Final fields in this class require that the be set here, which requires that they exist
     * in the {@link ConfigurationBuilder builder}.
     * @param builder the set of all properties set by all {@link ConfigurationDefault}s
     * @param configurationSource the original source of configuration properties that may have
     *                            default values overridden. We keep a reference to this source
     *                            because it contains other configuration properties besides
     *                            Splice properties and it may be needed by systems on which we
     *                            depend.
     */
    SConfigurationImpl(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        configSource = configurationSource;
        activeTransactionCacheSize = builder.activeTransactionCacheSize;
        completedTxnCacheSize = builder.completedTxnCacheSize;
        completedTxnConcurrency = builder.completedTxnConcurrency;
        readResolverQueueSize = builder.readResolverQueueSize;
        readResolverThreads = builder.readResolverThreads;
        timestampClientWaitTime = builder.timestampClientWaitTime;
        timestampServerBindPort = builder.timestampServerBindPort;
        transactionKeepAliveThreads = builder.transactionKeepAliveThreads;
        transactionLockStripes = builder.transactionLockStripes;
        transactionKeepAliveInterval = builder.transactionKeepAliveInterval;
        transactionTimeout = builder.transactionTimeout;
        sequenceBlockSize = builder.sequenceBlockSize;
        threadPoolMaxSize = builder.threadPoolMaxSize;
        ddlDrainingInitialWait = builder.ddlDrainingInitialWait;
        ddlDrainingMaximumWait = builder.ddlDrainingMaximumWait;
        ddlRefreshInterval = builder.ddlRefreshInterval;
        maxDdlWait = builder.maxDdlWait;
        mergeRegionTimeout = builder.mergeRegionTimeout;
        authenticationNativeCreateCredentialsDatabase = builder.authenticationNativeCreateCredentialsDatabase;
        authentication = builder.authentication;
        authenticationCustomProvider = builder.authenticationCustomProvider;
        authenticationLdapSearchauthdn = builder.authenticationLdapSearchauthdn;
        authenticationLdapSearchauthpw = builder.authenticationLdapSearchauthpw;
        authenticationLdapSearchbase = builder.authenticationLdapSearchbase;
        authenticationLdapSearchfilter = builder.authenticationLdapSearchfilter;
        authenticationLdapServer = builder.authenticationLdapServer;
        authenticationMapGroupAttr = builder.authenticationMapGroupAttr;
        authenticationNativeAlgorithm = builder.authenticationNativeAlgorithm;
        authenticationTokenLength = builder.authenticationTokenLength;
        authenticationTokenRenewInterval = builder.authenticationTokenRenewInterval;
        authenticationTokenEnabled = builder.authenticationTokenEnabled;
        authenticationTokenDebugConnections = builder.authenticationTokenDebugConnections;
        authenticationTokenMaxConnections = builder.authenticationTokenMaxConnections;
        authenticationTokenPermissionCacheSize = builder.authenticationTokenPermissionCacheSize;
        authenticationTokenMaxLifetime = builder.authenticationTokenMaxLifetime;
        authenticationImpersonationEnabled = builder.authenticationImpersonationEnabled;
        authenticationImpersonationUsers = builder.authenticationImpersonationUsers;
        authorizationScheme = builder.authorizationScheme;
        rangerServiceName = builder.rangerServiceName;
        sentryPollingInterval = builder.sentryPollingInterval;
        fallbackNullFraction = builder.fallbackNullFraction;
        optimizerExtraQualifierMultiplier = builder.optimizerExtraQualifierMultiplier;
        cardinalityPrecision = builder.cardinalityPrecision;
        fallbackRowWidth = builder.fallbackRowWidth;
        topkSize = builder.topkSize;
        fallbackLocalLatency = builder.fallbackLocalLatency;
        fallbackMinimumRowCount = builder.fallbackMinimumRowCount;
        fallbackOpencloseLatency = builder.fallbackOpencloseLatency;
        fallbackRegionRowCount = builder.fallbackRegionRowCount;
        fallbackRemoteLatencyRatio = builder.fallbackRemoteLatencyRatio;
        partitionCacheExpiration = builder.partitionCacheExpiration;
        splitBlockSize = builder.splitBlockSize;
        regionMaxFileSize = builder.regionMaxFileSize;
        tableSplitSleepInterval = builder.tableSplitSleepInterval;
        splitsPerTableMin = builder.splitsPerTableMin;
        regionServerHandlerCount = builder.regionServerHandlerCount;
        timestampBlockSize = builder.timestampBlockSize;
        regionLoadUpdateInterval = builder.regionLoadUpdateInterval;
        backupPath = builder.backupPath;
        backupParallelism = builder.backupParallelism;
        backupKeepAliveInterval = builder.backupKeepAliveInterval;
        backupTimeout = builder.backupTimeout;
        backupMaxBandwidthMB = builder.backupMaxBandwidthMB;
        backupUseDistcp = builder.backupUseDistcp;
        backupIOBufferSize = builder.backupIOBufferSize;
        replicationSnapshotInterval = builder.replicationSnapshotInterval;
        replicationSinkPort = builder.replicationSinkPort;
        replicationProgressUpdateInterval = builder.replicationProgressUpdateInterval;
        compressionAlgorithm = builder.compressionAlgorithm;
        namespace = builder.namespace;
        spliceRootPath = builder.spliceRootPath;
        hbaseSecurityAuthorization = builder.hbaseSecurityAuthorization;
        hbaseSecurityAuthentication = builder.hbaseSecurityAuthentication;
        debugDumpBindTree = builder.debugDumpBindTree;
        debugDumpClassFile = builder.debugDumpClassFile;
        debugDumpOptimizedTree = builder.debugDumpOptimizedTree;
        debugLogStatementContext = builder.debugLogStatementContext;
        ignoreSavePoints = builder.ignoreSavePoints;
        upgradeForced = builder.upgradeForced;
        importMaxQuotedColumnLines = builder.importMaxQuotedColumnLines;
        indexBatchSize = builder.indexBatchSize;
        indexLookupBlocks = builder.indexLookupBlocks;
        kryoPoolSize = builder.kryoPoolSize;
        networkBindPort = builder.networkBindPort;
        partitionserverJmxPort = builder.partitionserverJmxPort;
        partitionserverJmxUser = builder.partitionserverJmxUser;
        partitionserverJmxPassword = builder.partitionserverJmxPassword;
        partitionserverPort = builder.partitionserverPort;
        broadcastRegionMbThreshold = builder.broadcastRegionMbThreshold;
        broadcastRegionRowThreshold = builder.broadcastRegionRowThreshold;
        broadcastDatasetCostThreshold = builder.broadcastDatasetCostThreshold;
        recursiveQueryIterationLimit = builder.recursiveQueryIterationLimit;
        optimizerPlanMaximumTimeout = builder.optimizerPlanMaximumTimeout;
        optimizerPlanMinimumTimeout = builder.optimizerPlanMinimumTimeout;
        determineSparkRowThreshold = builder.determineSparkRowThreshold;
        networkBindAddress = builder.networkBindAddress;
        upgradeForcedFrom = builder.upgradeForcedFrom;
        coreWriterThreads = builder.coreWriterThreads;
        maxDependentWriteThreads = builder.maxDependentWriteThreads;
        maxIndependentWriteThreads = builder.maxIndependentWriteThreads;
        maxBufferEntries = builder.maxBufferEntries;
        maxDependentWrites = builder.maxDependentWrites;
        maxIndependentWrites = builder.maxIndependentWrites;
        maxRetries = builder.maxRetries;
        maxWriterThreads = builder.maxWriterThreads;
        pipelineKryoPoolSize = builder.pipelineKryoPoolSize;
        writeMaxFlushesPerRegion = builder.writeMaxFlushesPerRegion;
        clientPause = builder.clientPause;
        maxBufferHeapSize = builder.maxBufferHeapSize;
        startupLockWaitPeriod = builder.startupLockWaitPeriod;
        threadKeepaliveTime = builder.threadKeepaliveTime;
        indexFetchSampleSize = builder.indexFetchSampleSize;
        batchOnceBatchSize = builder.batchOnceBatchSize;
        sparkIoCompressionCodec = builder.sparkIoCompressionCodec;
        olapClientWaitTime = builder.olapClientWaitTime;
        olapClientTickTime = builder.olapClientTickTime;
        olapServerBindPort = builder.olapServerBindPort;
        olapServerStagingDir = builder.olapServerStagingDir;
        olapServerExternal = builder.olapServerExternal;
        olapServerThreads = builder.olapServerThreads;
        olapServerTickLimit = builder.olapServerTickLimit;
        olapServerSubmitAttempts = builder.olapServerSubmitAttempts;
        olapServerMemory = builder.olapServerMemory;
        olapServerMemoryOverhead = builder.olapServerMemoryOverhead;
        olapServerVirtualCores = builder.olapServerVirtualCores;
        olapClientRetries = builder.olapClientRetries;
        olapShufflePartitions = builder.olapShufflePartitions;
        sparkResultStreamingBatches = builder.sparkResultStreamingBatches;
        sparkResultStreamingBatchSize = builder.sparkResultStreamingBatchSize;
        compactionReservedSlots = builder.compactionReservedSlots;
        olapCompactionMaximumWait = builder.olapCompactionMaximumWait;
        olapCompactionMaximumConcurrent = builder.olapCompactionMaximumConcurrent;
        olapCompactionResolutionShare = builder.olapCompactionResolutionShare;
        flushResolutionShare = builder.flushResolutionShare;
        olapCompactionResolutionBufferSize = builder.olapCompactionResolutionBufferSize;
        olapCompactionBlocking = builder.olapCompactionBlocking;
        olapLog4jConfig = builder.olapLog4jConfig;
        olapServerIsolatedRoles = builder.olapServerIsolatedRoles;
        olapServerYarnQueues = builder.olapServerYarnQueues;
        olapServerIsolatedCompaction = builder.olapServerIsolatedCompaction;
        olapServerIsolatedCompactionQueueName = builder.olapServerIsolatedCompactionQueueName;
        resolutionOnFlushes = builder.resolutionOnFlushes;
        reservedSlotsTimeout = builder.reservedSlotsTimeout;
        storageFactoryHome = builder.storageFactoryHome;
        nestedLoopJoinBatchSize = builder.nestedLoopJoinBatchSize;
        controlExecutionRowLimit = builder.controlExecutionRowLimit;
        bulkImportSampleFraction = builder.bulkImportSampleFraction;
        bulkImportTasksPerRegion = builder.bulkImportTasksPerRegion;
        regionToLoadPerTask = builder.regionToLoadPerTask;
        ignoreMissingTxns = builder.ignoreMissingTxns;
        maxCheckTableErrors = builder.maxCheckTableErrors;
        rollForwardQueueSize = builder.rollForwardQueueSize;
        rollForwardFirstWait = builder.rollForwardFirstWait;
        rollForwardSecondWait = builder.rollForwardSecondWait;
        rollForwardFirstThreads = builder.rollForwardFirstThreads;
        rollForwardSecondThreads = builder.rollForwardSecondThreads;
        metadataRestrictionEnabled = builder.metadataRestrictionEnabled;
        nativeSparkAggregationMode = builder.nativeSparkAggregationMode;
    }

    private static final Logger LOG = Logger.getLogger("splice.config");
    @Override
    public void traceConfig() {
        for (Map.Entry<String, Object> entry : getConfigMap().entrySet()) {
            if (entry.getKey().toLowerCase().contains("searchauthpw")
                    || entry.getKey().toLowerCase().contains("password"))
                continue;
            LOG.info(String.format(" %s = [%s]", entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public Map<String,Object> getConfigMap() {
        Map<String,Object> config = new TreeMap<>();
        for (Field field : this.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            Object value = null;
            try {
                value = field.get(this);
            } catch (IllegalAccessException e) {
                value = "cannot access";
            }
            config.put(field.getName(), (value == null ? "null" : value));
        }
        for (Map.Entry<String,String> hadoopEntry : getConfigSource().prefixMatch(null).entrySet()) {
            String value = hadoopEntry.getValue();
            config.put(hadoopEntry.getKey(), (value == null ? "null" : value));
        }
        return config;
    }

    @Override
    public int getCompactionReservedSlots() {
        return compactionReservedSlots;
    }

    @Override
    public int getOlapCompactionMaximumWait() {
        return olapCompactionMaximumWait;
    }

    @Override
    public int getOlapCompactionMaximumConcurrent() {
        return olapCompactionMaximumConcurrent;
    }

    @Override
    public double getOlapCompactionResolutionShare() {
        return olapCompactionResolutionShare;
    }

    @Override
    public double getFlushResolutionShare() {
        return flushResolutionShare;
    }

    @Override
    public int getOlapCompactionResolutionBufferSize() {
        return olapCompactionResolutionBufferSize;
    }

    @Override
    public boolean getOlapCompactionBlocking() {
        return olapCompactionBlocking;
    }

    @Override
    public String getOlapLog4jConfig() {
        return olapLog4jConfig;
    }

    @Override
    public Map<String, String> getOlapServerIsolatedRoles() {
        return olapServerIsolatedRoles;
    }

    @Override
    public Map<String, String> getOlapServerYarnQueues() {
        return olapServerYarnQueues;
    }

    @Override
    public boolean getOlapServerIsolatedCompaction() {
        return olapServerIsolatedCompaction;
    }

    @Override
    public String getOlapServerIsolatedCompactionQueueName() {
        return olapServerIsolatedCompactionQueueName;
    }

    @Override
    public boolean getResolutionOnFlushes() {
        return resolutionOnFlushes;
    }

    @Override
    public int getReservedSlotsTimeout() {
        return reservedSlotsTimeout;
    }

    @Override
    public int getOlapServerTickLimit(){
        return olapServerTickLimit;
    }

    @Override
    public long getControlExecutionRowLimit() {
        return controlExecutionRowLimit;
    }

    @Override
    public int getMaxCheckTableErrors() {
        return maxCheckTableErrors;
    }

    @Override
    public void setNativeSparkAggregationMode(CompilerContext.NativeSparkModeType newValue) {
        nativeSparkAggregationMode = newValue;
    }

    @Override
    public CompilerContext.NativeSparkModeType getNativeSparkAggregationMode() {
        return nativeSparkAggregationMode;
    }
}
