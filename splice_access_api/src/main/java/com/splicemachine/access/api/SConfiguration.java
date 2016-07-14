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

package com.splicemachine.access.api;

import java.util.Map;

import com.splicemachine.access.configuration.ConfigurationSource;

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

    String getAuthenticationLdapSearchauthpw();

    String getAuthenticationLdapSearchbase();

    String getAuthenticationLdapSearchfilter();

    String getAuthenticationLdapServer();

    String getAuthenticationNativeAlgorithm();

    // DDLConfiguration
    long getDdlDrainingInitialWait();

    long getDdlDrainingMaximumWait();

    long getDdlRefreshInterval();

    long getMaxDdlWait();

    // HConfiguration
    int getRegionServerHandlerCount();

    int getTimestampBlockSize();

    long getRegionLoadUpdateInterval();

    String getBackupPath();

    String getCompressionAlgorithm();

    String getNamespace();

    String getSpliceRootPath();

    String getHbaseSecurityAuthorization();

    boolean getHbaseSecurityAuthentication();

    // OperationConfiguration
    int getSequenceBlockSize();

    // PipelineConfiguration
    int getCoreWriterThreads();

    int getIpcThreads();

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

    // SIConfigurations
    int getActiveTransactionCacheSize();

    int getCompletedTxnCacheSize();

    int getCompletedTxnConcurrency();

    int getReadResolverQueueSize();

    int getReadResolverThreads();

    int getOlapClientWaitTime();

    int getOlapClientTickTime();

    int getOlapServerBindPort();

    int getOlapServerThreads();

    int getTimestampClientWaitTime();

    int getTimestampServerBindPort();

    int getTransactionKeepAliveThreads();

    int getTransactionLockStripes();

    long getTransactionKeepAliveInterval();

    long getTransactionTimeout();

    // SQLConfiguration
    boolean debugDumpBindTree();

    boolean debugDumpClassFile();

    boolean debugDumpOptimizedTree();

    boolean debugLogStatementContext();

    boolean ignoreSavePoints();

    boolean upgradeForced();

    int getBatchOnceBatchSize();

    int getImportMaxQuotedColumnLines();

    int getIndexBatchSize();

    int getIndexLookupBlocks();

    int getKryoPoolSize();

    int getNetworkBindPort();

    int getPartitionserverJmxPort();

    int getPartitionserverPort();

    long getBroadcastRegionMbThreshold();

    long getBroadcastRegionRowThreshold();

    long getOptimizerPlanMaximumTimeout();

    long getOptimizerPlanMinimumTimeout();

    String getNetworkBindAddress();

    String getUpgradeForcedFrom();

    String getStorageFactoryHome();

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

    // StorageConfiguration
    int getSplitBlockSize();

    long getRegionMaxFileSize();

    long getTableSplitSleepInterval();

    /**
     * Dump splice configuration, including hadoop config, to the log.
     */
    void traceConfig();

    /**
     * Get all, splice and hadoop, configuration properties
     * @return mapping of key -> value configuration props
     */
    Map<String,Object> getConfigMap();

    int getCompactionReservedSlots();

    int getReservedSlotsTimeout();
}
