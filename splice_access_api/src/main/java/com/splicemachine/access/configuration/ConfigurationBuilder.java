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

import com.splicemachine.access.api.SConfiguration;

/**
 * A builder containing all Splice subsystem properties that can be used to instantiate an {@link SConfiguration}.
 * <p/>
 * When adding a new configuration property, a public field must be added here in the configuration
 * builder so that it's set in the configuration constructor:
 * {@link SConfigurationImpl#SConfigurationImpl(ConfigurationBuilder, ConfigurationSource)}
 */
public class ConfigurationBuilder {
    // SIConfigurations
    public int activeTransactionCacheSize;
    public int completedTxnCacheSize;
    public int completedTxnConcurrency;
    public int readResolverQueueSize;
    public int readResolverThreads;
    public int timestampClientWaitTime;
    public int timestampServerBindPort;
    public int transactionKeepAliveThreads;
    public int transactionLockStripes;
    public long transactionKeepAliveInterval;
    public long transactionTimeout;

    // OperationConfiguration
    public int sequenceBlockSize;
    public int threadPoolMaxSize;

    // DDLConfiguration
    public long ddlDrainingInitialWait;
    public long ddlDrainingMaximumWait;
    public long ddlRefreshInterval;
    public long maxDdlWait;

    // AuthenticationConfiguration
    public boolean authenticationNativeCreateCredentialsDatabase;
    public String authentication;
    public String authenticationCustomProvider;
    public String authenticationLdapSearchauthdn;
    public String authenticationLdapSearchauthpw;
    public String authenticationLdapSearchbase;
    public String authenticationLdapSearchfilter;
    public String authenticationLdapServer;
    public String authenticationNativeAlgorithm;

    // StatsConfiguration
    public double fallbackNullFraction;
    public double optimizerExtraQualifierMultiplier;
    public int cardinalityPrecision;
    public int fallbackRowWidth;
    public int indexFetchSampleSize;
    public int topkSize;
    public long fallbackLocalLatency;
    public long fallbackMinimumRowCount;
    public long fallbackOpencloseLatency;
    public long fallbackRegionRowCount;
    public long fallbackRemoteLatencyRatio;
    public long partitionCacheExpiration;

    // StorageConfiguration
    public int splitBlockSize;
    public long regionMaxFileSize;
    public long tableSplitSleepInterval;

    // HConfiguration
    public int regionServerHandlerCount;
    public int timestampBlockSize;
    public long regionLoadUpdateInterval;
    public String backupPath;
    public String compressionAlgorithm;
    public String namespace;
    public String spliceRootPath;
    public String hbaseSecurityAuthorization;
    public boolean hbaseSecurityAuthentication;
    public int backupParallelism;

    // SQLConfiguration
    public boolean debugDumpBindTree;
    public boolean debugDumpClassFile;
    public boolean debugDumpOptimizedTree;
    public boolean debugLogStatementContext;
    public boolean ignoreSavePoints;
    public boolean upgradeForced;
    public int batchOnceBatchSize;
    public int importMaxQuotedColumnLines;
    public int indexBatchSize;
    public int indexLookupBlocks;
    public int kryoPoolSize;
    public int networkBindPort;
    public int olapClientWaitTime;
    public int olapClientTickTime;
    public int olapServerBindPort;
    public int olapServerThreads;
    public int olapServerTickLimit;
    public int partitionserverJmxPort;
    public int partitionserverPort;
    public long broadcastRegionMbThreshold;
    public long broadcastRegionRowThreshold;
    public long optimizerPlanMaximumTimeout;
    public long optimizerPlanMinimumTimeout;
    public String networkBindAddress;
    public String upgradeForcedFrom;
    public String storageFactoryHome;
    public int nestedLoopJoinBatchSize;

    // PipelineConfiguration
    public int coreWriterThreads;
    public int ipcThreads;
    public int maxBufferEntries;
    public int maxDependentWrites;
    public int maxIndependentWrites;
    public int maxRetries;
    public int maxWriterThreads;
    public int pipelineKryoPoolSize;
    public int writeMaxFlushesPerRegion;
    public long clientPause;
    public long maxBufferHeapSize;
    public long startupLockWaitPeriod;
    public long threadKeepaliveTime;
    public String sparkIoCompressionCodec;
    public int sparkResultStreamingBatchSize;
    public int sparkResultStreamingBatches;
    public int compactionReservedSlots;
    public int reservedSlotsTimeout;
    public int olapCompactionMaximumWait;
    public int olapClientRetries;

    /**
     * Build the {@link SConfiguration} given the list of subsystem defaults and the configuration source.<br/>
     * When this method returns, this builder can be discarded.
     * @param defaultsList list of subsystem defaults
     * @param configurationSource the source of the configuration properties which may contain property values
     *                            that will override defaults.
     * @return the set of configuration property values that will persist for the life of this VM.
     */
    public SConfiguration build(ConfigurationDefaultsList defaultsList, ConfigurationSource configurationSource) {
        // Lay down the defaults, use the configuration source to overlay the defaults, if they exist,
        // and construct the SConfiguration config.
        for (ConfigurationDefault configurationDefault : defaultsList) {
            configurationDefault.setDefaults(this, configurationSource);
        }
        return new SConfigurationImpl(this, configurationSource);
    }
}
