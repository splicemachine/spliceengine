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

package com.splicemachine.si;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;

import com.splicemachine.access.api.*;
import com.splicemachine.access.configuration.ConfigurationBuilder;
import com.splicemachine.access.configuration.HConfigurationDefaultsList;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.ConcurrentTicker;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.server.ClusterHealth;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.store.IgnoreTxnSupplier;
import com.splicemachine.storage.*;
import com.splicemachine.timestamp.api.TimestampSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemSIEnvironment implements SIEnvironment{
    @SuppressFBWarnings(value = "UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD",justification = "Referenced outside of the module")
    public static volatile MemSIEnvironment INSTANCE;
    private final ExceptionFactory exceptionFactory = MExceptionFactory.INSTANCE;
    private final Clock clock;
    private final TimestampSource tsSource = new MemTimestampSource();
    private final TxnStore txnStore;
    private final PartitionFactory tableFactory;
    private final OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory;
    private final DataFilterFactory filterFactory = MFilterFactory.INSTANCE;
    private final SnowflakeFactory snowflakeFactory = MSnowflakeFactory.INSTANCE;
    private final OperationStatusFactory operationStatusFactory =MOpStatusFactory.INSTANCE;
    private final OperationFactory opFactory;
    private final TxnOperationFactory txnOpFactory;
    private final KeepAliveScheduler kaScheduler;
    private final MPartitionCache partitionCache = new MPartitionCache();
    private final SConfiguration config;


    private transient SIDriver siDriver;
    private final DistributedFileSystem fileSystem = new MemFileSystem(FileSystems.getDefault().provider());

    public MemSIEnvironment(PartitionFactory tableFactory, Clock clock, SConfiguration config){
        this.tableFactory = tableFactory;
        this.txnStore = new MemTxnStore(clock,tsSource,exceptionFactory,1000);
        this.config=config;
        this.opFactory = new MOperationFactory(clock);
        this.txnOpFactory = new SimpleTxnOperationFactory(exceptionFactory,opFactory);
        this.kaScheduler = new ManualKeepAliveScheduler(txnStore);
        this.clock = clock;
        this.oldestActiveTransactionTaskFactory = new OldestActiveTransactionTaskFactory() {
            @Override
            public GetOldestActiveTransactionTask get(String hostName, int port, long startupTimestamp) throws IOException {
                throw new UnsupportedOperationException("Operation not supported in Mem profile");
            }
        };
    }

    @Override
    public PartitionFactory tableFactory(){
        return tableFactory;
    }

    @Override
    public ExceptionFactory exceptionFactory(){
        return exceptionFactory;
    }

    @Override
    public SConfiguration configuration(){
        return config;
    }

    @Override
    public TxnStore txnStore(){
        return txnStore;
    }

    @Override
    public OperationStatusFactory statusFactory(){
        return operationStatusFactory;
    }

    @Override
    public TimestampSource timestampSource(){
        return tsSource;
    }

    @Override
    public TxnSupplier txnSupplier(){
        return txnStore;
    }

    @Override
    public IgnoreTxnSupplier ignoreTxnSupplier(){
        return null;
    }

    @Override
    public RollForward rollForward(){
        return NoopRollForward.INSTANCE;
    }

    @Override
    public TxnOperationFactory operationFactory(){
        return txnOpFactory;
    }

    @Override
    public OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory(){
        return oldestActiveTransactionTaskFactory;
    }

    @Override
    public SIDriver getSIDriver(){
        if(siDriver==null)
            siDriver = new SIDriver(this);
        return siDriver;
    }

    @Override
    public PartitionInfoCache partitionInfoCache(){
        return partitionCache;
    }

    @Override
    public KeepAliveScheduler keepAliveScheduler(){
        return kaScheduler;
    }

    @Override
    public DataFilterFactory filterFactory(){
        return filterFactory;
    }

    @Override
    public Clock systemClock(){
        return clock;
    }

    @Override
    public KeyedReadResolver keyedReadResolver(){
        return MSynchronousReadResolver.INSTANCE;
    }

    @Override
    public DistributedFileSystem fileSystem(String path) throws IOException, URISyntaxException {
        return fileSystem;
    }

    @Override
    public OperationFactory baseOperationFactory(){
        return opFactory;
    }

    @Override
    public SnowflakeFactory snowflakeFactory() {
        return snowflakeFactory;
    }

    @Override
    public ClusterHealth clusterHealthFactory() {
        return new MClusterHealth();
    }

    @Override
    public FilesystemAdmin filesystemAdmin() {
        throw new UnsupportedOperationException("Operation not supported in Mem profile");
    }

}
