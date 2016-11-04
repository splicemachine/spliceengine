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

package com.splicemachine.si;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.SnowflakeFactory;
import com.splicemachine.access.configuration.ConfigurationBuilder;
import com.splicemachine.access.configuration.HConfigurationDefaultsList;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.ConcurrentTicker;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
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

    public MemSIEnvironment(PartitionFactory tableFactory){
       this(tableFactory,new ConcurrentTicker(0l));
    }

    public MemSIEnvironment(PartitionFactory tableFactory,Clock clock){
        this.tableFactory = tableFactory;
        this.txnStore = new MemTxnStore(clock,tsSource,exceptionFactory,1000);
        this.config=new ConfigurationBuilder().build(new HConfigurationDefaultsList(), new ReflectingConfigurationSource());
        this.opFactory = new MOperationFactory(clock);
        this.txnOpFactory = new SimpleTxnOperationFactory(exceptionFactory,opFactory);
        this.kaScheduler = new ManualKeepAliveScheduler(txnStore);
            this.clock = clock;
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
    public RollForward rollForward(){
        return NoopRollForward.INSTANCE;
    }

    @Override
    public TxnOperationFactory operationFactory(){
        return txnOpFactory;
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
    public DistributedFileSystem fileSystem(){
        return fileSystem;
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
}
