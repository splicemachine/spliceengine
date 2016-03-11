package com.splicemachine.si;

import java.nio.file.FileSystems;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
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
import com.splicemachine.si.impl.MOpStatusFactory;
import com.splicemachine.si.impl.MOperationFactory;
import com.splicemachine.si.impl.MSynchronousReadResolver;
import com.splicemachine.si.impl.ManualKeepAliveScheduler;
import com.splicemachine.si.impl.MemTimestampSource;
import com.splicemachine.si.impl.MemTxnStore;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.MFilterFactory;
import com.splicemachine.storage.MPartitionCache;
import com.splicemachine.storage.MPartitionFactory;
import com.splicemachine.storage.MTxnPartitionFactory;
import com.splicemachine.storage.MemFileSystem;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemSIEnvironment implements SIEnvironment{
    public static volatile MemSIEnvironment INSTANCE;
    private final ExceptionFactory exceptionFactory = MExceptionFactory.INSTANCE;
    private final Clock clock = new ConcurrentTicker(0l);
    private final TimestampSource tsSource = new MemTimestampSource();
    private final TxnStore txnStore = new MemTxnStore(clock,tsSource,exceptionFactory,1000);
    private final PartitionFactory tableFactory;
    private final DataFilterFactory filterFactory = MFilterFactory.INSTANCE;
    private final OperationStatusFactory operationStatusFactory =MOpStatusFactory.INSTANCE;
    private final OperationFactory opFactory = new MOperationFactory(clock);
    private final TxnOperationFactory txnOpFactory = new SimpleTxnOperationFactory(exceptionFactory,opFactory);
    private final KeepAliveScheduler kaScheduler = new ManualKeepAliveScheduler(txnStore);
    private final MPartitionCache partitionCache = new MPartitionCache();
    private final SConfiguration config;

    private transient SIDriver siDriver;
    private final DistributedFileSystem fileSystem = new MemFileSystem(FileSystems.getDefault().provider());

    public MemSIEnvironment(){
        this(new MTxnPartitionFactory(new MPartitionFactory()));
    }

    public MemSIEnvironment(PartitionFactory tableFactory){
        this.tableFactory = tableFactory;
        this.config=new ConfigurationBuilder().build(new HConfigurationDefaultsList(), new ReflectingConfigurationSource());
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
    public OperationFactory baseOperationFactory(){
        return opFactory;
    }
}
