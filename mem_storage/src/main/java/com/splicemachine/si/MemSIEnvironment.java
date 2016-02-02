package com.splicemachine.si;

import com.splicemachine.MapConfiguration;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.ConcurrentTicker;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.si.api.SIConfigurations;
import com.splicemachine.si.api.data.*;
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
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.*;
import com.splicemachine.timestamp.api.TimestampSource;

import java.nio.file.FileSystems;

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
    private final IgnoreTxnCacheSupplier ignoreSupplier;
    private final DataFilterFactory filterFactory = MFilterFactory.INSTANCE;
    private final OperationStatusFactory operationStatusFactory =MOpStatusFactory.INSTANCE;
    private final OperationFactory opFactory = new MOperationFactory(clock);
    private final TxnOperationFactory txnOpFactory = new SimpleTxnOperationFactory(exceptionFactory,opFactory);
    private final KeepAliveScheduler kaScheduler = new ManualKeepAliveScheduler(txnStore);
    private final SConfiguration config;

    private transient SIDriver siDriver;
    private final DistributedFileSystem fileSystem = new MemFileSystem(FileSystems.getDefault().provider());

    public MemSIEnvironment(){
        this(new MTxnPartitionFactory(new MPartitionFactory()));
    }

    public MemSIEnvironment(PartitionFactory tableFactory){
        this.tableFactory = tableFactory;
        this.config=new MapConfiguration();
        config.addDefaults(SIConfigurations.defaults);
        this.ignoreSupplier = new IgnoreTxnCacheSupplier(opFactory,tableFactory);
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
    public IgnoreTxnCacheSupplier ignoreTxnSupplier(){
        return ignoreSupplier;
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
        throw new UnsupportedOperationException("IMPLEMENT");
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
