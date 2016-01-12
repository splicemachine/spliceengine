package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.SIConfigurations;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.AsyncReadResolver;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HOperationStatusFactory;
import com.splicemachine.si.impl.CoprocessorTxnStore;
import com.splicemachine.si.impl.HTxnOperationFactory;
import com.splicemachine.si.impl.QueuedKeepAliveScheduler;
import com.splicemachine.si.impl.TxnNetworkLayerFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.*;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.timestamp.hbase.ZkTimestampSource;
import com.splicemachine.utils.GreenLight;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HBaseSIEnvironment implements SIEnvironment{
    private static volatile HBaseSIEnvironment INSTANCE;

    private final TimestampSource timestampSource;
    private final PartitionFactory<TableName> tableFactory;
    private final TxnStore txnStore;
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier<OperationWithAttributes,Cell,
            Get, Scan,TableName> ignoreTxnSupplier;
    private final HTxnOperationFactory txnOpFactory;
    private final AsyncReadResolver readResolver;
    private final PartitionInfoCache partitionCache;
    private final KeepAliveScheduler keepAlive;
    private final SConfiguration config;
    private Clock clock;

    public static HBaseSIEnvironment loadEnvironment(Clock clock,RecoverableZooKeeper rzk) throws IOException{
        HBaseSIEnvironment env = INSTANCE;
        if(env==null){
            synchronized(HBaseSIEnvironment.class){
                env = INSTANCE;
                if(env==null){
                    env = INSTANCE = new HBaseSIEnvironment(clock,new ZkTimestampSource(rzk));
                    SIDriver.loadDriver(INSTANCE);
                }
            }
        }
        return env;
    }

    public static void setEnvironment(HBaseSIEnvironment siEnv){
        INSTANCE = siEnv;
    }

    public HBaseSIEnvironment(Clock clock,TimestampSource timestampSource) throws IOException{
        this(timestampSource,clock);
    }

    @SuppressWarnings("unchecked")
    public HBaseSIEnvironment(TimestampSource timestampSource,Clock clock) throws IOException{
        this.timestampSource =timestampSource;
        this.config=new HConfiguration(SIConstants.config,SIConfigurations.defaults);
        this.config.addDefaults(StorageConfiguration.defaults);

        this.tableFactory=TableFactoryService.loadTableFactory(clock,this.config);
        this.partitionCache = PartitionCacheService.loadPartitionCache();
        TxnNetworkLayerFactory txnNetworkLayerFactory= TableFactoryService.loadTxnNetworkLayer();
        this.txnStore = new CoprocessorTxnStore(txnNetworkLayerFactory,timestampSource,null);
        this.txnSupplier = new CompletedTxnCacheSupplier(txnStore,SIConstants.completedTransactionCacheSize,SIConstants.completedTransactionConcurrency);
        this.txnStore.setCache(txnSupplier);
        this.ignoreTxnSupplier = new IgnoreTxnCacheSupplier<>(dataLib(),tableFactory);
        this.txnOpFactory = new HTxnOperationFactory(dataLib(),exceptionFactory());
        this.clock = clock;

        this.readResolver = initializeReadResolver();

        this.keepAlive = new QueuedKeepAliveScheduler(config.getLong(SIConfigurations.TRANSACTION_KEEP_ALIVE_INTERVAL),
                config.getLong(SIConfigurations.TRANSACTION_TIMEOUT),
                config.getInt(SIConfigurations.TRANSACTION_KEEP_ALIVE_THREADS),
                txnStore);
    }


    @Override public PartitionFactory tableFactory(){ return tableFactory; }

    @Override
    public ExceptionFactory exceptionFactory(){
        return HExceptionFactory.INSTANCE;
    }

    @Override
    public SConfiguration configuration(){
        return config;
    }

    @Override public SDataLib dataLib(){ return HDataLib.instance(); }

    @Override
    public TxnStore txnStore(){
        return txnStore;
    }

    @Override
    public TxnSupplier txnSupplier(){
        return txnSupplier;
    }

    @Override
    public IgnoreTxnCacheSupplier ignoreTxnSupplier(){
        return ignoreTxnSupplier;
    }

    @Override
    public OperationStatusFactory statusFactory(){
        return HOperationStatusFactory.INSTANCE;
    }

    @Override
    public TimestampSource timestampSource(){
        return timestampSource;
    }

    public ReadResolver getReadResolver(Partition region){
        if(readResolver==null) return NoOpReadResolver.INSTANCE; //disabled read resolution
        return readResolver.getResolver(region,rollForward());
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
        return SIDriver.driver();
    }

    @Override
    public PartitionInfoCache partitionInfoCache(){
        return partitionCache;
    }

    @Override
    public KeepAliveScheduler keepAliveScheduler(){
        return keepAlive;
    }

    @Override
    public DataFilterFactory filterFactory(){
        return HFilterFactory.INSTANCE;
    }

    @Override
    public Clock systemClock(){
        return clock;
    }

    @Override
    public KeyedReadResolver keyedReadResolver(){
        return SynchronousReadResolver.INSTANCE;
    }

    private AsyncReadResolver initializeReadResolver(){
        int readResolverQueueSize=SIConstants.readResolverQueueSize;
        if(readResolverQueueSize<=0) return null; //read resolution is disabled
        //TODO -sf- add in the proper TrafficControl and RollForwardStatus fields
        return new AsyncReadResolver(SIConstants.readResolverThreads,readResolverQueueSize,
                txnSupplier(),new RollForwardStatus(),new GreenLight(),SynchronousReadResolver.INSTANCE);
    }
}
