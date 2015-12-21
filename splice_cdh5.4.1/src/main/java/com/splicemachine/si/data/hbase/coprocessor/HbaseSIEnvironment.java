package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.STableFactory;
import com.splicemachine.access.hbase.HBaseTableFactory;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.AsyncReadResolver;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HOperationStatusFactory;
import com.splicemachine.si.impl.CoprocessorTxnStore;
import com.splicemachine.si.impl.HTxnOperationFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.timestamp.hbase.ZkTimestampSource;
import com.splicemachine.utils.GreenLight;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HbaseSIEnvironment implements SIEnvironment{
    private static volatile HbaseSIEnvironment INSTANCE;

    private final TimestampSource timestampSource;
    private final HBaseTableFactory tableFactory;
    private final TxnStore txnStore;
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier<OperationWithAttributes,Cell,Delete,
            Get,Put,RegionScanner,Result,Scan,TableName> ignoreTxnSupplier;
    private final HTxnOperationFactory txnOpFactory;
    private final AsyncReadResolver readResolver;

    public static HbaseSIEnvironment loadEnvironment(RecoverableZooKeeper rzk){
        HbaseSIEnvironment env = INSTANCE;
        if(env==null){
            synchronized(HbaseSIEnvironment.class){
                env = INSTANCE;
                if(env==null){
                    env = INSTANCE = new HbaseSIEnvironment(rzk);
                    SIDriver.loadDriver(INSTANCE);
                }
            }
        }
        return env;
    }

    public static void setEnvironment(HBaseSIEnvironment siEnv){
        INSTANCE = siEnv;
    }

    @SuppressWarnings("unchecked")
    public HBaseSIEnvironment(TimestampSource timestampSource){
        this.timestampSource =timestampSource;
        HBaseTableFactory hBaseTableFactory=new HBaseTableFactory();
        this.tableFactory =hBaseTableFactory;
        this.txnStore = new CoprocessorTxnStore(hBaseTableFactory,timestampSource,null);
        this.txnSupplier = new CompletedTxnCacheSupplier(txnStore,SIConstants.completedTransactionCacheSize,SIConstants.completedTransactionConcurrency);
        this.txnStore.setCache(txnSupplier);
        this.ignoreTxnSupplier = new IgnoreTxnCacheSupplier<>(dataLib(),tableFactory);
        this.txnOpFactory = new HTxnOperationFactory(dataLib(),exceptionFactory());

        this.readResolver = initializeReadResolver();
    }


    @Override public STableFactory tableFactory(){ return tableFactory; }

    @Override
    public ExceptionFactory exceptionFactory(){
        return HExceptionFactory.INSTANCE;
    }

    @Override
    public SConfiguration configuration(){
        return new HConfiguration(SIConstants.config);
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

    public SIDriver getDriver(){
        return SIDriver.driver();
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

    private AsyncReadResolver initializeReadResolver(){
        int readResolverQueueSize=SIConstants.readResolverQueueSize;
        if(readResolverQueueSize<=0) return null; //read resolution is disabled
        //TODO -sf- add in the proper TrafficControl and RollForwardStatus fields
        return new AsyncReadResolver(SIConstants.readResolverThreads,readResolverQueueSize,
                txnSupplier(),new RollForwardStatus(),new GreenLight(),SynchronousReadResolver.INSTANCE);
    }
}
