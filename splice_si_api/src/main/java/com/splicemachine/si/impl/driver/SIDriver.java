package com.splicemachine.si.impl.driver;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.ClientTxnLifecycleManager;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;

public class SIDriver {
    private static final SIDriver INSTANCE = new SIDriver();

    public static SIDriver driver(){ return INSTANCE;}

    public static void loadDriver(SIEnvironment env){
        INSTANCE.tableFactory = env.tableFactory();
        INSTANCE.exceptionFactory = env.exceptionFactory();
        INSTANCE.config = env.configuration();
        INSTANCE.dataLib = env.dataLib();
        INSTANCE.txnStore = env.txnStore();
        INSTANCE.operationStatusFactory = env.statusFactory();
        INSTANCE.timestampSource = env.timestampSource();
        INSTANCE.txnSupplier = env.txnSupplier();
        INSTANCE.ignoreTxnSupplier = env.ignoreTxnSupplier();
        INSTANCE.txnOpFactory = env.operationFactory();
        INSTANCE.rollForward = env.rollForward();

        INSTANCE.dataStore = new DataStore(INSTANCE.dataLib,
                SIConstants.SI_NEEDED,
                SIConstants.SI_DELETE_PUT,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                SIConstants.EMPTY_BYTE_ARRAY,
                SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,
                SIConstants.DEFAULT_FAMILY_BYTES);
        //noinspection unchecked
        INSTANCE.transactor = new SITransactor<>(
                INSTANCE.txnSupplier,
                INSTANCE.ignoreTxnSupplier,
                INSTANCE.txnOpFactory,
                INSTANCE.dataStore,
                INSTANCE.operationStatusFactory,
                INSTANCE.exceptionFactory);
        ClientTxnLifecycleManager clientTxnLifecycleManager=new ClientTxnLifecycleManager(INSTANCE.timestampSource,INSTANCE.exceptionFactory);
        clientTxnLifecycleManager.setTxnStore(INSTANCE.txnStore);
        clientTxnLifecycleManager.setKeepAliveScheduler(env.keepAliveScheduler());
        INSTANCE.lifecycleManager =clientTxnLifecycleManager;
    }

    private PartitionFactory tableFactory;
    private ExceptionFactory exceptionFactory;
    private SConfiguration config;
    private SDataLib dataLib;
    private TxnStore txnStore;
    private OperationStatusFactory operationStatusFactory;
    private TimestampSource timestampSource;
    private TxnSupplier txnSupplier;
    private IgnoreTxnCacheSupplier ignoreTxnSupplier;
    private DataStore dataStore;
    private Transactor transactor;
    private TxnOperationFactory txnOpFactory;
    private RollForward rollForward;
    private ReadResolver readResolver;
    private TxnLifecycleManager lifecycleManager;

    public PartitionFactory getTableFactory(){
        return tableFactory;
    }

    public ExceptionFactory getExceptionFactory(){
        return exceptionFactory;
    }

    /**
     * @return the configuration specific to this architecture.
     */
    public SConfiguration getConfiguration(){
        return config;
    }

	public SDataLib getDataLib() {
        return dataLib;
    }

    public TxnStore getTxnStore() {
        return txnStore;
    }

    public TxnSupplier getTxnSupplier(){
        return txnSupplier;
    }

    public OperationStatusFactory getOperationStatusLib() {
        return operationStatusFactory;
    }

    public TimestampSource getTimestampSource() {
        return timestampSource;
    }

    public DataStore getDataStore(){
        return dataStore;
    }

    public Transactor getTransactor(){
        return transactor;
    }

    public TxnOperationFactory getOperationFactory(){
        return txnOpFactory;
    }

    public IgnoreTxnCacheSupplier getIgnoreTxnSupplier(){
        return ignoreTxnSupplier;
    }

    public RollForward getRollForward(){
        return rollForward;
    }

    public ReadResolver getReadResolver(){
        return readResolver;
    }

    public TxnLifecycleManager lifecycleManager(){
        return lifecycleManager;
    }

    public TransactionalRegion transactionalPartition(long conglomId,Partition basePartition){
        if(conglomId>=0){
            return new TxnRegion(basePartition,
                    getRollForward(),
                    getReadResolver(),
                    getTxnSupplier(),
                    getIgnoreTxnSupplier(),
                    getDataStore(),
                    getTransactor(),
                    getOperationFactory());
        }else{
            return new TxnRegion(basePartition,
                    NoopRollForward.INSTANCE,
                    NoOpReadResolver.INSTANCE,
                    getTxnSupplier(),
                    getIgnoreTxnSupplier(),
                    getDataStore(),
                    getTransactor(),
                    getOperationFactory());
        }
    }
}
