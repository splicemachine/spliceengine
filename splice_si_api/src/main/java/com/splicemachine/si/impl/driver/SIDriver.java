package com.splicemachine.si.impl.driver;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.SIConfigurations;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.readresolve.AsyncReadResolver;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
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
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.si.impl.txn.SITransactionReadController;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.GreenLight;

public class SIDriver {
    private static SIDriver INSTANCE;

    public static SIDriver driver(){ return INSTANCE;}

    public static void loadDriver(SIEnvironment env){
        INSTANCE = new SIDriver(env);
    }

    private final SITransactionReadController readController;
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
    private TxnLifecycleManager lifecycleManager;
    private DataFilterFactory filterFactory;
    private Clock clock;
    private AsyncReadResolver readResolver;

    public SIDriver(SIEnvironment env){
        this.tableFactory = env.tableFactory();
        this.exceptionFactory = env.exceptionFactory();
        this.config = env.configuration();
        this.dataLib = env.dataLib();
        this.txnStore = env.txnStore();
        this.operationStatusFactory = env.statusFactory();
        this.timestampSource = env.timestampSource();
        this.txnSupplier = env.txnSupplier();
        this.ignoreTxnSupplier = env.ignoreTxnSupplier();
        this.txnOpFactory = env.operationFactory();
        this.rollForward = env.rollForward();
        this.filterFactory = env.filterFactory();
        this.clock = env.systemClock();

        this.dataStore = new DataStore(env.dataLib(),
                SIConstants.SI_NEEDED,
                SIConstants.SI_DELETE_PUT,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                SIConstants.EMPTY_BYTE_ARRAY,
                SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,
                SIConstants.DEFAULT_FAMILY_BYTES);
        //noinspection unchecked
        this.transactor = new SITransactor<>(
                this.txnSupplier,
                this.ignoreTxnSupplier,
                this.txnOpFactory,
                this.dataStore,
                this.operationStatusFactory,
                this.exceptionFactory);
        ClientTxnLifecycleManager clientTxnLifecycleManager=new ClientTxnLifecycleManager(this.timestampSource,env.exceptionFactory());
        clientTxnLifecycleManager.setTxnStore(this.txnStore);
        clientTxnLifecycleManager.setKeepAliveScheduler(env.keepAliveScheduler());
        this.lifecycleManager =clientTxnLifecycleManager;
        readController = new SITransactionReadController(dataStore,txnSupplier,ignoreTxnSupplier);
        readResolver = initializedReadResolver(config,env.keyedReadResolver());
    }


    public TransactionReadController readController(){
        return readController;
    }

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

    public ReadResolver getReadResolver(Partition basePartition){
        return readResolver.getResolver(basePartition,getRollForward());
    }

    public TxnLifecycleManager lifecycleManager(){
        return lifecycleManager;
    }

    public DataFilterFactory filterFactory(){
        return filterFactory;
    }

    public TransactionalRegion transactionalPartition(long conglomId,Partition basePartition){
        if(conglomId>=0){
            return new TxnRegion(basePartition,
                    getRollForward(),
                    getReadResolver(basePartition),
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

    public Clock getClock(){
        return clock;
    }

    public DistributedFileSystem fileSystem(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    private AsyncReadResolver initializedReadResolver(SConfiguration config,KeyedReadResolver keyedResolver){
        int maxThreads = config.getInt(SIConfigurations.READ_RESOLVER_THREADS);
        int bufferSize = config.getInt(SIConfigurations.READ_RESOLVER_QUEUE_SIZE);
        final AsyncReadResolver asyncReadResolver=new AsyncReadResolver(maxThreads,
                bufferSize,
                txnSupplier,
                new RollForwardStatus(),
                GreenLight.INSTANCE,keyedResolver);
        asyncReadResolver.start();
        return asyncReadResolver;
    }
}
