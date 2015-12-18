package com.splicemachine.si;

import com.splicemachine.access.api.STableFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.impl.MOpStatusFactory;
import com.splicemachine.si.impl.MTxnOperationFactory;
import com.splicemachine.si.impl.MemTxnStore;
import com.splicemachine.si.impl.TxnPartition;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.impl.data.light.LDataLib;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.TestTransactionSetup;
import com.splicemachine.storage.*;
import com.splicemachine.timestamp.api.TimestampSource;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MemSITestEnv implements SITestEnv{
    private final SDataLib dataLib = new LDataLib();
    private final ExceptionFactory exceptionFactory = new MExceptionFactory();
    private final Clock clock = new IncrementingClock();
    private final TimestampSource tsSource = new MemTimestampSource();
    private final TxnStore txnStore = new MemTxnStore(clock,tsSource,exceptionFactory,1000);
    private final Partition personPartition = new MPartition("person","person");
    private final STableFactory tableFactory = new STableFactory(){
        @Override
        public Partition getTable(Object tableName) throws IOException{
            assert tableName instanceof String: "Programmer error: improper type!";
            return getTable((String)tableName);
        }

        @Override
        public Partition getTable(String name) throws IOException{
            assert name.equalsIgnoreCase("person"): "Unknown table:"+name;
            return personPartition;
        }
    };
    private final IgnoreTxnCacheSupplier ignoreSupplier = new IgnoreTxnCacheSupplier(dataLib,tableFactory);
    private final DataFilterFactory filterFactory = new MFilterFactory();
    private final OperationStatusFactory operationStatusFactory = new MOpStatusFactory();
    private final TxnOperationFactory txnOpFactory = new MTxnOperationFactory(dataLib,exceptionFactory);

    public MemSITestEnv(){
    }

    @Override public SDataLib getDataLib(){ return dataLib; }
    @Override public Object getStore(){ return personPartition; }
    @Override public String getPersonTableName(){ return "person"; }
    @Override public Clock getClock(){ return clock; }
    @Override public TxnStore getTxnStore(){ return txnStore; }
    @Override public IgnoreTxnCacheSupplier getIgnoreTxnStore(){ return ignoreSupplier; }
    @Override public TimestampSource getTimestampSource(){ return tsSource; }

    @Override
    public DataFilterFactory getFilterFactory(){
        return filterFactory;
    }

    @Override
    public ExceptionFactory getExceptionFactory(){
        return exceptionFactory;
    }

    @Override
    public OperationStatusFactory getOperationStatusFactory(){
        return operationStatusFactory;
    }

    @Override
    public TxnOperationFactory getOperationFactory(){
        return txnOpFactory;
    }

    @Override
    public STableFactory getTableFactory(){
        return tableFactory;
    }

    @Override
    public Partition getPersonTable(TestTransactionSetup tts){
        return new TxnPartition(personPartition,
                tts.transactor,
                NoopRollForward.INSTANCE,
                txnOpFactory,
                tts.readController,
                NoOpReadResolver.INSTANCE);
    }
}
