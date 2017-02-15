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

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
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
    private final ExceptionFactory exceptionFactory = MExceptionFactory.INSTANCE;
    private final Clock clock = new IncrementingClock();
    private final TimestampSource tsSource = new MemTimestampSource();
    private final TxnStore txnStore = new MemTxnStore(clock,tsSource,exceptionFactory,1000);
    protected Partition personPartition;
    private final PartitionFactory tableFactory = new MPartitionFactory();
    private final OperationFactory opFactory = new MOperationFactory(clock);
    private final DataFilterFactory filterFactory = MFilterFactory.INSTANCE;
    private final OperationStatusFactory operationStatusFactory =MOpStatusFactory.INSTANCE;
    private final TxnOperationFactory txnOpFactory = new SimpleTxnOperationFactory(exceptionFactory,opFactory);

    public MemSITestEnv() throws IOException{
    }

    public void initialize() throws IOException{
        createTransactionalTable(Bytes.toBytes("person"));
        this.personPartition = tableFactory.getTable("person");
    }

    @Override public String getPersonTableName(){ return "person"; }
    @Override public Clock getClock(){ return clock; }
    @Override public TxnStore getTxnStore(){ return txnStore; }
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
    public OperationFactory getBaseOperationFactory(){
        return opFactory;
    }

    @Override
    public PartitionFactory getTableFactory(){
        return tableFactory;
    }

    @Override
    public void createTransactionalTable(byte[] tableNameBytes) throws IOException{
        try(PartitionAdmin pa = tableFactory.getAdmin()){
            pa.deleteTable(Bytes.toString(tableNameBytes));
            pa.newPartition().withName(Bytes.toString(tableNameBytes)).create();
        }
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

    @Override
    public Partition getPartition(String name,TestTransactionSetup tts) throws IOException{
        return new TxnPartition(tableFactory.getTable(name),
                tts.transactor,
                NoopRollForward.INSTANCE,
                txnOpFactory,
                tts.readController,
                NoOpReadResolver.INSTANCE);
    }
}
