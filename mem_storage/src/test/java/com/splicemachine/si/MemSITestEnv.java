/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
