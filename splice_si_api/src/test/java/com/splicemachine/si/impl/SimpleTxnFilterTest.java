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

package com.splicemachine.si.impl;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.store.TestingTimestampSource;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.*;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.SITestDataEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;
import com.splicemachine.storage.DataPut;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 6/23/14
 */
@Category(ArchitectureSpecific.class)
public class SimpleTxnFilterTest{

    private ClientTxnLifecycleManager txnLifecycleManager;
    private TxnStore txnStore;
    private TxnSupplier txnSupplier;
    private TxnOperationFactory operationFactory;
    private ExceptionFactory exceptionFactory;

    @Before
    public void setUp() throws Exception{
        SITestDataEnv testEnv=SITestEnvironment.loadTestDataEnvironment();

        this.operationFactory=testEnv.getOperationFactory();
        this.exceptionFactory = testEnv.getExceptionFactory();


        OperationFactory operationFactory = testEnv.getBaseOperationFactory();
        TimestampSource tss = new TestingTimestampSource();
        this.txnStore=new TestingTxnStore(new IncrementingClock(),tss,exceptionFactory,Long.MAX_VALUE);
        this.txnSupplier = new ActiveTxnCacheSupplier(txnStore,1024);
        this.txnLifecycleManager= new ClientTxnLifecycleManager(tss,exceptionFactory);
        this.txnLifecycleManager.setTxnStore(txnStore);
        this.txnLifecycleManager.setKeepAliveScheduler(new ManualKeepAliveScheduler(txnStore));
    }

    @Test
    public void testCanSeeCommittedRowSnapshotIsolation() throws Exception{
        TxnSupplier baseStore=txnStore;

        TxnView committed=new CommittedTxn(0x100l,0x200l);
        baseStore.cache(committed);

        ReadResolver noopResolver=NoOpReadResolver.INSTANCE;
        TxnView myTxn=new InheritingTxnView(Txn.ROOT_TRANSACTION,0x300l,0x300l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ACTIVE);

        DataPut testCommitKv=operationFactory.newDataPut(myTxn,Encoding.encode("1"));
        testCommitKv.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0x100l,Bytes.toBytes(1l));
        DataCell commitCell=testCommitKv.cells().iterator().next();
        Assert.assertNotNull("Did not create a commit cell!",commitCell);
        Assert.assertEquals("Incorrect data type!",CellType.COMMIT_TIMESTAMP,commitCell.dataType());

        SimpleTxnFilter filterState=new SimpleTxnFilter(null,myTxn,noopResolver,baseStore);

        DataFilter.ReturnCode code=filterState.filterCell(commitCell);
        Assert.assertEquals("Incorrect return code for commit keyvalue!",DataFilter.ReturnCode.SKIP,code);

        DataPut testUserPut=operationFactory.newDataPut(myTxn,Encoding.encode("1"));
        testUserPut.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,0x100l,Encoding.encode("hello"));
        DataCell userCell=testUserPut.cells().iterator().next();
        Assert.assertNotNull("Did not create a user cell!",userCell);
        Assert.assertEquals("Incorrect data type!",CellType.USER_DATA,userCell.dataType());

        DataFilter.ReturnCode returnCode=filterState.filterCell(userCell);
        Assert.assertEquals("Incorrect return code for data cell!",DataFilter.ReturnCode.INCLUDE,returnCode);
    }

    @Test
    public void testCannotSeeRolledBackRow() throws Exception{
        TxnSupplier baseStore=txnStore;

        Txn rolledBack=txnLifecycleManager.beginTransaction(Bytes.toBytes("hello"));
        rolledBack.rollback();

        ReadResolver noopResolver=NoOpReadResolver.INSTANCE;
        TxnView myTxn=new InheritingTxnView(Txn.ROOT_TRANSACTION,0x300l,0x300l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ACTIVE);
        SimpleTxnFilter filterState=new SimpleTxnFilter(null,myTxn,noopResolver,baseStore);

        DataCell userCell=getUserCell(rolledBack);

        DataFilter.ReturnCode returnCode=filterState.filterCell(userCell);
        Assert.assertEquals("Incorrect return code for data cell!",DataFilter.ReturnCode.SKIP,returnCode);
    }


	/*Tests that Read-Resolution doesn't happen with active transactions*/

    @Test
    public void testWillNotReadResolveActiveTransaction() throws Exception{
        /*
         * Tests that data written by an active transaction will not read-resolve
		 */
        TxnSupplier baseStore=txnSupplier;

        Txn active=new WritableTxn(0x200l,0x200l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,mock(TxnLifecycleManager.class),false,exceptionFactory);
        baseStore.cache(active);

        assertActive(baseStore,active,0x400l);
    }

    @Test
    public void testWillNotReadResolveActiveTransactionWithCommittedDependentChild() throws Exception{
		/*
		 * Tests that data written by an active transaction will not read-resolve, even if
		 * it was written by a child transaction which was committed.
		 */
        TxnSupplier baseStore=txnSupplier;

        TxnView active=new WritableTxn(0x200l,0x200l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,mock(TxnLifecycleManager.class),false,exceptionFactory);
        TxnView child=new WritableTxn(0x300l,0x300l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,active,mock(TxnLifecycleManager.class),false,exceptionFactory);
        baseStore.cache(active);
        baseStore.cache(child);

        assertActive(baseStore,child,0x500l);
    }

    /*Tests for Read-Resolution of Committed transactions*/
    @Test
    public void testWillReadResolveCommittedTransaction() throws Exception{
		/*
		 * Tests that data written by a committed transaction will read-resolve
		 */
        TxnSupplier baseStore=txnStore;

        Txn committed = txnLifecycleManager.beginTransaction(Bytes.toBytes("table"));
        committed.commit();

        assertCommitted(baseStore,committed,0x500l);
    }

    @Test
    public void testWillReadResolveCommittedDependentChildTransaction() throws Exception{
		/*
		 * Tests that data written by a committed transaction will read-resolve a transaction
		 * as committed if its parent is committed
		 */
        TxnSupplier baseStore=txnStore;

        Txn parentTxn=txnLifecycleManager.beginTransaction(Bytes.toBytes("table"));
        Txn committed=txnLifecycleManager.beginChildTransaction(parentTxn,Bytes.toBytes("hello"));
        committed.commit();
        parentTxn.commit();

        assertCommitted(baseStore,committed,0x500l);
    }

    @Test
    public void testWillReadResolveActiveDependentChildOfCommittedParent() throws Exception{
		/*
		 * Tests that data written by a dependent child transaction will
		 * read-resolve as committed if its parent is committed and it is NOT rolled back
		 */
        TxnSupplier baseStore=txnSupplier;

        TxnView parentTxn=getMockCommittedTxn(0x200l,0x500l,null);
        TxnView committed=getMockCommittedTxn(0x300l,0x400l,null);
        baseStore.cache(parentTxn);
        baseStore.cache(committed);

        assertCommitted(baseStore,committed,0x600l);
    }

    /*Tests around read-resolution of rolled back transactions*/
    @Test
    public void testWillReadResolveRolledBackTransaction() throws Exception{
		/*
		 * Tests that data written by a rolled-back transaction will read-resolve as rolled back.
		 */
        TxnSupplier baseStore=txnSupplier;

        TxnView rolledBackTxn=getMockRolledBackTxn(0x100l,null);
        baseStore.cache(rolledBackTxn);

        assertRolledBack(baseStore,rolledBackTxn);
    }


    @Test
    public void testWillReadResolveActiveDependentChildOfRolledBackParent() throws Exception{
		/*
		 * Tests that data written by a dependent child transaction will be rolled back if
		 * the parent has been rolled back, if the child itself is still active
		 */
        TxnSupplier baseStore=txnSupplier;

        TxnView parenTxn=getMockRolledBackTxn(0x100l,null);
        TxnView rolledBackTxn=getMockActiveTxn(0x200l,parenTxn);
        baseStore.cache(rolledBackTxn);
        baseStore.cache(parenTxn);

        assertRolledBack(baseStore,rolledBackTxn);
    }


    @Test
    public void testWillReadResolveDependentChildOfRolledBackParent() throws Exception{
		/*
		 * Tests that data written by a dependent child transaction will be rolled back if
		 * the parent has been rolled back, even if the child itself has committed
		 */
        TxnSupplier baseStore=txnSupplier;

        TxnView parenTxn=getMockRolledBackTxn(0x100l,null);
        TxnView rolledBackTxn=getMockCommittedTxn(0x200l,0x300l,parenTxn);
        baseStore.cache(parenTxn);
        baseStore.cache(rolledBackTxn);

        assertRolledBack(baseStore,rolledBackTxn);
    }

    @Test
    public void testWillReadResolveRolledBackDependentChildTransaction() throws Exception{
		/*
		 * Tests that data written by a rolled-back transaction will read-resolve as rolled back,
		 * even if that transaction is the child of a transaction which has been committed
		 */
        TxnSupplier baseStore=txnSupplier;

        TxnView parenTxn=getMockCommittedTxn(0x100l,0x300l,null);
        TxnView rolledBackTxn=getMockRolledBackTxn(0x200l,parenTxn);
        baseStore.cache(parenTxn);
        baseStore.cache(rolledBackTxn);

        assertRolledBack(baseStore,rolledBackTxn);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private ReadResolver getRollBackReadResolver(final Pair<ByteSlice, Long> rolledBackTs){
        ReadResolver resolver=mock(ReadResolver.class);

        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable{
                Object[] args=invocationOnMock.getArguments();
                rolledBackTs.setFirst((ByteSlice)args[0]);
                rolledBackTs.setSecond((Long)args[1]);
                return null;
            }
        }).when(resolver).resolve(any(ByteSlice.class),anyLong());
        return resolver;
    }

    private ReadResolver getCommitReadResolver(final Pair<ByteSlice, Pair<Long, Long>> committedTs,final TxnSupplier txnStore){
        ReadResolver resolver=mock(ReadResolver.class);

        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable{
                Object[] args=invocationOnMock.getArguments();
                committedTs.setFirst((ByteSlice)args[0]);
                long tx=(Long)args[1];
                long commitTs=txnStore.getTransaction(tx).getEffectiveCommitTimestamp();
                committedTs.setSecond(Pair.newPair(tx,commitTs));
                return null;
            }
        }).when(resolver).resolve(any(ByteSlice.class),anyLong());
        return resolver;
    }

    private ReadResolver getActiveReadResolver(){
        ReadResolver resolver=mock(ReadResolver.class);
        doThrow(new AssertionError("Attempted to resolve an entry as committed!"))
                .when(resolver)
                .resolve(any(ByteSlice.class),anyLong());
        return resolver;
    }

    private void assertRolledBack(TxnSupplier baseStore,TxnView rolledBackTxn) throws IOException{
        TxnView myTxn=new InheritingTxnView(Txn.ROOT_TRANSACTION,4l,4l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ACTIVE);

        final Pair<ByteSlice, Long> rolledBackTs=new Pair<>();
        ReadResolver resolver=getRollBackReadResolver(rolledBackTs);
        SimpleTxnFilter filter=new SimpleTxnFilter(null,myTxn,resolver,baseStore);

        DataCell testDataKv=getUserCell(rolledBackTxn);

        DataFilter.ReturnCode returnCode=filter.filterCell(testDataKv);
        Assert.assertEquals("Incorrect return code for data keyvalue!",DataFilter.ReturnCode.SKIP,returnCode);

        Assert.assertNotNull("ReadResolver was not told to rollback!",rolledBackTs.getFirst());

        ByteSlice first=rolledBackTs.getFirst();
        Assert.assertArrayEquals("Incorrect row to resolve rolledBackTxn!",testDataKv.key(),first.getByteCopy());

        long rolledBackTxnId=rolledBackTs.getSecond();
        Assert.assertEquals("Incorrect version of data to be rolled back!",rolledBackTxn.getTxnId(),rolledBackTxnId);
    }

    private void assertCommitted(TxnSupplier baseStore,TxnView committed,long readTs) throws IOException{
        TxnView myTxn=new InheritingTxnView(Txn.ROOT_TRANSACTION,readTs,readTs,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ACTIVE);

        final Pair<ByteSlice, Pair<Long, Long>> committedTs=new Pair<>();
        ReadResolver resolver=getCommitReadResolver(committedTs,baseStore);
        SimpleTxnFilter filter=new SimpleTxnFilter(null,myTxn,resolver,baseStore);

        DataCell testDataKv=getUserCell(committed);

        DataFilter.ReturnCode returnCode=filter.filterCell(testDataKv);
        Assert.assertEquals("Incorrect return code for data keyvalue!",DataFilter.ReturnCode.INCLUDE,returnCode);

        Assert.assertNotNull("ReadResolver was not told to commit!",committedTs.getFirst());

        ByteSlice first=committedTs.getFirst();
        Assert.assertArrayEquals("Incorrect row to resolve committed!",testDataKv.key(),first.getByteCopy());

        Pair<Long, Long> txnIdToCommitTs=committedTs.getSecond();
        Assert.assertEquals("Incorrect transaction id!",
                committed.getTxnId() & SIConstants.TRANSANCTION_ID_MASK,
                txnIdToCommitTs.getFirst().longValue() & SIConstants.TRANSANCTION_ID_MASK);
        Assert.assertEquals("Incorrect commit timestamp!",committed.getEffectiveCommitTimestamp(),
                txnIdToCommitTs.getSecond().longValue());
    }


    private void assertActive(TxnSupplier baseStore,TxnView active,long readTs) throws IOException{
        Txn myTxn=new ReadOnlyTxn(readTs,readTs,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,mock(TxnLifecycleManager.class),exceptionFactory,false);
        ReadResolver resolver=getActiveReadResolver();

        SimpleTxnFilter filter=new SimpleTxnFilter(null,myTxn,resolver,baseStore);

        DataCell testDataKv=getUserCell(active);

        DataFilter.ReturnCode returnCode=filter.filterCell(testDataKv);
        Assert.assertEquals("Incorrect return code for data keyValue!",DataFilter.ReturnCode.SKIP,returnCode);

        //the read-resolver will ensure that an error is thrown if we attempt to read-resolve
    }

    private DataCell getUserCell(TxnView txn) throws IOException{
        DataPut testUserPut=operationFactory.newDataPut(txn,Encoding.encode("1"));
        testUserPut.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,txn.getBeginTimestamp(),Encoding.encode("hello"));
        DataCell userCell=testUserPut.cells().iterator().next();
        Assert.assertNotNull("Did not create a user cell!",userCell);
        Assert.assertEquals("Incorrect data type!",CellType.USER_DATA,userCell.dataType());
        return userCell;
    }

    private TxnView getMockCommittedTxn(long begin,long commit,TxnView parent){
        if(parent==null)
            parent=Txn.ROOT_TRANSACTION;
        return new InheritingTxnView(parent,begin,begin,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                false,false,
                true,true,
                commit,-1l,
                Txn.State.COMMITTED);
    }

    private TxnView getMockRolledBackTxn(long begin,TxnView parent){
        if(parent==null)
            parent=Txn.ROOT_TRANSACTION;
        return new InheritingTxnView(parent,
                begin,begin,true,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ROLLEDBACK);
    }

    private TxnView getMockActiveTxn(long begin,TxnView parent){
        if(parent==null)
            parent=Txn.ROOT_TRANSACTION;
        return new InheritingTxnView(parent,begin,begin,true,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ACTIVE);
    }

}
