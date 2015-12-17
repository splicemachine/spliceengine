package com.splicemachine.si.impl;

import com.google.common.collect.Maps;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.si.impl.txn.*;
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;
import com.splicemachine.storage.DataPut;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class SimpleTxnFilterTest{

    private SITestEnv testEnv;

    private ClientTxnLifecycleManager txnLifecycleManager;
    private DataStore ds;

    @Before
    public void setUp() throws Exception{
        if(testEnv==null)
            testEnv=SITestEnvironment.loadTestEnvironment();
        this.ds =TxnTestUtils.playDataStore(testEnv.getDataLib(),testEnv.getTxnStore(), txnLifecycleManager,testEnv.getExceptionFactory());
        this.txnLifecycleManager= new ClientTxnLifecycleManager(testEnv.getTimestampSource(),testEnv.getExceptionFactory());
        this.txnLifecycleManager.setTxnStore(testEnv.getTxnStore());
        this.txnLifecycleManager.setKeepAliveScheduler(new ManualKeepAliveScheduler(testEnv.getTxnStore()));
    }

    @Test
    public void testCanSeeCommittedRowSnapshotIsolation() throws Exception{
        TxnSupplier baseStore=testEnv.getTxnStore();
        IgnoreTxnCacheSupplier ignoreTxnCacheSupplier=new IgnoreTxnCacheSupplier(testEnv.getDataLib());

        TxnView committed=new CommittedTxn(0l,1l);
        baseStore.cache(committed);

        ReadResolver noopResolver=NoOpReadResolver.INSTANCE;
        TxnView myTxn=new InheritingTxnView(Txn.ROOT_TRANSACTION,2l,2l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ACTIVE);

        DataPut testCommitKv=testEnv.getOperationFactory().newDataPut(myTxn,Encoding.encode("1"));
        testCommitKv.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0l,Bytes.toBytes(1l));
        DataCell commitCell=testCommitKv.cells().iterator().next();
        Assert.assertNotNull("Did not create a commit cell!",commitCell);
        Assert.assertEquals("Incorrect data type!",CellType.COMMIT_TIMESTAMP,commitCell.dataType());

        SimpleTxnFilter filterState=new SimpleTxnFilter(null,myTxn,noopResolver,baseStore,ignoreTxnCacheSupplier,ds);

        DataFilter.ReturnCode code=filterState.filterKeyValue(commitCell);
        Assert.assertEquals("Incorrect return code for commit keyvalue!",DataFilter.ReturnCode.SKIP,code);

        DataPut testUserPut=testEnv.getOperationFactory().newDataPut(myTxn,Encoding.encode("1"));
        testUserPut.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,0l,Encoding.encode("hello"));
        DataCell userCell=testUserPut.cells().iterator().next();
        Assert.assertNotNull("Did not create a user cell!",userCell);
        Assert.assertEquals("Incorrect data type!",CellType.USER_DATA,userCell.dataType());

        DataFilter.ReturnCode returnCode=filterState.filterKeyValue(userCell);
        Assert.assertEquals("Incorrect return code for data cell!",DataFilter.ReturnCode.INCLUDE,returnCode);
    }

    @Test
    public void testCannotSeeRolledBackRow() throws Exception{
        TxnSupplier baseStore=testEnv.getTxnStore();
        IgnoreTxnCacheSupplier ignoreTxnCacheSupplier=new IgnoreTxnCacheSupplier(testEnv.getDataLib());

        Txn rolledBack=txnLifecycleManager.beginTransaction(Bytes.toBytes("hello"));
        rolledBack.rollback();

        ReadResolver noopResolver=NoOpReadResolver.INSTANCE;
        TxnView myTxn=new InheritingTxnView(Txn.ROOT_TRANSACTION,2l,2l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ACTIVE);
        SimpleTxnFilter filterState=new SimpleTxnFilter(null,myTxn,noopResolver,baseStore,ignoreTxnCacheSupplier,ds);

        DataCell userCell=getUserCell(rolledBack);

        DataFilter.ReturnCode returnCode=filterState.filterKeyValue(userCell);
        Assert.assertEquals("Incorrect return code for data cell!",DataFilter.ReturnCode.SKIP,returnCode);
    }


	/*Tests that Read-Resolution doesn't happen with active transactions*/

    @Test
    public void testWillNotReadResolveActiveTransaction() throws Exception{
        /*
         * Tests that data written by an active transaction will not read-resolve
		 */
        Map<Long, TxnView> txnMap=Maps.newHashMap();
        TxnSupplier baseStore=getMapStore(txnMap);

        Txn active=new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,mock(TxnLifecycleManager.class),false,testEnv.getExceptionFactory());
        txnMap.put(active.getTxnId(),active);

        assertActive(baseStore,active,2l);
    }

    @Test
    public void testWillNotReadResolveActiveTransactionWithCommittedDependentChild() throws Exception{
		/*
		 * Tests that data written by an active transaction will not read-resolve, even if
		 * it was written by a child transaction which was committed.
		 */
        Map<Long, TxnView> txnMap=Maps.newHashMap();
        TxnSupplier baseStore=getMapStore(txnMap);

        TxnView active=new WritableTxn(1l,1l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,mock(TxnLifecycleManager.class),false,testEnv.getExceptionFactory());
        TxnView child=new WritableTxn(2l,2l,Txn.IsolationLevel.SNAPSHOT_ISOLATION,active,mock(TxnLifecycleManager.class),false,testEnv.getExceptionFactory());
        txnMap.put(active.getTxnId(),active);
        txnMap.put(child.getTxnId(),child);

        assertActive(baseStore,child,5l);
    }

    /*Tests for Read-Resolution of Committed transactions*/
    @Test
    public void testWillReadResolveCommittedTransaction() throws Exception{
		/*
		 * Tests that data written by a committed transaction will read-resolve
		 */
        TxnSupplier baseStore=testEnv.getTxnStore();

        Txn committed = txnLifecycleManager.beginTransaction(Bytes.toBytes("table"));
        committed.commit();

        assertCommitted(baseStore,committed,5l);
    }

    @Test
    public void testWillReadResolveCommittedDependentChildTransaction() throws Exception{
		/*
		 * Tests that data written by a committed transaction will read-resolve a transaction
		 * as committed if its parent is committed
		 */
        TxnSupplier baseStore=testEnv.getTxnStore();

        Txn parentTxn=txnLifecycleManager.beginTransaction(Bytes.toBytes("table"));
        Txn committed=txnLifecycleManager.beginChildTransaction(parentTxn,Bytes.toBytes("hello"));
        committed.commit();
        parentTxn.commit();

        assertCommitted(baseStore,committed,5l);
    }

    @Test
    public void testWillReadResolveActiveDependentChildOfCommittedParent() throws Exception{
		/*
		 * Tests that data written by a dependent child transaction will
		 * read-resolve as committed if its parent is committed and it is NOT rolled back
		 */
        Map<Long, TxnView> txnMap=Maps.newHashMap();
        TxnSupplier baseStore=getMapStore(txnMap);

        TxnView parentTxn=getMockCommittedTxn(1,4,null);
        TxnView committed=getMockCommittedTxn(2,3,null);
        txnMap.put(committed.getTxnId(),committed);
        txnMap.put(parentTxn.getTxnId(),parentTxn);

        assertCommitted(baseStore,committed,5l);
    }

    /*Tests around read-resolution of rolled back transactions*/
    @Test
    public void testWillReadResolveRolledBackTransaction() throws Exception{
		/*
		 * Tests that data written by a rolled-back transaction will read-resolve as rolled back.
		 */
        Map<Long, TxnView> txnMap=Maps.newHashMap();
        TxnSupplier baseStore=getMapStore(txnMap);

        TxnView rolledBackTxn=getMockRolledBackTxn(1l,null);
        txnMap.put(rolledBackTxn.getTxnId(),rolledBackTxn);

        assertRolledBack(baseStore,rolledBackTxn);
    }


    @Test
    public void testWillReadResolveActiveDependentChildOfRolledBackParent() throws Exception{
		/*
		 * Tests that data written by a dependent child transaction will be rolled back if
		 * the parent has been rolled back, if the child itself is still active
		 */
        Map<Long, TxnView> txnMap=Maps.newHashMap();
        TxnSupplier baseStore=getMapStore(txnMap);

        TxnView parenTxn=getMockRolledBackTxn(1l,null);
        TxnView rolledBackTxn=getMockActiveTxn(2l,parenTxn);
        txnMap.put(rolledBackTxn.getTxnId(),rolledBackTxn);
        txnMap.put(parenTxn.getTxnId(),parenTxn);

        assertRolledBack(baseStore,rolledBackTxn);
    }


    @Test
    public void testWillReadResolveDependentChildOfRolledBackParent() throws Exception{
		/*
		 * Tests that data written by a dependent child transaction will be rolled back if
		 * the parent has been rolled back, even if the child itself has committed
		 */
        Map<Long, TxnView> txnMap=Maps.newHashMap();
        TxnSupplier baseStore=getMapStore(txnMap);

        TxnView parenTxn=getMockRolledBackTxn(1l,null);
        TxnView rolledBackTxn=getMockCommittedTxn(2,3,parenTxn);
        txnMap.put(rolledBackTxn.getTxnId(),rolledBackTxn);
        txnMap.put(parenTxn.getTxnId(),parenTxn);

        assertRolledBack(baseStore,rolledBackTxn);
    }

    @Test
    public void testWillReadResolveRolledBackDependentChildTransaction() throws Exception{
		/*
		 * Tests that data written by a rolled-back transaction will read-resolve as rolled back,
		 * even if that transaction is the child of a transaction which has been committed
		 */
        Map<Long, TxnView> txnMap=Maps.newHashMap();
        TxnSupplier baseStore=getMapStore(txnMap);

        TxnView parenTxn=getMockCommittedTxn(1l,3l,null);
        TxnView rolledBackTxn=getMockRolledBackTxn(2l,parenTxn);
        txnMap.put(rolledBackTxn.getTxnId(),rolledBackTxn);
        txnMap.put(parenTxn.getTxnId(),parenTxn);

        assertRolledBack(baseStore,rolledBackTxn);
    }

    /**
     * **************************************************************************************************************
     */
	/*private helper methods*/
    private TxnSupplier getMapStore(final Map<Long, TxnView> txnMap) throws IOException{
        TxnSupplier baseStore=mock(TxnSupplier.class);
        when(baseStore.getTransaction(anyLong())).thenAnswer(new Answer<TxnView>(){
            @Override
            public TxnView answer(InvocationOnMock invocationOnMock) throws Throwable{
                //noinspection SuspiciousMethodCalls
                return txnMap.get(invocationOnMock.getArguments()[0]);
            }
        });
        when(baseStore.getTransaction(anyLong(),anyBoolean())).thenAnswer(new Answer<TxnView>(){
            @Override
            public TxnView answer(InvocationOnMock invocationOnMock) throws Throwable{
                //noinspection SuspiciousMethodCalls
                return txnMap.get(invocationOnMock.getArguments()[0]);
            }
        });
        return baseStore;
    }

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
        final IgnoreTxnCacheSupplier ignoreTxnCacheSupplier=new IgnoreTxnCacheSupplier(testEnv.getDataLib());
        SimpleTxnFilter filter=new SimpleTxnFilter(null,myTxn,resolver,baseStore,ignoreTxnCacheSupplier,ds);

        DataCell testDataKv=getUserCell(rolledBackTxn);

        DataFilter.ReturnCode returnCode=filter.filterKeyValue(testDataKv);
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
        final IgnoreTxnCacheSupplier ignoreTxnCacheSupplier=new IgnoreTxnCacheSupplier(testEnv.getDataLib());
        SimpleTxnFilter filter=new SimpleTxnFilter(null,myTxn,resolver,baseStore,ignoreTxnCacheSupplier,ds);

        DataCell testDataKv=getUserCell(committed);

        DataFilter.ReturnCode returnCode=filter.filterKeyValue(testDataKv);
        Assert.assertEquals("Incorrect return code for data keyvalue!",DataFilter.ReturnCode.INCLUDE,returnCode);

        Assert.assertNotNull("ReadResolver was not told to commit!",committedTs.getFirst());

        ByteSlice first=committedTs.getFirst();
        Assert.assertArrayEquals("Incorrect row to resolve committed!",testDataKv.key(),first.getByteCopy());

        Pair<Long, Long> txnIdToCommitTs=committedTs.getSecond();
        Assert.assertEquals("Incorrect transaction id!",committed.getTxnId(),txnIdToCommitTs.getFirst().longValue());
        Assert.assertEquals("Incorrect commit timestamp!",committed.getEffectiveCommitTimestamp(),
                txnIdToCommitTs.getSecond().longValue());
    }


    private void assertActive(TxnSupplier baseStore,TxnView active,long readTs) throws IOException{
        Txn myTxn=new ReadOnlyTxn(readTs,readTs,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,mock(TxnLifecycleManager.class),false);
        final IgnoreTxnCacheSupplier ignoreTxnCacheSupplier=new IgnoreTxnCacheSupplier(testEnv.getDataLib());
        ReadResolver resolver=getActiveReadResolver();

        SimpleTxnFilter filter=new SimpleTxnFilter(null,myTxn,resolver,baseStore,ignoreTxnCacheSupplier,ds);

        DataCell testDataKv=getUserCell(active);

        DataFilter.ReturnCode returnCode=filter.filterKeyValue(testDataKv);
        Assert.assertEquals("Incorrect return code for data keyValue!",DataFilter.ReturnCode.SKIP,returnCode);

        //the read-resolver will ensure that an error is thrown if we attempt to read-resolve
    }

    private DataCell getUserCell(TxnView txn){
        DataPut testUserPut=testEnv.getOperationFactory().newDataPut(txn,Encoding.encode("1"));
        testUserPut.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,txn.getBeginTimestamp(),Encoding.encode("hello"));
        DataCell userCell=testUserPut.cells().iterator().next();
        Assert.assertNotNull("Did not create a user cell!",userCell);
        Assert.assertEquals("Incorrect data type!",CellType.USER_DATA,userCell.dataType());
        return userCell;
    }

    protected TxnView getMockCommittedTxn(long begin,long commit,TxnView parent){
        if(parent==null)
            parent=Txn.ROOT_TRANSACTION;
        return new InheritingTxnView(parent,begin,begin,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                false,false,
                true,true,
                commit,-1l,
                Txn.State.COMMITTED);
    }

    protected TxnView getMockRolledBackTxn(long begin,TxnView parent){
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
