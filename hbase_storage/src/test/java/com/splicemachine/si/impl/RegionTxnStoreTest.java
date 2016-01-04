package com.splicemachine.si.impl;

import com.google.protobuf.ByteString;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.impl.MockRegionUtils;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.lifecycle.TxnPartition;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.TransactionResolver;
import com.splicemachine.si.impl.store.TestingTimestampSource;
import com.splicemachine.si.impl.store.TestingTxnStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 *         Date: 6/30/14
 */
public class RegionTxnStoreTest{
    private static final Clock clock = new IncrementingClock();
    private static final TxnSupplier txnSupplier = new TestingTxnStore(clock,new TestingTimestampSource(),null,Long.MAX_VALUE);
    private static final HDataLib dataLib = new HDataLib();


    @Test
    public void testCanWriteAndReadNewTransactionInformation() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        TransactionResolver resolver=getTransactionResolver();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,resolver,dataLib,Long.MAX_VALUE,clock);
        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        TxnMessage.Txn txn = TxnMessage.Txn.newBuilder()
                .setCommitTs(-1)
                .setState(Txn.State.ACTIVE.getId())
                .setInfo(info)
                .build();
        store.recordTransaction(info);
        TxnTestUtils.assertTxnsMatch("Transactions do not match!",txn,store.getTransaction(info.getTxnId()));
    }

    @Test
    public void testNoTransactionReturnsNull() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        TransactionResolver resolver=getTransactionResolver();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,resolver,dataLib,Long.MAX_VALUE,clock);
        Assert.assertNull("Non-null txn came back!",store.getTransaction(1));
    }


    @Test
    public void testCanCommitATransaction() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,getTransactionResolver(),dataLib,Long.MAX_VALUE,clock);

        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        TxnMessage.Txn txn = TxnMessage.Txn.newBuilder()
                .setCommitTs(-1)
                .setState(Txn.State.ACTIVE.getId())
                .setInfo(info)
                .build();
        store.recordTransaction(info);
        TxnMessage.Txn transaction = store.getTransaction(info.getTxnId());
        TxnTestUtils.assertTxnsMatch("Transactions do not match!",txn,transaction);
        Txn.State currentState=store.getState(1);
        Assert.assertEquals("Incorrect current state!",Txn.State.ACTIVE,currentState);
        long commitTs=2l;
        store.recordCommit(1,commitTs);
        currentState=store.getState(1);
        Assert.assertEquals("Incorrect current state!",Txn.State.COMMITTED,currentState);

    }

    @Test
    public void testCanRollbackATransaction() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,getTransactionResolver(),dataLib,Long.MAX_VALUE,clock);

        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        TxnMessage.Txn txn = TxnMessage.Txn.newBuilder()
                .setCommitTs(-1)
                .setState(Txn.State.ACTIVE.getId())
                .setInfo(info)
                .build();
        store.recordTransaction(info);
        TxnMessage.Txn transaction = store.getTransaction(info.getTxnId());
        TxnTestUtils.assertTxnsMatch("Transactions do not match!",txn,transaction);
        store.recordRollback(1);
        Txn.State currentState=store.getState(1);
        Assert.assertEquals("Incorrect current state!",Txn.State.ROLLEDBACK,currentState);
    }

    @Test
    public void testCanGetActiveTransactions() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,getTransactionResolver(),dataLib,Long.MAX_VALUE,clock);

        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(info);
        long[] activeTxnIds=store.getActiveTxnIds(0,2,null);
        Assert.assertEquals("Incorrect length!",1,activeTxnIds.length);
        Assert.assertArrayEquals("Incorrect listing!",new long[]{1},activeTxnIds);
    }

    @Test
    public void home(){
        System.out.println("--"+Bytes.toBytes("g").length);
    }

    @Test
    public void testGetActiveTransactionsFiltersOutRolledbackTxns() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,getTransactionResolver(),dataLib,Long.MAX_VALUE,clock);

        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(info);

        clock.sleep(100,TimeUnit.MILLISECONDS); //sleep for 100 ms to ensure that the System.currentTimeMillis() moves forward
        store.recordRollback(1);
        long[] activeTxnIds=store.getActiveTxnIds(0,2,null);
        Assert.assertEquals("Incorrect length!",0,activeTxnIds.length);
    }

    @Test
    public void testGetActiveTransactionsFiltersOutCommittedTxns() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,getTransactionResolver(),dataLib,Long.MAX_VALUE,clock);

        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(info);
        clock.sleep(100,TimeUnit.MILLISECONDS); //sleep for 100 ms to ensure that the System.currentTimeMillis() moves forward
        store.recordCommit(1,2l);
        long[] activeTxnIds=store.getActiveTxnIds(0,3,null);
        Assert.assertEquals("Incorrect length!",0,activeTxnIds.length);
    }

    protected TransactionResolver getTransactionResolver(){
        TransactionResolver resolver=mock(TransactionResolver.class);
        doNothing().when(resolver).resolveGlobalCommitTimestamp(any(TxnPartition.class),any(TxnMessage.Txn.class));
        doNothing().when(resolver).resolveTimedOut(any(TxnPartition.class),any(TxnMessage.Txn.class));
        return resolver;
    }

}
