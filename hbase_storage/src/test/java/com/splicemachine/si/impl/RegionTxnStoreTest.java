/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.carrotsearch.hppc.LongArrayList;
import com.google.protobuf.ByteString;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.encoding.Encoding;import com.splicemachine.impl.MockRegionUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.lifecycle.TxnPartition;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.TransactionResolver;
import com.splicemachine.si.impl.store.TestingTimestampSource;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.CommittedTxn;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 6/30/14
 */
public class RegionTxnStoreTest{
    private static final Clock clock = new IncrementingClock();
    private static final TxnSupplier txnSupplier = new TestingTxnStore(clock,new TestingTimestampSource(),null,Long.MAX_VALUE);


    @Test
    public void testCanWriteAndReadNewTransactionInformation() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        TransactionResolver resolver= getNullTransactionResolver();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,resolver,Long.MAX_VALUE,clock);
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
        TransactionResolver resolver= getNullTransactionResolver();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier,resolver,Long.MAX_VALUE,clock);
        Assert.assertNull("Non-null txn came back!",store.getTransaction(1));
    }


    @Test
    public void testCanCommitATransaction() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock);

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
        assertEquals("Incorrect current state!",Txn.State.ACTIVE,currentState);
        long commitTs=2l;
        store.recordCommit(1,commitTs);
        currentState=store.getState(1);
        assertEquals("Incorrect current state!",Txn.State.COMMITTED,currentState);

    }

    @Test
    public void testCanRollbackATransaction() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock);

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
        assertEquals("Incorrect current state!",Txn.State.ROLLEDBACK,currentState);
    }

    @Test
    public void testCanGetActiveTransactions() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock){
            @Override
            protected byte[] getRowKey(long txnId){
                byte[] rowKey = new byte[9];
                Bytes.longToBytes(txnId, rowKey, 1);
                return rowKey;
            }
        };

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
        assertEquals("Incorrect length!",1,activeTxnIds.length);
        Assert.assertArrayEquals("Incorrect listing!",new long[]{1},activeTxnIds);
    }

    @Test
    public void getManyActiveTransactions() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock){
            @Override
            protected byte[] getRowKey(long txnId){
                byte[] rowKey = new byte[9];
                Bytes.longToBytes(txnId, rowKey, 1);
                return rowKey;
            }
        };

        LongArrayList txns = new LongArrayList(6);
        TxnMessage.TxnInfo.Builder builder=TxnMessage.TxnInfo.newBuilder()
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")));
        store.recordTransaction(builder.setTxnId(1).build());
        txns.add(1);
        for(int i=0;i<4;i++){
            TxnMessage.TxnInfo build=builder.setTxnId(i+2).build();
            txns.add(build.getTxnId());
            store.recordTransaction(build);
        }

        TxnMessage.TxnInfo build=builder.setTxnId(7).build();
        txns.add(build.getTxnId());
        store.recordTransaction(build);
        long[] activeTxnIds=store.getActiveTxnIds(0,7,null);
        assertEquals("Incorrect length!",txns.size(),activeTxnIds.length);
        Assert.assertArrayEquals("Incorrect listing!",txns.toArray(),activeTxnIds);
    }

    @Test
    public void testCanGetActiveTransactionsOutsideRange() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock){
            @Override
            protected byte[] getRowKey(long txnId){
                byte[] rowKey = new byte[9];
                Bytes.longToBytes(txnId, rowKey, 1);
                return rowKey;
            }
        };

        TxnMessage.TxnInfo.Builder builder=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel());
        TxnMessage.TxnInfo info=builder
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(info);
        info = builder.setTxnId(5).setBeginTs(5).setDestinationTables(ByteString.copyFrom(Bytes.toBytes("3124"))).build();
        store.recordTransaction(info);
        long[] activeTxnIds=store.getActiveTxnIds(0,3,null);
        assertEquals("Incorrect length!",1,activeTxnIds.length);
        Assert.assertArrayEquals("Incorrect listing!",new long[]{1},activeTxnIds);
    }

    @Test
    public void testCanGetActiveTransactionsForSpecificTable() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock){
            @Override
            protected byte[] getRowKey(long txnId){
                byte[] rowKey = new byte[9];
                Bytes.longToBytes(txnId, rowKey, 1);
                return rowKey;
            }
        };

        TxnMessage.TxnInfo.Builder builder=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel());
        TxnMessage.TxnInfo info=builder
                .setDestinationTables(ByteString.copyFrom(Encoding.encodeBytesUnsorted(Bytes.toBytes("1234"))))
                .build();
        store.recordTransaction(info);
        info = builder.setTxnId(2).setBeginTs(2).setDestinationTables(ByteString.copyFrom(Encoding.encodeBytesUnsorted(Bytes.toBytes("3124")))).build();
        store.recordTransaction(info);
        long[] activeTxnIds=store.getActiveTxnIds(0,3,Encoding.encodeBytesUnsorted(Bytes.toBytes("1234")));
        assertEquals("Incorrect length!",1,activeTxnIds.length);
        Assert.assertArrayEquals("Incorrect listing!",new long[]{1},activeTxnIds);
    }

    @Test
    public void testCanGetActiveTransactionsForSpecificTableMultipleElevatedTables() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock){
            @Override
            protected byte[] getRowKey(long txnId){
                byte[] rowKey = new byte[9];
                Bytes.longToBytes(txnId, rowKey, 1);
                return rowKey;
            }
        };

        TxnMessage.TxnInfo.Builder builder=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(1)
                .setBeginTs(1)
                .setAllowsWrites(true)
                .setIsAdditive(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel());
        TxnMessage.TxnInfo info=builder
                .setDestinationTables(ByteString.copyFrom(Encoding.encodeBytesUnsorted(Bytes.toBytes("3124"))))
                .build();
        store.recordTransaction(info);
        store.addDestinationTable(1,Encoding.encodeBytesUnsorted(Bytes.toBytes("1234")));
        long[] activeTxnIds=store.getActiveTxnIds(0,3,Encoding.encodeBytesUnsorted(Bytes.toBytes("1234")));
        assertEquals("Incorrect length!",1,activeTxnIds.length);
        Assert.assertArrayEquals("Incorrect listing!",new long[]{1},activeTxnIds);
    }

    @Test
    public void testGetActiveTransactionsFiltersOutRolledbackTxns() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock);

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
        assertEquals("Incorrect length!",0,activeTxnIds.length);
    }

    @Test
    public void testGetActiveTransactionsFiltersOutCommittedTxns() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region,txnSupplier, getNullTransactionResolver(),Long.MAX_VALUE,clock);

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
        assertEquals("Incorrect length!",0,activeTxnIds.length);
    }

    protected TransactionResolver getNullTransactionResolver(){
        TransactionResolver resolver=mock(TransactionResolver.class);
        doNothing().when(resolver).resolveGlobalCommitTimestamp(any(TxnPartition.class),any(TxnMessage.Txn.class));
        doNothing().when(resolver).resolveTimedOut(any(TxnPartition.class),any(TxnMessage.Txn.class));
        return resolver;
    }

    @Test
    public void testSubtransactionRollbackDoesntRollbackWholeTransaction() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store=new RegionTxnStore(region, txnSupplier, getTransactionResolver(txnSupplier), 100, clock);

        long txnId = SIConstants.TRASANCTION_INCREMENT * 2l;
        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(txnId)
                .setBeginTs(txnId)
                .setAllowsWrites(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(info); // record main txn
        store.recordRollbackSubtransactions(txnId, new long[] {1});
        store.recordCommit(txnId, txnId * 2);
        clock.sleep(200,TimeUnit.MILLISECONDS); //sleep for 100 ms to ensure that the System.currentTimeMillis() moves forward

        assertEquals(Txn.State.COMMITTED, getState(store.getTransaction(txnId)));

        long subTxnId = txnId + 1;
        assertEquals(Txn.State.ROLLEDBACK, getState(store.getTransaction(subTxnId)));

        // sleep a bit for real to let disruptor process the event
        Thread.sleep( 100);

        // make sure we haven't rolledback the whole txn
        assertEquals(Txn.State.COMMITTED, getState(store.getTransaction(txnId)));
    }

    @Test
    public void testTimedOutTransactionResolution() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        RegionTxnStore store= spy(new RegionTxnStore(region, txnSupplier, getTransactionResolver(txnSupplier), 100, clock));
        ArgumentCaptor<Long> valueCapture = ArgumentCaptor.forClass(Long.class);

        doCallRealMethod().when(store).recordRollback(valueCapture.capture());

        long txnId = SIConstants.TRASANCTION_INCREMENT * 2l;
        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(txnId)
                .setBeginTs(txnId)
                .setAllowsWrites(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(info); // record main txn

        clock.sleep(20,TimeUnit.MILLISECONDS);

        assertEquals(0, valueCapture.getAllValues().size());
        assertEquals(Txn.State.ACTIVE, getState(store.getTransaction(txnId)));

        clock.sleep(100,TimeUnit.MILLISECONDS);

        assertEquals(Txn.State.ROLLEDBACK, getState(store.getTransaction(txnId)));

        // sleep a bit for real to let disruptor process the event
        Thread.sleep( 100);

        assertEquals(1, valueCapture.getAllValues().size());
        assertEquals(txnId, valueCapture.getValue().longValue());

    }

    @Test
    public void testCommittedTxnResolution() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        TxnSupplier localSupplier = mock(TxnSupplier.class);

        RegionTxnStore store= spy(new RegionTxnStore(region, localSupplier, getTransactionResolver(localSupplier), 100, clock));
        ArgumentCaptor<Long> txnIdCapture = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> globalCommitTsCapture = ArgumentCaptor.forClass(Long.class);

        doCallRealMethod().when(store).recordGlobalCommit(txnIdCapture.capture(), globalCommitTsCapture.capture());

        long txnId = SIConstants.TRASANCTION_INCREMENT * 2l;
        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(txnId)
                .setBeginTs(txnId)
                .setAllowsWrites(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(info); // record main txn

        long childId = SIConstants.TRASANCTION_INCREMENT * 3l;
        TxnMessage.TxnInfo child=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(childId)
                .setBeginTs(childId)
                .setParentTxnid(txnId)
                .setAllowsWrites(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(child);

        long childCommit = SIConstants.TRASANCTION_INCREMENT * 4l;
        long txnCommit = SIConstants.TRASANCTION_INCREMENT * 5l;

        store.recordCommit(childId, childCommit);
        store.recordCommit(txnId, txnCommit);

        when(localSupplier.getTransaction(anyLong())).thenReturn(new CommittedTxn(txnId, txnCommit)); // register txn in supplier

        assertEquals(Txn.State.COMMITTED, getState(store.getTransaction(txnId)));
        // sleep a bit for real to let disruptor process the event
        Thread.sleep( 100);

        // no resolution should happen for a top level transaction
        assertEquals(0, txnIdCapture.getAllValues().size());

        TxnMessage.Txn childInfo = store.getTransaction(childId);
        assertEquals(Txn.State.COMMITTED, getState(childInfo));
        assertEquals(childCommit, childInfo.getCommitTs());
        assertEquals(-1, childInfo.getGlobalCommitTs());

        // sleep a bit for real to let disruptor process the event
        Thread.sleep( 100);

        // we should have resolved the child transaction
        assertEquals(1, txnIdCapture.getAllValues().size());
        assertEquals(childId, txnIdCapture.getValue().longValue());
        assertEquals(txnCommit, globalCommitTsCapture.getValue().longValue());

        childInfo = store.getTransaction(childId);
        assertEquals(Txn.State.COMMITTED, getState(childInfo));
        assertEquals(childCommit, childInfo.getCommitTs());
        assertEquals(txnCommit, childInfo.getGlobalCommitTs());
    }

    @Test
    public void testSubtransactionRollbackDoesntGetResolved() throws Exception{
        HRegion region=MockRegionUtils.getMockRegion();
        TxnSupplier localSupplier = mock(TxnSupplier.class);

        RegionTxnStore store= spy(new RegionTxnStore(region, localSupplier, getTransactionResolver(localSupplier), 100, clock));
        ArgumentCaptor<Long> txnIdCapture = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> globalCommitTsCapture = ArgumentCaptor.forClass(Long.class);

        doCallRealMethod().when(store).recordGlobalCommit(txnIdCapture.capture(), globalCommitTsCapture.capture());

        long txnId = SIConstants.TRASANCTION_INCREMENT * 2l;
        TxnMessage.TxnInfo info=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(txnId)
                .setBeginTs(txnId)
                .setAllowsWrites(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(info); // record main txn

        long childId = SIConstants.TRASANCTION_INCREMENT * 3l;
        TxnMessage.TxnInfo child=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(childId)
                .setBeginTs(childId)
                .setParentTxnid(txnId)
                .setAllowsWrites(true)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel())
                .setDestinationTables(ByteString.copyFrom(Bytes.toBytes("1234")))
                .build();
        store.recordTransaction(child);

        store.recordRollbackSubtransactions(childId, new long[] {1});
        long subTxnId = childId + 1;

        long childCommit = SIConstants.TRASANCTION_INCREMENT * 4l;
        long txnCommit = SIConstants.TRASANCTION_INCREMENT * 5l;

        store.recordCommit(childId, childCommit);
        store.recordCommit(txnId, txnCommit);

        when(localSupplier.getTransaction(anyLong())).thenReturn(new CommittedTxn(txnId, txnCommit)); // register txn in supplier

        assertEquals(Txn.State.ROLLEDBACK, getState(store.getTransaction(subTxnId)));
        // sleep a bit for real to let disruptor process the event
        Thread.sleep( 100);

        assertEquals(Txn.State.COMMITTED, getState(store.getTransaction(txnId)));
        assertEquals(Txn.State.COMMITTED, getState(store.getTransaction(childId)));
    }

    private Txn.State getState(TxnMessage.Txn transaction) {
        return Txn.State.fromByte((byte) transaction.getState());
    }

    private TransactionResolver getTransactionResolver(TxnSupplier txnSupplier) {
        return new TransactionResolver(txnSupplier, 2, 10);
    }


}
