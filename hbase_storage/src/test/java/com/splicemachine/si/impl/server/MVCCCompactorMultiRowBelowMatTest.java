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
 *
 */

package com.splicemachine.si.impl.server;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.impl.UnsupportedLifecycleManager;
import com.splicemachine.si.impl.store.TestingTimestampSource;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.Cell;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


import static com.splicemachine.si.impl.server.MVCCDataUtils.*;
/**
 * @author Scott Fines
 *         Date: 12/5/16
 */
public class MVCCCompactorMultiRowBelowMatTest{

    private static final KryoPool kryoPool=new KryoPool(1);
    private static final TestingTimestampSource tsGen=new TestingTimestampSource(1L);

    private final TestingTxnStore supplier = new TestingTxnStore(SystemClock.INSTANCE,
            tsGen,
            HExceptionFactory.INSTANCE,Long.MAX_VALUE);

    @Test
    public void mergesInsertAndUpdates() throws Exception{
        /*
         * Stick some updates in the middle of two inserted rows, and ensure it is properly merged.
         */
        Txn i1 = committedTransaction();
        List<Cell> r1 = new ArrayList<>();r1.add(insert(i1,"row1"));
        Txn i2 = committedTransaction();
        byte[] i2Data = rowData(0,"col1");
        Txn u2 = committedTransaction();
        byte[] u2Data = rowData(1,"col2");
        List<Cell> r2 = new ArrayList<>();  r2.add(insert(u2,"row2",u2Data)); r2.add(insert(i2,"row2",i2Data));

        Txn i3 = committedTransaction();
        List<Cell> r3 = new ArrayList<>(); r3.add(insert(i3,"row3"));

        long mat = i3.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>();
        compactor.compact(r1,data);
        r1.add(0,commitTimestamp(i1,"row1"));
        assertListsCorrect(r1,data);

        data.clear();
        compactor.compact(r2,data);
        BitSet finalSet = new BitSet();
        finalSet.set(0); finalSet.set(1);
        byte[] mergedData = rowData(finalSet,"col1","col2");
        List<Cell> merged = Arrays.asList( commitTimestamp(u2,"row2"),insert(u2,"row2",mergedData));
        assertListsCorrect(merged,data);

        data.clear();
        compactor.compact(r3,data);
        r3.add(0,commitTimestamp(i3,"row3"));
        assertListsCorrect(r3,data);
    }

    @Test
    public void deletesBelowAntiTombstone() throws Exception{
        /*
         * Stick some updates in the middle of two inserted rows, and ensure it is properly merged.
         */
        Txn i1 = committedTransaction();
        List<Cell> r1 = new ArrayList<>();r1.add(insert(i1,"row1"));
        Txn i2 = committedTransaction();
        byte[] i2Data = rowData(0,"col1");
        Txn d2 = committedTransaction();
        Txn u2 = committedTransaction();
        byte[] u2Data = rowData(1,"col2");
        List<Cell> r2 = new ArrayList<>(); r2.add(antiTombstone(u2,"row2")); r2.add(tombstone(d2,"row2")); r2.add(insert(u2,"row2",u2Data)); r2.add(insert(i2,"row2",i2Data));

        Txn i3 = committedTransaction();
        List<Cell> r3 = new ArrayList<>(); r3.add(insert(i3,"row3"));

        long mat = i3.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>();
        compactor.compact(r1,data);
        r1.add(0,commitTimestamp(i1,"row1"));
        assertListsCorrect(r1,data);

        data.clear();
        compactor.compact(r2,data);
        List<Cell> merged = Arrays.asList( commitTimestamp(u2,"row2"),insert(u2,"row2",u2Data));
        assertListsCorrect(merged,data);

        data.clear();
        compactor.compact(r3,data);
        r3.add(0,commitTimestamp(i3,"row3"));
        assertListsCorrect(r3,data);
    }

    @Test
    public void removesDeletedRows() throws Exception{
        /*
         * Stick some updates in the middle of two inserted rows, and ensure it is properly merged.
         */
        Txn i1 = committedTransaction();
        List<Cell> r1 = new ArrayList<>();r1.add(insert(i1,"row1"));
        Txn i2 = committedTransaction();
        byte[] i2Data = rowData(0,"col1");
        Txn d2 = committedTransaction();
        List<Cell> r2 = new ArrayList<>();  r2.add(tombstone(d2,"row2")); r2.add(insert(i2,"row2",i2Data));

        Txn i3 = committedTransaction();
        List<Cell> r3 = new ArrayList<>(); r3.add(insert(i3,"row3"));

        long mat = i3.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>();
        compactor.compact(r1,data);
        r1.add(0,commitTimestamp(i1,"row1"));
        assertListsCorrect(r1,data);

        data.clear();
        compactor.compact(r2,data);
        Assert.assertEquals("Did not delete row2",0,data.size());

        data.clear();
        compactor.compact(r3,data);
        r3.add(0,commitTimestamp(i3,"row3"));
        assertListsCorrect(r3,data);
    }

    @Test
    public void ignoresRolledBackDeletes() throws Exception{
        /*
         * Stick some updates in the middle of two inserted rows, and ensure it is properly merged.
         */
        Txn i1 = committedTransaction();
        List<Cell> r1 = new ArrayList<>();r1.add(insert(i1,"row1"));
        Txn i2 = committedTransaction();
        byte[] i2Data = rowData(0,"col1");
        Txn d2 = rolledBackTransaction();
        List<Cell> r2 = new ArrayList<>();  r2.add(tombstone(d2,"row2")); r2.add(insert(i2,"row2",i2Data));

        Txn i3 = committedTransaction();
        List<Cell> r3 = new ArrayList<>(); r3.add(insert(i3,"row3"));

        long mat = i3.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>();
        compactor.compact(r1,data);
        r1.add(0,commitTimestamp(i1,"row1"));
        assertListsCorrect(r1,data);

        data.clear();
        compactor.compact(r2,data);
        assertListsCorrect(Arrays.asList(commitTimestamp(i2,"row2"),insert(i2,"row2",i2Data)),data);

        data.clear();
        compactor.compact(r3,data);
        r3.add(0,commitTimestamp(i3,"row3"));
        assertListsCorrect(r3,data);
    }

    @Test
    public void ignoresRolledBackUpdate() throws Exception{
        /*
         * Stick some updates in the middle of two inserted rows, and ensure it is properly merged.
         */
        Txn i1 = committedTransaction();
        List<Cell> r1 = new ArrayList<>();r1.add(insert(i1,"row1"));
        Txn i2 = committedTransaction();
        byte[] i2Data = rowData(0,"col1");
        Txn u2 = rolledBackTransaction();
        byte[] u2Data = rowData(1,"col2");
        List<Cell> r2 = new ArrayList<>();  r2.add(insert(u2,"row2",u2Data)); r2.add(insert(i2,"row2",i2Data));

        Txn i3 = committedTransaction();
        List<Cell> r3 = new ArrayList<>(); r3.add(insert(i3,"row3"));

        long mat = i3.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>();
        compactor.compact(r1,data);
        r1.add(0,commitTimestamp(i1,"row1"));
        assertListsCorrect(r1,data);

        data.clear();
        compactor.compact(r2,data);
        assertListsCorrect(Arrays.asList(commitTimestamp(i2,"row2"),insert(i2,"row2",i2Data)),data);

        data.clear();
        compactor.compact(r3,data);
        r3.add(0,commitTimestamp(i3,"row3"));
        assertListsCorrect(r3,data);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private byte[] rowData(int filledColumn,String fillValue) throws IOException{
        BitSet setCols = new BitSet();
        setCols.set(filledColumn);
        return rowData(setCols,fillValue);
    }

    private byte[] rowData(BitSet filledCols,String... fillValues) throws IOException{
        BitSet empty = new BitSet();
        assert filledCols.cardinality()==fillValues.length;
        EntryEncoder ee = EntryEncoder.create(kryoPool,fillValues.length,filledCols,empty,empty,empty);
        MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
        for(String fillValue:fillValues){
            entryEncoder.encodeNext(fillValue);
        }
        return ee.encode();
    }

    private Txn newTransaction() throws IOException{
        long ts=tsGen.nextTimestamp();
        Txn txn = new WritableTxn(ts,ts,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,UnsupportedLifecycleManager.INSTANCE,false,HExceptionFactory.INSTANCE);
        supplier.recordNewTransaction(txn);
        return txn;
    }

    private Txn committedTransaction() throws IOException{
        Txn t = newTransaction();
        supplier.commit(t.getTxnId());
        return supplier.getTransaction(t.getTxnId());
    }

    private Txn rolledBackTransaction() throws IOException{
        Txn t = newTransaction();
        supplier.rollback(t.getTxnId());
        return supplier.getTransaction(t.getTxnId());
    }

    private <T> void assertListsCorrect(List<T> correctList,List<T> actualList){
        Iterator<T> corrIter = correctList.iterator();
        Iterator<T> outIter = actualList.iterator();
        while(corrIter.hasNext()){
            Assert.assertTrue("Ran off output list too early!",outIter.hasNext());
            Assert.assertEquals("Incorrect cell!",corrIter.next(),outIter.next());
        }
    }
}
