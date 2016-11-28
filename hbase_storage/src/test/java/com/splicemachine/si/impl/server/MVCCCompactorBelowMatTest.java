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
import java.util.*;

import static com.splicemachine.si.impl.server.MVCCDataUtils.*;

/**
 * Tests related to the behavior of the MVCC Compactor
 * when data is present below the Minimum Active Transaction(MAT).
 *
 * @author Scott Fines
 *         Date: 11/28/16
 */
public class MVCCCompactorBelowMatTest{
    private static final KryoPool kryoPool=new KryoPool(1);
    private static final TestingTimestampSource tsGen=new TestingTimestampSource(1L);

    private final TestingTxnStore supplier = new TestingTxnStore(SystemClock.INSTANCE,
            tsGen,
            HExceptionFactory.INSTANCE,Long.MAX_VALUE);

    /* ****************************************************************************************************************/
    /*insert tests*/

    @Test
    public void doesNothingBelowMatIfOnlySingleCell() throws Exception{
        Txn insertTxn = newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Cell insertCell = insert(insertTxn);
        Cell insertCt = commitTimestamp(insertTxn);

        long mat = insertTxn.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,2,2);

        List<Cell> data = new ArrayList<>(2);
        compactor.compact(Arrays.asList(insertCt,insertCell),data);

        assertListsCorrect(Arrays.asList(insertCt,insertCell),data);
    }

    @Test
    public void mergesInsertAndUpdates() throws Exception{
        Txn insertTxn = newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        BitSet setCols = new BitSet();
        setCols.set(0);
        BitSet empty = new BitSet();
        EntryEncoder ee = EntryEncoder.create(kryoPool,2,setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col1");

        Cell insertCol1 = insert(insertTxn,ee.encode());
        Cell ctCol1 = commitTimestamp(insertTxn);

        Txn icol2Txn = newTransaction();
        supplier.commit(icol2Txn.getTxnId());
        icol2Txn = supplier.getTransaction(icol2Txn.getTxnId());

        setCols.clear();
        setCols.set(1);
        ee.reset(setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col2");
        Cell col2 = insert(icol2Txn,ee.encode());
        Cell ctCol2 = commitTimestamp(icol2Txn);

        long mat = icol2Txn.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>(2);
        //don't include the ctCol2, to make sure that we create the commit ts for the MAT cell
        compactor.compact(Arrays.asList(ctCol1,col2,insertCol1),data);

        setCols.clear();
        setCols.set(0);
        setCols.set(1);
        ee.reset(setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col1").encodeNext("col2");
        assertListsCorrect(Arrays.asList(ctCol2,insert(icol2Txn,ee.encode())),data);
    }

    @Test
    public void doesNotMergeAcrossMat() throws Exception{
        Txn insertTxn = newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        BitSet setCols = new BitSet();
        setCols.set(0);
        BitSet empty = new BitSet();
        EntryEncoder ee = EntryEncoder.create(kryoPool,2,setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col1");

        Cell insertCol1 = insert(insertTxn,ee.encode());
        Cell ctCol1 = commitTimestamp(insertTxn);

        Txn icol2Txn = newTransaction();
        supplier.commit(icol2Txn.getTxnId());
        icol2Txn = supplier.getTransaction(icol2Txn.getTxnId());

        setCols.clear();
        setCols.set(1);
        ee.reset(setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col2");
        Cell col2 = insert(icol2Txn,ee.encode());
        Cell ctCol2 = commitTimestamp(icol2Txn);

        long mat = insertTxn.getEffectiveCommitTimestamp()+1;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>(2);
        //don't include the ctCol2, to make sure that we create the commit ts for the MAT cell
        compactor.compact(Arrays.asList(ctCol1,col2,insertCol1),data);

        assertListsCorrect(Arrays.asList(ctCol2,ctCol1,col2,insertCol1),data);
    }

    @Test
    public void mergeIgnoresMultipleUpdatesToSameColumn() throws Exception{
        Txn insertTxn = newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        BitSet setCols = new BitSet();
        setCols.set(0);
        BitSet empty = new BitSet();
        EntryEncoder ee = EntryEncoder.create(kryoPool,2,setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col1");

        Cell insertCol1 = insert(insertTxn,ee.encode());
        Cell ctCol1 = commitTimestamp(insertTxn);

        Txn icol2Txn = newTransaction();
        supplier.commit(icol2Txn.getTxnId());
        icol2Txn = supplier.getTransaction(icol2Txn.getTxnId());

        setCols.clear();
        setCols.set(1);
        ee.reset(setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col2");
        Cell col2 = insert(icol2Txn,ee.encode());
        Cell ctCol2 = commitTimestamp(icol2Txn);

        Txn update2Txn = newTransaction();
        supplier.commit(update2Txn.getTxnId());
        update2Txn = supplier.getTransaction(update2Txn.getTxnId());
        ee.reset(setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col2Update");
        Cell update3 = insert(update2Txn,ee.encode());
        Cell ctUpdate3 = commitTimestamp(update2Txn);

        long mat = update2Txn.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>(2);
        compactor.compact(Arrays.asList(ctUpdate3,ctCol2,ctCol1,update3,col2,insertCol1),data);

        setCols.clear();
        setCols.set(0);
        setCols.set(1);
        ee.reset(setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col1").encodeNext("col2Update");
        assertListsCorrect(Arrays.asList(ctUpdate3,insert(update2Txn,ee.encode())),data);
    }

    @Test
    public void addsCommitTimestampSingleCell() throws Exception{
        Txn insertTxn = newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Cell insertCell = insert(insertTxn);
        Cell insertCt = commitTimestamp(insertTxn);

        long mat = insertTxn.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,2,2);

        List<Cell> data = new ArrayList<>(2);
        compactor.compact(Collections.singletonList(insertCell),data);

        assertListsCorrect(Arrays.asList(insertCt,insertCell),data);
    }

    @Test
    public void removesRolledBackDataBelowMat() throws Exception{
        Txn insertTxn = newTransaction();
        supplier.rollback(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Cell insertCell = insert(insertTxn);

        long mat = insertTxn.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,2,2);

        List<Cell> data = new ArrayList<>(2);
        compactor.compact(Collections.singletonList(insertCell),data);

        Assert.assertEquals("Did not remove rolled back data!",0,data.size());
    }

    /* ****************************************************************************************************************/
    /*delete tests*/

    @Test
    public void deletesEverything() throws Exception{
        Txn iTxn = newTransaction();
        supplier.commit(iTxn.getTxnId());
        iTxn = supplier.getTransaction(iTxn.getTxnId());

        Cell i = insert(iTxn);
        Cell iCt = commitTimestamp(iTxn);

        Txn dTxn = newTransaction();
        supplier.commit(dTxn.getTxnId());
        dTxn = supplier.getTransaction(dTxn.getTxnId());

        Cell d = tombstone(dTxn);
        Cell dCt = commitTimestamp(dTxn);

        long mat = dTxn.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> output = new ArrayList<>(0);
        compactor.compact(Arrays.asList(dCt,iCt,d,i),output);

        Assert.assertEquals("Row was not deleted!",0,output.size());
    }

    @Test
    public void ignoresRolledBackDelete() throws Exception{
        Txn iTxn = newTransaction();
        supplier.commit(iTxn.getTxnId());
        iTxn = supplier.getTransaction(iTxn.getTxnId());

        Cell i = insert(iTxn);
        Cell iCt = commitTimestamp(iTxn);

        Txn dTxn = newTransaction();
        supplier.rollback(dTxn.getTxnId());
        dTxn = supplier.getTransaction(dTxn.getTxnId());

        Cell d = tombstone(dTxn);

        long mat = dTxn.getTxnId()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> output = new ArrayList<>(0);
        compactor.compact(Arrays.asList(iCt,d,i),output);

        assertListsCorrect(Arrays.asList(iCt,i),output);
    }

    @Test
    public void doesNotDeleteAcrossMat() throws Exception{
        Txn iTxn = newTransaction();
        supplier.commit(iTxn.getTxnId());
        iTxn = supplier.getTransaction(iTxn.getTxnId());

        Cell i = insert(iTxn);
        Cell iCt = commitTimestamp(iTxn);

        Txn dTxn = newTransaction();

        Cell d = tombstone(dTxn);

        long mat = dTxn.getTxnId();
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> output = new ArrayList<>(0);
        compactor.compact(Arrays.asList(iCt,d,i),output);

        assertListsCorrect(Arrays.asList(iCt,d,i),output);
    }

    /* ****************************************************************************************************************/
    /*anti-tombstone tests*/

    @Test
    public void ignoresRolledBackAntiTombstone() throws Exception{
        Txn iTxn = newTransaction();
        supplier.commit(iTxn.getTxnId());
        iTxn = supplier.getTransaction(iTxn.getTxnId());

        Cell i = insert(iTxn);
        Cell iCt = commitTimestamp(iTxn);

        Txn dTxn = newTransaction();
        supplier.commit(dTxn.getTxnId());
        dTxn = supplier.getTransaction(dTxn.getTxnId());

        Cell d = tombstone(dTxn);
        Cell dCt = commitTimestamp(dTxn);

        Txn i2Txn = newTransaction();
        supplier.rollback(i2Txn.getTxnId());
        i2Txn = supplier.getTransaction(i2Txn.getTxnId());

        Cell i2 = insert(i2Txn);
        Cell at = antiTombstone(i2Txn);

        long mat = i2Txn.getTxnId()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> output = new ArrayList<>(0);
        compactor.compact(Arrays.asList(dCt,iCt,at,d,i2,i),output);

        Assert.assertEquals("Row was not deleted!",0,output.size());
    }

    @Test
    public void doesNotMergeAcrossDelete() throws Exception{
        Txn iTxn = newTransaction();
        supplier.commit(iTxn.getTxnId());
        iTxn = supplier.getTransaction(iTxn.getTxnId());

        BitSet setCols = new BitSet();
        setCols.set(0);
        BitSet empty = new BitSet();
        EntryEncoder ee = EntryEncoder.create(kryoPool,2,setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col1");

        Cell iCol1 = insert(iTxn,ee.encode());
        Cell ctCol1 = commitTimestamp(iTxn);

        Txn dTxn = newTransaction();
        supplier.commit(dTxn.getTxnId());
        dTxn = supplier.getTransaction(dTxn.getTxnId());

        Cell d = tombstone(dTxn);

        Txn i2Txn = newTransaction();
        supplier.commit(i2Txn.getTxnId());
        i2Txn = supplier.getTransaction(i2Txn.getTxnId());

        setCols.clear();
        setCols.set(1);
        ee.reset(setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col2");
        Cell col2 = insert(i2Txn,ee.encode());
        Cell ctCol2 = commitTimestamp(i2Txn);
        Cell at = antiTombstone(i2Txn);

        long mat = i2Txn.getEffectiveCommitTimestamp()+2;
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>(2);
        //don't include the ctCol2, to make sure that we create the commit ts for the MAT cell
        compactor.compact(Arrays.asList(ctCol1,at,d,col2,iCol1),data);

        assertListsCorrect(Arrays.asList(ctCol2,col2),data);
    }

    @Test
    public void doesNotMergeAcrossDeleteAcrossMat() throws Exception{
        Txn iTxn = newTransaction();
        supplier.commit(iTxn.getTxnId());
        iTxn = supplier.getTransaction(iTxn.getTxnId());

        BitSet setCols = new BitSet();
        setCols.set(0);
        BitSet empty = new BitSet();
        EntryEncoder ee = EntryEncoder.create(kryoPool,2,setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col1");

        Cell iCol1 = insert(iTxn,ee.encode());
        Cell ctCol1 = commitTimestamp(iTxn);

        Txn dTxn = newTransaction();
        supplier.commit(dTxn.getTxnId());
        dTxn = supplier.getTransaction(dTxn.getTxnId());

        Cell d = tombstone(dTxn);

        Txn i2Txn = newTransaction();

        setCols.clear();
        setCols.set(1);
        ee.reset(setCols,empty,empty,empty);
        ee.getEntryEncoder().encodeNext("col2");
        Cell col2 = insert(i2Txn,ee.encode());
        Cell at = antiTombstone(i2Txn);

        long mat = i2Txn.getTxnId();
        MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

        List<Cell> data = new ArrayList<>(2);
        //don't include the ctCol2, to make sure that we create the commit ts for the MAT cell
        compactor.compact(Arrays.asList(ctCol1,at,d,col2,iCol1),data);

        /*
         * In this situation, the anti-tombstone is above the MAT, but the delete
         * is below it. This means that the anti-tombstone cell can't be deleted (it's above
         * the MAT), but the tombstone can (it's immediately below the MAT). This means that
         * we end up with an anti-tombstone cell which is NOT immediately followed by a delete. It's
         * a bit of a waste of space, but it won't cause any problems: reading an anti-tombstone at
         * the bottom-most cell of data won't be problematic, and the next time we compact it'll (hopefully) have
         * advanced the MAT such that the anti-tombstone is below the MAT and gets removed.
         */
        assertListsCorrect(Arrays.asList(at,col2),data);
    }
    /* ****************************************************************************************************************/
    /*private helper methods*/

    private Txn newTransaction() throws IOException{
        return newTransaction(tsGen.nextTimestamp());
    }

    private Txn newTransaction(long ts) throws IOException{
        Txn txn = new WritableTxn(ts,ts,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                Txn.ROOT_TRANSACTION,UnsupportedLifecycleManager.INSTANCE,false,HExceptionFactory.INSTANCE);
        supplier.recordNewTransaction(txn);
        return txn;
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
