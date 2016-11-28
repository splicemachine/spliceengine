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

import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.impl.UnsupportedLifecycleManager;
import com.splicemachine.si.impl.store.TestingTimestampSource;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.WritableTxn;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.splicemachine.si.impl.server.MVCCDataUtils.*;

/**
 * Tests related to behavior of the MVCC compactor when above the MAT
 *
 * @author Scott Fines
 *         Date: 11/23/16
 */
public class MVCCCompactorAboveMatTest{
    private static final TestingTimestampSource tsGen=new TestingTimestampSource(10L);

    private final TestingTxnStore supplier = new TestingTxnStore(SystemClock.INSTANCE,
            tsGen,
            HExceptionFactory.INSTANCE,Long.MAX_VALUE);
    private final long mat = 5L;
    private final MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

    /* ****************************************************************************************************************/
    /*insert only tests*/
    @Test
    public void doesNotRemoveInserts() throws Exception{
        Txn dataTxn=newTransaction();

        Cell dataCell=insert(dataTxn);
        List<Cell> output = new ArrayList<>(1);
        compactor.compact(Collections.singletonList(dataCell),output);

        Assert.assertEquals("Incorrect output list size!",1,output.size());
        Assert.assertEquals("Incorrect returned cell!",dataCell,output.get(0));
    }

    @Test
    public void addsCommitTimestampToInsert() throws Exception{
        Txn dataTxn=newTransaction();
        supplier.commit(dataTxn.getTxnId());
        dataTxn = supplier.getTransaction(dataTxn.getTxnId());

        Cell dataCell=insert(dataTxn);
        Cell commitCell = commitTimestamp(dataTxn);
        List<Cell> output = new ArrayList<>(2);
        compactor.compact(Collections.singletonList(dataCell),output);

        Assert.assertEquals("Incorrect output list size!",2,output.size());
        assertListsCorrect(Arrays.asList(commitCell,dataCell),output);
    }

    @Test
    public void removesRolledBackInsert() throws Exception{
        Txn dataTxn=newTransaction();
        supplier.rollback(dataTxn.getTxnId());
        dataTxn = supplier.getTransaction(dataTxn.getTxnId());

        Cell dataCell=insert(dataTxn);
        List<Cell> output = new ArrayList<>(0);
        compactor.compact(Collections.singletonList(dataCell),output);

        Assert.assertEquals("Incorrect output list size!",0,output.size());
    }

    @Test
    public void doesNotRemoveInsertsWithCommitTimestamp() throws Exception{

        Txn dataTxn=newTransaction();
        supplier.commit(dataTxn.getTxnId());
        dataTxn = supplier.getTransaction(dataTxn.getTxnId());

        Cell dataCell=insert(dataTxn);

        Cell commitTs =commitTimestamp(dataTxn);

        List<Cell> output = new ArrayList<>(1);
        compactor.compact(Arrays.asList(commitTs,dataCell),output);

        Assert.assertEquals("Incorrect output list size!",2,output.size());
        Assert.assertEquals("Incorrect returned commit cell!",commitTs,output.get(0));
        Assert.assertEquals("Incorrect returned user cell!",dataCell,output.get(1));
    }

    /* ****************************************************************************************************************/
    /*delete tests*/
    @Test
    public void doesNotRemoveDeletes() throws Exception{
        Txn insertTxn=newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Txn deleteTxn =newTransaction();


        Cell insert=insert(insertTxn);
        Cell insertCommit =commitTimestamp(insertTxn);
        Cell delete=tombstone(deleteTxn);

        List<Cell> output = new ArrayList<>(1);
        List<Cell> input=Arrays.asList(insertCommit,delete,insert);
        input.sort(KeyValue.COMPARATOR);
        compactor.compact(input,output);

        assertListsCorrect(input,output);
    }

    @Test
    public void removesRolledBackDeletes() throws Exception{

        Txn insertTxn=newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Txn deleteTxn =newTransaction();
        supplier.rollback(deleteTxn.getTxnId());
        deleteTxn = supplier.getTransaction(deleteTxn.getTxnId());


        Cell insert=insert(insertTxn);
        Cell insertCommit =commitTimestamp(insertTxn);
        Cell delete=tombstone(deleteTxn);

        List<Cell> output = new ArrayList<>(2);
        List<Cell> input=Arrays.asList(insertCommit,delete,insert);
        input.sort(KeyValue.COMPARATOR);
        compactor.compact(input,output);

        assertListsCorrect(Arrays.asList(insertCommit,insert),output);
    }

    @Test
    public void addsCommitTimestampToDelete() throws Exception{

        Txn insertTxn=newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Txn deleteTxn =newTransaction();
        supplier.commit(deleteTxn.getTxnId());
        deleteTxn = supplier.getTransaction(deleteTxn.getTxnId());


        Cell insert=insert(insertTxn);
        Cell insertCommit =commitTimestamp(insertTxn);
        Cell delete=tombstone(deleteTxn);
        Cell deleteCt = commitTimestamp(deleteTxn);

        List<Cell> output = new ArrayList<>(4);
        List<Cell> input=Arrays.asList(insertCommit,delete,insert);
        input.sort(KeyValue.COMPARATOR);
        compactor.compact(input,output);

        assertListsCorrect(Arrays.asList(deleteCt,insertCommit,delete,insert),output);
    }

    @Test
    public void doesNotRemoveDeletesWithCommitTimestamp() throws Exception{

        Txn insertTxn=newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Txn deleteTxn = newTransaction();
        supplier.commit(deleteTxn.getTxnId());
        deleteTxn = supplier.getTransaction(deleteTxn.getTxnId());

        Cell insert=insert(insertTxn);

        Cell insertCommit =commitTimestamp(insertTxn);

        Cell delete=tombstone(deleteTxn);

        Cell deleteCommit =commitTimestamp(deleteTxn);

        List<Cell> output = new ArrayList<>(1);
        List<Cell> input=Arrays.asList(deleteCommit,insertCommit,delete,insert);
        input.sort(KeyValue.COMPARATOR);
        compactor.compact(input,output);

        assertListsCorrect(input,output);
    }

    /* ****************************************************************************************************************/
    /*anti-tombstone tests*/
    @Test
    public void doesNotDeleteActiveAntiTombstones() throws Exception{
        Txn insertTxn=newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Txn deleteTxn =newTransaction();
        supplier.commit(deleteTxn.getTxnId());
        deleteTxn = supplier.getTransaction(deleteTxn.getTxnId());

        Txn aTTxn = newTransaction();


        Cell insert=insert(insertTxn);
        Cell insertCommit =commitTimestamp(insertTxn);
        Cell delete=tombstone(deleteTxn);
        Cell deleteCt = commitTimestamp(deleteTxn);
        Cell at = antiTombstone(aTTxn);
        Cell atInsert = insert(aTTxn);

        List<Cell> output = new ArrayList<>(5);
        List<Cell> input=Arrays.asList(deleteCt,insertCommit,at,delete,atInsert,insert);
        input.sort(KeyValue.COMPARATOR);
        compactor.compact(input,output);

        assertListsCorrect(input,output);
    }

    @Test
    public void keepsCommittedAntiTombstone() throws Exception{
        Txn insertTxn=newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Txn deleteTxn =newTransaction();
        supplier.commit(deleteTxn.getTxnId());
        deleteTxn = supplier.getTransaction(deleteTxn.getTxnId());

        Txn aTTxn = newTransaction();
        supplier.commit(aTTxn.getTxnId());
        aTTxn = supplier.getTransaction(aTTxn.getTxnId());


        Cell insert=insert(insertTxn);
        Cell insertCommit =commitTimestamp(insertTxn);
        Cell delete=tombstone(deleteTxn);
        Cell deleteCt = commitTimestamp(deleteTxn);
        Cell at = antiTombstone(aTTxn);
        Cell atInsert = insert(aTTxn);
        Cell atCt = commitTimestamp(aTTxn);

        List<Cell> output = new ArrayList<>(6);
        List<Cell> input=Arrays.asList(atCt,deleteCt,insertCommit,at,delete,atInsert,insert);
        input.sort(KeyValue.COMPARATOR);
        compactor.compact(input,output);

        assertListsCorrect(input,output);
    }

    @Test
    public void addsCommitTimestampToAntiTombstone() throws Exception{
        Txn insertTxn=newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Txn deleteTxn =newTransaction();
        supplier.commit(deleteTxn.getTxnId());
        deleteTxn = supplier.getTransaction(deleteTxn.getTxnId());

        Txn aTTxn = newTransaction();
        supplier.commit(aTTxn.getTxnId());
        aTTxn = supplier.getTransaction(aTTxn.getTxnId());


        Cell insert=insert(insertTxn);
        Cell insertCommit =commitTimestamp(insertTxn);
        Cell delete=tombstone(deleteTxn);
        Cell deleteCt = commitTimestamp(deleteTxn);
        Cell at = antiTombstone(aTTxn);
        Cell atInsert = insert(aTTxn);
        Cell atCt = commitTimestamp(aTTxn);

        List<Cell> output = new ArrayList<>(6);
        List<Cell> input=Arrays.asList(deleteCt,insertCommit,at,delete,atInsert,insert);
        List<Cell> correctOutput=Arrays.asList(atCt,deleteCt,insertCommit,at,delete,atInsert,insert);
        input.sort(KeyValue.COMPARATOR);
        compactor.compact(input,output);

        assertListsCorrect(correctOutput,output);
    }


    @Test
    public void removesRolledbackAntiTombstone() throws Exception{
        Txn insertTxn=newTransaction();
        supplier.commit(insertTxn.getTxnId());
        insertTxn = supplier.getTransaction(insertTxn.getTxnId());

        Txn deleteTxn =newTransaction();
        supplier.commit(deleteTxn.getTxnId());
        deleteTxn = supplier.getTransaction(deleteTxn.getTxnId());

        Txn aTTxn = newTransaction();
        supplier.rollback(aTTxn.getTxnId());
        aTTxn = supplier.getTransaction(aTTxn.getTxnId());

        Cell insertCell=insert(insertTxn);
        Cell insertCommit =commitTimestamp(insertTxn);
        Cell delete=tombstone(deleteTxn);
        Cell deleteCt = commitTimestamp(deleteTxn);
        Cell at = antiTombstone(aTTxn);
        Cell atInsert = insert(aTTxn);

        List<Cell> output = new ArrayList<>(5);
        List<Cell> input=Arrays.asList(deleteCt,insertCommit,at,delete,atInsert,insertCell);
        input.sort(KeyValue.COMPARATOR);
        compactor.compact(input,output);

        List<Cell> correctOutput=Arrays.asList(deleteCt,insertCommit,delete,insertCell);
        assertListsCorrect(correctOutput,output);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private Txn newTransaction() throws IOException{
        long ts=tsGen.nextTimestamp();
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