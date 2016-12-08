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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;


import static com.splicemachine.si.impl.server.MVCCDataUtils.*;

/**
 * @author Scott Fines
 *         Date: 12/5/16
 */
public class MVCCCompactorMultiRowAboveMatTest{

    private static final TestingTimestampSource tsGen=new TestingTimestampSource(10L);

    private final TestingTxnStore supplier = new TestingTxnStore(SystemClock.INSTANCE,
            tsGen,
            HExceptionFactory.INSTANCE,Long.MAX_VALUE);
    private final long mat = 5L;
    private final MVCCCompactor compactor = new MVCCCompactor(supplier,mat,1,2);

    @Test
    public void compactsMultipleInsertsCorrectly() throws Exception{
        Txn d1 = newTransaction();

        List<Cell> row1 =Collections.singletonList(insert(d1,"row1"));
        Txn d2 = committedTransaction();
        List<Cell> row2 = Arrays.asList(commitTimestamp(d2,"row2"),insert(d2,"row2"));

        List<Cell> output = new ArrayList<>();
        compactor.compact(row1,output);
        assertListsCorrect(row1,output);
        output.clear();
        compactor.compact(row2,output);
        assertListsCorrect(row2,output);
    }

    @Test
    public void rolledBackRowDoesNotDeleteOtherRow() throws Exception{
        //also tests that we add a commit timestamp to the second row, just for funsies
        Txn d1 = rolledBackTransaction();

        List<Cell> row1 =Collections.singletonList(insert(d1,"row1"));
        Txn d2 = committedTransaction();
        List<Cell> row2 =Collections.singletonList(insert(d2,"row2"));

        List<Cell> output = new ArrayList<>();
        compactor.compact(row1,output);
        Assert.assertEquals("Did not delete rolled back data!",0,output.size());
        output.clear();
        compactor.compact(row2,output);

        assertListsCorrect(Arrays.asList(commitTimestamp(d2,"row2"),row2.get(0)),output);
    }

    @Test
    public void doesNotRemoveDeletes() throws Exception{
        Txn i1 = committedTransaction();
        Txn d1 = newTransaction();

        List<Cell> row1 =new ArrayList<>();row1.add(tombstone(d1,"row1"));row1.add(insert(i1,"row1"));
        Txn i2 = committedTransaction();
        List<Cell> row2 = Arrays.asList(commitTimestamp(i2,"row2"),insert(i2,"row2"));

        List<Cell> output = new ArrayList<>();
        row1.add(0,commitTimestamp(i1,"row1"));
        compactor.compact(row1,output);
        assertListsCorrect(row1,output);
        output.clear();

        compactor.compact(row2,output);
        assertListsCorrect(row2,output);
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
