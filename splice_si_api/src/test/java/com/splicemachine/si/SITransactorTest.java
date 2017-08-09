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

import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.ReadOnlyModificationException;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.api.txn.lifecycle.CannotCommitException;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.testenv.*;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.hamcrest.core.IsInstanceOf;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.spark_project.guava.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
@Category(ArchitectureSpecific.class)
public class SITransactorTest {
    @Rule public ExpectedException error = ExpectedException.none();
    private boolean useSimple = true;
    private static SITestEnv testEnv;
    private static TestTransactionSetup transactorSetup;
    private TxnLifecycleManager control;
    private TransactorTestUtility testUtility;
    private final List<Txn> createdParentTxns = Lists.newArrayList();
    private TransactionStore txnStore;

    @SuppressWarnings("unchecked")
    private void baseSetUp() {
        System.out.println("transactorSetup" + "-->" + transactorSetup);
        control = new ForwardingLifecycleManager(transactorSetup.txnLifecycleManager) {
            @Override
            protected void afterStart(Txn txn) {
                createdParentTxns.add(txn);
            }
        };
        testUtility = new TransactorTestUtility(useSimple,testEnv, transactorSetup);
        txnStore = transactorSetup.txnStore;
    }

    @Before
    public void setUp() throws IOException {
        if(testEnv==null){
            testEnv =SITestEnvironment.loadTestEnvironment();
            System.out.println("testEnv" + "-->" + testEnv);
            transactorSetup = new TestTransactionSetup(testEnv,true);
            System.out.println("transactorSetup" + "-->" + transactorSetup);
        }
        baseSetUp();
    }

    @After
    public void tearDown() throws Exception {
        txnStore.rollback(createdParentTxns.toArray(new Txn[createdParentTxns.size()]));
    }

    @Test
    public void writeRead() throws IOException {
        //UnsafeRow row = new UnsafeRow(1);


        Txn t1 = control.beginTransaction();
        System.out.println("control -> " + control);
        System.out.println("Txn -> " + t1);
        t1 = control.elevateTransaction(t1);
        System.out.println("elevated?");
//				Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe9 absent", testUtility.read(t1, "joe9"));
        System.out.println("attempt insert?");
        testUtility.insertAge(t1, "joe9", 20);
        System.out.println("after insert?");
        Assert.assertEquals("joe9 age=20 job=null", testUtility.read(t1, "joe9"));
        System.out.println("begin commit?");
        control.commit(t1);
        System.out.println("after commit?");
        Txn t2 = control.beginTransaction();
        try {
            Assert.assertEquals("joe9 age=20 job=null", testUtility.read(t2, "joe9"));
        } finally {
            control.commit(t2);
        }
    }

    @Test
    @Ignore
    public void writeReadOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = elevate(t1);
        Assert.assertEquals("joe8 absent", testUtility.read(t1, "joe8"));
        testUtility.insertAge(t1, "joe8", 20);
        Assert.assertEquals("joe8 age=20 job=null", testUtility.read(t1, "joe8"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe8 age=20 job=null", testUtility.read(t1, "joe8"));
        Assert.assertEquals("joe8 absent", testUtility.read(t2, "joe8"));
        commit(t1);
        Assert.assertEquals("joe8 absent", testUtility.read(t2, "joe8"));
    }

    /*
    JL TODO
    @Test
    public void testGetActiveTransactionsFiltersOutChildrenCommit() throws Exception {
        Txn parent = control.beginTransaction();
        Txn child = control.beginChildTransaction(parent, parent.getIsolationLevel());
        long[] activeTxns = txnStore.getActiveTransactionIds(child, null);
        boolean foundParent = false;
        boolean foundChild = false;
        for(long activeTxn:activeTxns){
           if(activeTxn==parent.getTxnId())
               foundParent = true;
            if(activeTxn==child.getTxnId())
                foundChild=true;
            if(foundParent&&foundChild) break;
        }
        Assert.assertTrue("Missing parent txn!",foundParent);
        Assert.assertTrue("Missing child txn!",foundChild);

        parent.commit();
        activeTxns = txnStore.getActiveTransactionIds(child, null);
        for(long activeTxn:activeTxns){
            Assert.assertNotEquals("Parent txn still included!",parent.getTxnId(),activeTxn);
            Assert.assertNotEquals("Child txn still included!",child.getTxnId(),activeTxn);
        }

        Txn next = control.beginTransaction();
        activeTxns = txnStore.getActiveTransactionIds(next, null);
        boolean found=false;
        for(long activeTxn:activeTxns){
            if(activeTxn==next.getTxnId()){
                found=true;
                break;
            }
        }
        Assert.assertTrue("Did not find new transaction!",found);
    }
    */

    @Test
    @Ignore
    public void testChildTransactionsInterleave() throws Exception {
                /*
				 * Similar to what happens when an insertion occurs,
				 */
        Txn parent = control.beginTransaction();

        Txn interleave = control.beginTransaction();

        Txn child = control.beginChildTransaction(parent); //make it writable

        testUtility.insertAge(child, "joe", 20);

        Assert.assertEquals("joe absent", testUtility.read(interleave, "joe"));
    }

    @Test
    @Ignore
    public void testTwoChildTransactionTreesIntervleavedWritesAndReads() throws Exception {
        Txn p1 = control.beginTransaction();
        Txn c1 = control.beginChildTransaction(p1);
        Txn g1 = control.beginChildTransaction(c1);

        testUtility.insertAge(g1, "tickle", 20);
        commit(g1);
        commit(c1);

        Txn p2 = control.beginTransaction();
        commit(p1);
        Txn c2 = control.beginChildTransaction(p2);
        Txn g2 = control.beginChildTransaction(c2);

        Assert.assertEquals("tickle absent", testUtility.read(g2, "tickle"));
    }

    @Test
    @Ignore
    public void testTwoChildTransactionTreesIntervleavedWritesAndWrites() throws Throwable {
        Txn p1 = control.beginTransaction();
        Txn c1 = control.beginChildTransaction(p1);
        Txn g1 = control.beginChildTransaction(c1);

        testUtility.insertAge(g1, "tickle2", 20);
        commit(g1);
        commit(c1);

        Txn p2 = control.beginTransaction();
        commit(p1);
        Txn c2 = control.beginChildTransaction(p2);
        Txn g2 = control.beginChildTransaction(c2);

        try {
            testUtility.insertAge(g2, "tickle2", 22);
        } catch (IOException re) {
            testUtility.assertWriteConflict(re);
        }
    }

    /*
    @Test
    public void testCanRecordWriteTable() throws Exception {
        Txn parent = control.beginTransaction();
        Txn transaction = txnStore.getTransaction(parent.getTxnId());
        Iterator<ByteSlice> destinationTables = transaction.getDestinationTables();
        Assert.assertArrayEquals("Incorrect write table!", , destinationTables.next().getByteCopy());
    }
    */

    @Test
    @Ignore
    public void writeWrite() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20 job=null", testUtility.read(t1, "joe"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe age=20 job=null", testUtility.read(t2, "joe"));
        t2 = elevate(t2);
        testUtility.insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30 job=null", testUtility.read(t2, "joe"));
        commit(t2);
    }

    @Test//(expected = CannotCommitException.class)
    @Ignore
    public void writeWriteOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe012 absent", testUtility.read(t1, "joe012"));
        t1 = elevate(t1);
        testUtility.insertAge(t1, "joe012", 20);
        Assert.assertEquals("joe012 age=20 job=null", testUtility.read(t1, "joe012"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe012 age=20 job=null", testUtility.read(t1, "joe012"));
        Assert.assertEquals("joe012 absent", testUtility.read(t2, "joe012"));
        t2 = elevate(t2);
        try {
            testUtility.insertAge(t2, "joe012", 30);
            Assert.fail("was able to insert age");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            rollback(t2);
        }
        Assert.assertEquals("joe012 age=20 job=null", testUtility.read(t1, "joe012"));
        commit(t1);
        error.expect(IsInstanceOf.instanceOf(CannotCommitException.class));
        commit(t2); //should not work, probably need to change assertion
        Assert.fail("Was able to commit a rolled back transaction");
    }

    @Test
    @Ignore
    public void writeWriteOverlapRecovery() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe142 absent", testUtility.read(t1, "joe142"));
        t1 = elevate(t1);
        testUtility.insertAge(t1, "joe142", 20);
        Assert.assertEquals("joe142 age=20 job=null", testUtility.read(t1, "joe142"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe142 age=20 job=null", testUtility.read(t1, "joe142"));
        Assert.assertEquals("joe142 absent", testUtility.read(t2, "joe142"));
        t2 = elevate(t2);
        try {
            testUtility.insertAge(t2, "joe142", 30);
            Assert.fail();
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        }
        // can still use a transaction after a write conflict
        testUtility.insertAge(t2, "bob142", 30);
        Assert.assertEquals("bob142 age=30 job=null", testUtility.read(t2, "bob142"));
        Assert.assertEquals("joe142 age=20 job=null", testUtility.read(t1, "joe142"));
        commit(t1);
        commit(t2);
    }

    @Test
    @Ignore
    public void readAfterCommit() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe3", 20);
        commit(t1);
        Assert.assertEquals("joe3 age=20 job=null", testUtility.read(t1, "joe3"));
    }

    @Test
    @Ignore
    public void testCannotInsertUsingReadOnlyTransaction() throws Throwable {
        Txn t1 = control.beginTransaction();
        try{
            testUtility.insertAge(t1,"scott2",20);
            Assert.fail("Was able to insert age!");
        }catch(IOException ioe){
            ioe =testEnv.getExceptionFactory().processRemoteException(ioe);
            Assert.assertTrue("Incorrect exception",ReadOnlyModificationException.class.isAssignableFrom(ioe.getClass()));
        }
    }

    @Test
    @Ignore
    public void testRollingBackAfterCommittingDoesNothing() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = elevate(t1);
        testUtility.insertAge(t1, "joe50", 20);
        commit(t1);
        rollback(t1);
        Assert.assertEquals("joe50 age=20 job=null", testUtility.read(t1, "joe50"));
    }

    @Test
    @Ignore
    public void writeScan() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe4 absent", testUtility.read(t1, "joe4"));
        testUtility.insertAge(t1, "joe4", 20);
        Assert.assertEquals("joe4 age=20 job=null", testUtility.read(t1, "joe4"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe4 age=20 job=null[ V.age@~9=20 ]", testUtility.scan(t2, "joe4"));
        Assert.assertEquals("joe4 age=20 job=null", testUtility.read(t2, "joe4"));
    }

    @Test
    @Ignore
    public void writeScanWithDeleteActive() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe128", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();

        Txn t3 = control.beginTransaction();

        testUtility.deleteRow(t2, "joe128");

        Assert.assertEquals("joe128 age=20 job=null[ V.age@~9=20 ]", testUtility.scan(t3, "joe128"));
        commit(t2);
    }

    @Test
    @Ignore
    public void writeDeleteScanNoColumns() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe85", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "joe85");
        commit(t2);


        Txn t3 = control.beginTransaction();
        // reading si only (i.e. no data columns) returns the rows but none of the "user" data
        Assert.assertEquals("", testUtility.scanNoColumns(t3, "joe85", true));
    }

    @Test
    @Ignore
    public void writeScanMultipleRows() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "17joe", 20);
        testUtility.insertAge(t1, "17bob", 30);
        testUtility.insertAge(t1, "17boe", 40);
        testUtility.insertAge(t1, "17tom", 50);
        commit(t1);

        Txn t2 = control.beginTransaction();
        String expected = "17bob age=30 job=null[ V.age@~9=30 ]\n" +
                "17boe age=40 job=null[ V.age@~9=40 ]\n" +
                "17joe age=20 job=null[ V.age@~9=20 ]\n" +
                "17tom age=50 job=null[ V.age@~9=50 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t2, "17a", "17z", null));
    }

    @Test
    @Ignore
    public void writeScanWithFilter() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "91joe", 20);
        testUtility.insertAge(t1, "91bob", 30);
        testUtility.insertAge(t1, "91boe", 40);
        testUtility.insertAge(t1, "91tom", 50);
        commit(t1);

        Txn t2 = control.beginTransaction();
        String expected = "91boe age=40 job=null[ V.age@~9=40 ]\n";
        if (!useSimple) {
            Assert.assertEquals(expected, testUtility.scanAll(t2, "91a", "91z", 40));
        }
    }

    @Test
    @Ignore
    public void writeScanWithFilterAndPendingWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "92joe", 20);
        testUtility.insertAge(t1, "92bob", 30);
        testUtility.insertAge(t1, "92boe", 40);
        testUtility.insertAge(t1, "92tom", 50);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.insertAge(t2, "92boe", 41);

        Txn t3 = control.beginTransaction();
        String expected = "92boe age=40 job=null[ V.age@~9=40 ]\n";
        if (!useSimple) {
            Assert.assertEquals(expected, testUtility.scanAll(t3, "92a", "92z", 40));
        }
    }

    @Test
    @Ignore
    public void writeWriteRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe5", 20);
        Assert.assertEquals("joe5 age=20 job=null", testUtility.read(t1, "joe5"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=null", testUtility.read(t2, "joe5"));
        testUtility.insertJob(t2, "joe5", "baker");
        Assert.assertEquals("joe5 age=20 job=baker", testUtility.read(t2, "joe5"));
        commit(t2);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=baker", testUtility.read(t3, "joe5"));
    }

    @Test
    @Ignore
    public void multipleWritesSameTransaction() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe16 absent", testUtility.read(t1, "joe16"));
        t1 = elevate(t1);
        testUtility.insertAge(t1, "joe16", 20);
        Assert.assertEquals("joe16 age=20 job=null", testUtility.read(t1, "joe16"));

        testUtility.insertAge(t1, "joe16", 21);
        Assert.assertEquals("joe16 age=21 job=null", testUtility.read(t1, "joe16"));

        testUtility.insertAge(t1, "joe16", 22);
        Assert.assertEquals("joe16 age=22 job=null", testUtility.read(t1, "joe16"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe16 age=22 job=null", testUtility.read(t2, "joe16"));
        commit(t2);
    }

    @Test
    @Ignore
    public void manyWritesManyRollbacksRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe6", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.insertJob(t2, "joe6", "baker");
        commit(t2);

        Txn t3 = control.beginTransaction();
        testUtility.insertJob(t3, "joe6", "butcher");
        commit(t3);

        Txn t4 = control.beginTransaction();
        testUtility.insertJob(t4, "joe6", "blacksmith");
        commit(t4);

        Txn t5 = control.beginTransaction();
        testUtility.insertJob(t5, "joe6", "carter");
        commit(t5);

        Txn t6 = control.beginTransaction();
        testUtility.insertJob(t6, "joe6", "farrier");
        commit(t6);

        Txn t7 = control.beginTransaction();
        testUtility.insertAge(t7, "joe6", 27);
        rollback(t7);

        Txn t8 = control.beginTransaction();
        testUtility.insertAge(t8, "joe6", 28);
        rollback(t8);

        Txn t9 = control.beginTransaction();
        testUtility.insertAge(t9, "joe6", 29);
        rollback(t9);

        Txn t10 = control.beginTransaction();
        testUtility.insertAge(t10, "joe6", 30);
        rollback(t10);

        Txn t11 = control.beginTransaction();
        Assert.assertEquals("joe6 age=20 job=farrier", testUtility.read(t11, "joe6"));
        commit(t11);
    }

    @Test
    @Ignore
    public void writeDelete() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe10", 20);
        Assert.assertEquals("joe10 age=20 job=null", testUtility.read(t1, "joe10"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe10 age=20 job=null", testUtility.read(t2, "joe10"));
        t2 = elevate(t2);
        testUtility.deleteRow(t2, "joe10");
        Assert.assertEquals("joe10 absent", testUtility.read(t2, "joe10"));
        commit(t2);
    }

    @Test
    @Ignore
    public void writeDeleteRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe11", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "joe11");
        commit(t2);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe11 absent", testUtility.read(t3, "joe11"));
        commit(t3);
    }

    @Test
    @Ignore
    public void writeDeleteRollbackRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe90", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "joe90");
        rollback(t2);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe90 age=20 job=null", testUtility.read(t3, "joe90"));
        commit(t3);
    }

    @Test
    @Ignore
    public void writeChildDeleteParentRollbackDelete() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe93", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.deleteRow(t3, "joe93");
        rollback(t2);

        Txn t4 = control.beginTransaction();
        testUtility.deleteRow(t4, "joe93");
        Assert.assertEquals("joe93 absent", testUtility.read(t4, "joe93"));
        commit(t4);
    }

    @Test
    @Ignore
    public void writeDeleteOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = elevate(t1);
        testUtility.insertAge(t1, "joe2", 20);

        Txn t2 = control.beginTransaction();
        try {
            testUtility.deleteRow(t2, "joe2");
            Assert.fail("No Write conflict was detected!");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            rollback(t2);
        }
        Assert.assertEquals("joe2 age=20 job=null", testUtility.read(t1, "joe2"));
        commit(t1);
        error.expect(IsInstanceOf.instanceOf(CannotCommitException.class));
       // error.expectMessage(String.format("[%1$d]Txn %1$d cannot be committed--it is in the %2$s state",t2.getTxnId(),Txn.State.ROLLEDBACK));
        commit(t2);
        Assert.fail("Did not throw CannotCommit exception!");
    }

    @Test
    @Ignore
    public void writeWriteDeleteOverlap() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe013", 20);
        commit(t0);

        Txn t1 = control.beginTransaction();
        testUtility.deleteRow(t1, "joe013");

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe013 age=20 job=null",testUtility.read(t2,"joe013"));
        try {
            testUtility.insertAge(t2, "joe013", 21);
            Assert.fail("No WriteConflict thrown!");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            rollback(t2);
        }
        Assert.assertEquals("joe013 absent", testUtility.read(t1, "joe013"));
        commit(t1);
        try {
            commit(t2);
            Assert.fail();
        } catch (IOException dnrio) {
            Assert.assertTrue("Was not a CannotCommitException!",dnrio instanceof CannotCommitException);
            CannotCommitException cce = (CannotCommitException)dnrio;
            Assert.assertEquals("Incorrect transaction id!",t2.getTxnId(),cce.getTxnId());
//            Assert.assertEquals("Incorrect cannot-commit state",Txn.State.ROLLEDBACK,cce.getActualState());
        }

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe013 absent", testUtility.read(t3, "joe013"));
        commit(t3);
    }

    @Test
    @Ignore
    public void writeWriteDeleteWriteRead() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe14", 20);
        commit(t0);

        Txn t1 = control.beginTransaction();
        testUtility.insertJob(t1, "joe14", "baker");
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "joe14");
        commit(t2);

        Txn t3 = control.beginTransaction();
        testUtility.insertJob(t3, "joe14", "smith");
        Assert.assertEquals("joe14 age=null job=smith", testUtility.read(t3, "joe14"));
        commit(t3);

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe14 age=null job=smith", testUtility.read(t4, "joe14"));
        commit(t4);
    }

    @Test
    @Ignore
    public void writeWriteDeleteWriteDeleteWriteRead() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe15", 20);
        commit(t0);

        Txn t1 = control.beginTransaction();
        testUtility.insertJob(t1, "joe15", "baker");
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "joe15");
        commit(t2);

        Txn t3 = control.beginTransaction();
        testUtility.insertJob(t3, "joe15", "smith");
        Assert.assertEquals("joe15 age=null job=smith", testUtility.read(t3, "joe15"));
        commit(t3);

        Txn t4 = control.beginTransaction();
        testUtility.deleteRow(t4, "joe15");
        commit(t4);

        Txn t5 = control.beginTransaction();
        testUtility.insertAge(t5, "joe15", 21);
        commit(t5);

        Txn t6 = control.beginTransaction();
        Assert.assertEquals("joe15 age=21 job=null", testUtility.read(t6, "joe15"));
        commit(t6);
    }

    @Test
    @Ignore
    public void writeManyDeleteOneGets() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe47", 20);
        testUtility.insertAge(t1, "toe47", 30);
        testUtility.insertAge(t1, "boe47", 40);
        testUtility.insertAge(t1, "moe47", 50);
        testUtility.insertAge(t1, "zoe47", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "moe47");
        commit(t2);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe47 age=20 job=null", testUtility.read(t3, "joe47"));
        Assert.assertEquals("toe47 age=30 job=null", testUtility.read(t3, "toe47"));
        Assert.assertEquals("boe47 age=40 job=null", testUtility.read(t3, "boe47"));
        Assert.assertEquals("moe47 absent", testUtility.read(t3, "moe47"));
        Assert.assertEquals("zoe47 age=60 job=null", testUtility.read(t3, "zoe47"));
    }

    @Test
    @Ignore
    public void writeManyDeleteOneScan() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "48joe", 20);
        testUtility.insertAge(t1, "48toe", 30);
        testUtility.insertAge(t1, "48boe", 40);
        testUtility.insertAge(t1, "48moe", 50);
        testUtility.insertAge(t1, "48xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "48moe");
        commit(t2);

        Txn t3 = control.beginTransaction();
        String expected = "48boe age=40 job=null[ V.age@~9=40 ]\n" +
                "48joe age=20 job=null[ V.age@~9=20 ]\n" +
                "48toe age=30 job=null[ V.age@~9=30 ]\n" +
                "48xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "48a", "48z", null));
    }

    @Test
    @Ignore
    public void writeManyDeleteOneScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "110joe", 20);
        testUtility.insertAge(t1, "110toe", 30);
        testUtility.insertAge(t1, "110boe", 40);
        testUtility.insertAge(t1, "110moe", 50);
        testUtility.insertAge(t1, "110xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "110moe");
        commit(t2);

        Txn t3 = control.beginTransaction();
        String expected = "110boe age=40 job=null[ V.age@~9=40 ]\n" +
                "110joe age=20 job=null[ V.age@~9=20 ]\n" +
                "110toe age=30 job=null[ V.age@~9=30 ]\n" +
                "110xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "110a", "110z", null));
    }


    @Test
    @Ignore
    public void writeManyDeleteOneScanWithIncludeSIColumnSameTransaction() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = elevate(t1);
        testUtility.insertAge(t1, "143joe", 20);
        testUtility.insertAge(t1, "143toe", 30);
        testUtility.insertAge(t1, "143boe", 40);
        testUtility.insertAge(t1, "143moe", 50);
        testUtility.insertAge(t1, "143xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "143moe");
        String expected = "143boe age=40 job=null[ V.age@~9=40 ]\n" +
                "143joe age=20 job=null[ V.age@~9=20 ]\n" +
                "143toe age=30 job=null[ V.age@~9=30 ]\n" +
                "143xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t2, "143a", "143z", null));
    }

    @Test
    @Ignore
    public void writeManyDeleteOneSameTransactionScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "135joe", 20);
        testUtility.insertAge(t1, "135toe", 30);
        testUtility.insertAge(t1, "135boe", 40);
        testUtility.insertAge(t1, "135xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.insertAge(t1, "135moe", 50);
        testUtility.deleteRow(t2, "135moe");
        commit(t2);

        Txn t3 = control.beginTransaction();
        String expected = "135boe age=40 job=null[ V.age@~9=40 ]\n" +
                "135joe age=20 job=null[ V.age@~9=20 ]\n" +
                "135toe age=30 job=null[ V.age@~9=30 ]\n" +
                "135xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "135a", "135z", null));
    }

    @Test
    @Ignore
    public void writeManyDeleteOneAllNullsScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "137joe", 20);
        testUtility.insertAge(t1, "137toe", 30);
        testUtility.insertAge(t1, "137boe", 40);
        testUtility.insertAge(t1, "137moe", null);
        testUtility.insertAge(t1, "137xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "137moe");
        commit(t2);

        Txn t3 = control.beginTransaction();
        String expected = "137boe age=40 job=null[ V.age@~9=40 ]\n" +
                "137joe age=20 job=null[ V.age@~9=20 ]\n" +
                "137toe age=30 job=null[ V.age@~9=30 ]\n" +
                "137xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "137a", "137z", null));
    }

    @Test
    @Ignore
    public void writeManyDeleteOneAllNullsSameTransactionScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "138joe", 20);
        testUtility.insertAge(t1, "138toe", 30);
        testUtility.insertAge(t1, "138boe", 40);
        testUtility.insertAge(t1, "138xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.insertAge(t2, "138moe", null);
        testUtility.deleteRow(t2, "138moe");
        commit(t2);

        Txn t3 = control.beginTransaction();
        String expected = "138boe age=40 job=null[ V.age@~9=40 ]\n" +
                "138joe age=20 job=null[ V.age@~9=20 ]\n" +
                "138toe age=30 job=null[ V.age@~9=30 ]\n" +
                "138xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "138a", "138z", null));
    }

    @Test
    @Ignore
    public void writeManyDeleteOneBeforeWriteSameTransactionAsWriteScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "136joe", 20);
        testUtility.insertAge(t1, "136toe", 30);
        testUtility.insertAge(t1, "136boe", 40);
        testUtility.insertAge(t1, "136moe", 50);
        testUtility.insertAge(t1, "136xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "136moe");
        testUtility.insertAge(t2, "136moe", 51);
        commit(t2);

        Txn t3 = control.beginTransaction();
        String expected = "136boe age=40 job=null[ V.age@~9=40 ]\n" +
                "136joe age=20 job=null[ V.age@~9=20 ]\n" +
                "136moe age=51 job=null[ V.age@~9=51 ]\n" +
                "136toe age=30 job=null[ V.age@~9=30 ]\n" +
                "136xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "136a", "136z", null));
    }

    @Test
    @Ignore
    public void writeManyDeleteOneBeforeWriteAllNullsSameTransactionAsWriteScanWithIncludeSIColumn() throws IOException {
				/*
				 * Steps here:
				 * 1. write a bunch of rows (writeMany)
				 * 2. delete a row (DeleteOne)
				 * 3. Write all nulls (WriteAllNulls)
				 * 4. Scan the data, including SI Column
				 */
        Txn t1 = control.beginTransaction();
        //insert some data
        testUtility.insertAge(t1, "139joe", 20);
        testUtility.insertAge(t1, "139toe", 30);
        testUtility.insertAge(t1, "139boe", 40);
        testUtility.insertAge(t1, "139moe", 50);
        testUtility.insertAge(t1, "139xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        //delete a row, then insert all nulls into it
        testUtility.deleteRow(t2, "139moe");
        testUtility.insertAge(t2, "139moe", null);
        commit(t2);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("139moe age=null job=null", testUtility.read(t2, "139moe"));
        String expected = "139boe age=40 job=null[ V.age@~9=40 ]\n" +
                "139joe age=20 job=null[ V.age@~9=20 ]\n" +
                "139moe age=null job=null[  ]\n" +
                "139toe age=30 job=null[ V.age@~9=30 ]\n" +
                "139xoe age=60 job=null[ V.age@~9=60 ]\n";

        //read the data and make sure it's valid
        final String actual = testUtility.scanAll(t3, "139a", "139z", null);
        Assert.assertEquals("Incorrect read data", expected, actual);
    }

    @Test
    @Ignore
    public void writeManyWithOneAllNullsDeleteOneScan() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "112joe", 20);
        testUtility.insertAge(t1, "112toe", 30);
        testUtility.insertAge(t1, "112boe", null);
        testUtility.insertAge(t1, "112moe", 50); //
        testUtility.insertAge(t1, "112xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "112moe");
        commit(t2);

        Txn t3 = control.beginTransaction();
        String expected = "112boe age=null job=null[  ]\n" +
                "112joe age=20 job=null[ V.age@~9=20 ]\n" +
                "112toe age=30 job=null[ V.age@~9=30 ]\n" +
                "112xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "112a", "112z", null));
    }

    @Test
    @Ignore
    public void writeManyWithOneAllNullsDeleteOneScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "111joe", 20);
        testUtility.insertAge(t1, "111toe", 30);
        testUtility.insertAge(t1, "111boe", null);
        testUtility.insertAge(t1, "111moe", 50);
        testUtility.insertAge(t1, "111xoe", 60);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.deleteRow(t2, "111moe");
        commit(t2);

        Txn t3 = control.beginTransaction();
        String expected = "111boe age=null job=null[  ]\n" +
                "111joe age=20 job=null[ V.age@~9=20 ]\n" +
                "111toe age=30 job=null[ V.age@~9=30 ]\n" +
                "111xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "111a", "111z", null));
    }

    @Test
    @Ignore
    public void writeDeleteSameTransaction() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe81", 19);
        commit(t0);

        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe81", 20);
        testUtility.deleteRow(t1, "joe81");
        Assert.assertEquals("joe81 absent", testUtility.read(t1, "joe81"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe81 absent", testUtility.read(t2, "joe81"));
        commit(t2);
    }

    @Test
    @Ignore
    public void deleteWriteSameTransaction() throws IOException {
        Txn t0 = control.beginTransaction();
        t0 = elevate(t0);
        testUtility.insertAge(t0, "joe82", 19);
        commit(t0);

        Txn t1 = control.beginTransaction();
        testUtility.deleteRow(t1, "joe82");
        testUtility.insertAge(t1, "joe82", 20);
        Assert.assertEquals("joe82 age=20 job=null", testUtility.read(t1, "joe82"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe82 age=20 job=null", testUtility.read(t2, "joe82"));
        commit(t2);
    }

    @Test
    @Ignore
    public void fourTransactions() throws Exception {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe7", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe7 age=20 job=null", testUtility.read(t2, "joe7"));
        testUtility.insertAge(t2, "joe7", 30);
        Assert.assertEquals("joe7 age=30 job=null", testUtility.read(t2, "joe7"));

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe7 age=20 job=null", testUtility.read(t3, "joe7"));

        commit(t2);

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe7 age=30 job=null", testUtility.read(t4, "joe7"));
    }

    @Test
    @Ignore
    public void writeReadOnly() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe18", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe18 age=20 job=null", testUtility.read(t2, "joe18"));
        try {
            testUtility.insertAge(t2, "joe18", 21);
            Assert.fail("expected exception performing a write on a read-only transaction");
        } catch (IOException e) {
        }
    }

    @Test
    @Ignore
    public void testReadCommittedVisibleWithHappensAfter() throws IOException {
				/*
				 * Tests that a read-committed transaction is able to see rows that were committed
				 *  before the transaction was started
				 */
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe19", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe19 age=20 job=null", testUtility.read(t2, "joe19"));
    }

    @Test
    @Ignore
    public void testReadCommittedVisibleWithSimultaneousAction() throws IOException {
				/*
				 * Tests that a read-committed transaction is able to see rows that were committed,
				 * even if the commit TS is after the RC begin timestamp
				 */
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe20", 20);

        Txn t2 = control.beginTransaction();

        Assert.assertEquals("joe20 absent", testUtility.read(t2, "joe20"));
        commit(t1);
        Assert.assertEquals("joe20 age=20 job=null", testUtility.read(t2, "joe20"));
    }

    @Test
    @Ignore
    public void testReadUncommittedRowsVisibleHappensAfter() throws IOException {
				/*
				 * Tests that read-uncommitted transactions are able to see rows that were committed
				 * before the RU transaction was started
				 */
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe22", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe22 age=20 job=null", testUtility.read(t2, "joe22"));
    }

    @Test
    @Ignore
    public void testsReadUncommittedRowsVisibleIfNotRolledBack() throws IOException {
				/*
				 * Tests that a read-uncommitted transaction is able to see rows, even if the
				 * writing transaction has not committed
				 */
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe21", 20);

        Txn t2 = control.beginTransaction();

        Assert.assertEquals("joe21 age=20 job=null", testUtility.read(t2, "joe21"));
        commit(t1);
        Assert.assertEquals("joe21 age=20 job=null", testUtility.read(t2, "joe21"));
    }

    @Test
    @Ignore
    public void testReadUncommittedCannotSeeRolledBackRows() throws IOException {
				/*
				 * Tests that a read-uncommitted transaction is not able to see
				 * rolled back rows
				 */
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe23", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.insertAge(t2, "joe23", 21);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe23 age=21 job=null", testUtility.read(t3, "joe23"));

        rollback(t2);
        Assert.assertEquals("joe23 age=20 job=null", testUtility.read(t3, "joe23"));

        Txn t4 = control.beginTransaction();
        testUtility.insertAge(t4, "joe23", 22);
        Assert.assertEquals("joe23 age=22 job=null", testUtility.read(t3, "joe23"));
    }

    @Test
    @Ignore
    public void childDependentTransactionWriteRollbackRead() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe24", 19);
        commit(t0);
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe24", 20);
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "moe24", 21);
        Assert.assertEquals("joe24 age=20 job=null", testUtility.read(t1, "joe24"));
        Assert.assertEquals("moe24 absent", testUtility.read(t1, "moe24"));
        rollback(t2);
        Assert.assertEquals("joe24 age=20 job=null", testUtility.read(t1, "joe24"));
        Assert.assertEquals("moe24 absent", testUtility.read(t1, "moe24"));
        commit(t1);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe24 age=20 job=null", testUtility.read(t3, "joe24"));
        Assert.assertEquals("moe24 absent", testUtility.read(t3, "moe24"));
    }

    @Test
    @Ignore
    public void testRollbackDoesNothingToCommittedChildTransactions() throws IOException {
				/*
				 * Tests that rolling back a child transaction does nothing if the child transaction
				 * is already committed.
				 */
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "joe51", 21);
        commit(t2);
        rollback(t2);
        Assert.assertEquals("joe51 age=21 job=null", testUtility.read(t1, "joe51"));
        commit(t1);
    }

    @Test
    @Ignore
    public void testDependentChildSeesParentsWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe40", 20);
        Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe40 age=20 job=null", testUtility.read(t2, "joe40"));
    }

    @Test
    @Ignore
    public void childDependentTransactionWriteRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe25", 20);
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "moe25", 21);
        Assert.assertEquals("joe25 age=20 job=null", testUtility.read(t1, "joe25"));
        Assert.assertEquals("moe25 absent", testUtility.read(t1, "moe25"));
        commit(t2);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe25 absent", testUtility.read(t3, "joe25"));
        Assert.assertEquals("moe25 absent", testUtility.read(t3, "moe25"));

        Assert.assertEquals("joe25 age=20 job=null", testUtility.read(t1, "joe25"));
        Assert.assertEquals("moe25 age=21 job=null", testUtility.read(t1, "moe25"));
        commit(t1);

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe25 age=20 job=null", testUtility.read(t4, "joe25"));
        Assert.assertEquals("moe25 age=21 job=null", testUtility.read(t4, "moe25"));
    }

    @Test
    @Ignore
    public void childDependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe37", 20);
        commit(t0);

        Txn t1 = control.beginTransaction();

        Txn otherTransaction = control.beginTransaction();
        testUtility.insertAge(otherTransaction, "joe37", 30);
        commit(otherTransaction);

        Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe37 age=20 job=null", testUtility.read(t2, "joe37"));
        commit(t2);
        commit(t1);
    }

    @Test
    @Ignore
    public void multipleChildDependentTransactionWriteRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe26", 20);
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "moe26", 21);
        testUtility.insertJob(t3, "boe26", "baker");
        Assert.assertEquals("joe26 age=20 job=null", testUtility.read(t1, "joe26"));
        Assert.assertEquals("moe26 absent", testUtility.read(t1, "moe26"));
        Assert.assertEquals("boe26 absent", testUtility.read(t1, "boe26"));
        commit(t2);

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe26 absent", testUtility.read(t4, "joe26"));
        Assert.assertEquals("moe26 absent", testUtility.read(t4, "moe26"));
        Assert.assertEquals("boe26 absent", testUtility.read(t4, "boe26"));

        Assert.assertEquals("joe26 age=20 job=null", testUtility.read(t1, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", testUtility.read(t1, "moe26"));
        Assert.assertEquals("boe26 absent", testUtility.read(t1, "boe26"));
        commit(t3);

        Txn t5 = control.beginTransaction();
        Assert.assertEquals("joe26 absent", testUtility.read(t5, "joe26"));
        Assert.assertEquals("moe26 absent", testUtility.read(t5, "moe26"));
        Assert.assertEquals("boe26 absent", testUtility.read(t5, "boe26"));

        Assert.assertEquals("joe26 age=20 job=null", testUtility.read(t1, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", testUtility.read(t1, "moe26"));
        Assert.assertEquals("boe26 age=null job=baker", testUtility.read(t1, "boe26"));
        commit(t1);

        Txn t6 = control.beginTransaction();
        Assert.assertEquals("joe26 age=20 job=null", testUtility.read(t6, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", testUtility.read(t6, "moe26"));
        Assert.assertEquals("boe26 age=null job=baker", testUtility.read(t6, "boe26"));
    }

    @Test
    @Ignore
    public void multipleChildDependentTransactionsRollbackThenWrite() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe45", 20);
        testUtility.insertAge(t1, "boe45", 19);
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "joe45", 21);
        Txn t3 = control.beginChildTransaction(t1);
        testUtility.insertJob(t3, "boe45", "baker");
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("joe45 age=21 job=null", testUtility.read(t2, "joe45"));
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t3, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));
        Assert.assertEquals("boe45 age=19 job=baker", testUtility.read(t3, "boe45"));
        rollback(t2);
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t3, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));
        Assert.assertEquals("boe45 age=19 job=baker", testUtility.read(t3, "boe45"));
        rollback(t3);
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));
        Txn t4 = control.beginChildTransaction(t1);
        testUtility.insertAge(t4, "joe45", 24);
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("joe45 age=24 job=null", testUtility.read(t4, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t4, "boe45"));
        commit(t4);
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));

        Txn t5 = control.beginTransaction();
        Assert.assertEquals("joe45 absent", testUtility.read(t5, "joe45"));
        Assert.assertEquals("boe45 absent", testUtility.read(t5, "boe45"));
        commit(t1);
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t5, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t5, "boe45"));
    }

    /*
     * The following test is commented out because it actually
     * IS now impossible to create a child of a rolled back parent--however,
     * that impossibility is not cluster-safe--we don't put a protection
     * into the transaction subsystem directly to prevent one from creating
     * a child on another machine.
     *
     * Instead, we take it as a given that proper use is to not create a
     * child of a rolled back transaction, and that the behavior in such cases
     * is undefined--thus, no tests to assert behavior in those cases.
     */
//    @Test
//    public void multipleChildCommitParentRollback() throws IOException {
//        Txn t1 = control.beginTransaction();
//        testUtility.insertAge(t1, "joe46", 20);
//        Txn t2 = control.beginChildTransaction(t1);
//        testUtility.insertJob(t2, "moe46", "baker");
//        commit(t2);
//				Assert.assertEquals("joe46 age=20 job=null", testUtility.read(t1, "joe46"));
//				Assert.assertEquals("moe46 age=null job=baker", testUtility.read(t1, "moe46"));
//				rollback(t1);
//        Txn t3 = control.beginChildTransaction(t1,t1.getIsolationLevel());
//        Assert.assertEquals("joe46 absent", testUtility.read(t3, "joe46"));
//				/*
//				 * When we create a child transaction after rolling back the parent, we
//				 * enter the realm of undefined semantics. In this sense, we choose
//				 * to be strict with respect to roll backs--in that way, rolling back
//				 * a transaction is as if it never happened, EVEN TO ITSELF.
//				 *
//				 */
//        Assert.assertEquals("moe46 absent", testUtility.read(t3, "moe46"));
//    }

    @Test
    @Ignore
    public void childDependentTransactionWriteRollbackParentRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe27", 20);
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "moe27", 21);
        commit(t2);
        rollback(t1);

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe27 absent", testUtility.read(t4, "joe27"));
        Assert.assertEquals("moe27 absent", testUtility.read(t4, "moe27"));
    }

    @Test
    @Ignore
    public void commitParentOfCommittedDependent() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe32", 20);
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "moe32", 21);
        commit(t2);

//        final Txn transactionStatusA = transactorSetup.transactionStore.getTransaction(commit(t2);
        Txn t2Check = txnStore.getTransaction(t2.getTxnId());
        Assert.assertNotEquals("committing a child does not set a local commit timestamp", -1l, t2Check.getCommitTimestamp());
        Assert.assertEquals("child has effective commit timestamp even though parent has not committed!", -1l, t2Check.getCommitTimestamp());
        long earlyCommit = t2Check.getCommitTimestamp();
        commit(t1);

        t2Check = txnStore.getTransaction(t2.getTxnId());
        Assert.assertEquals("committing parent of dependent transaction should not change the commit time of the child",
                earlyCommit, t2Check.getCommitTimestamp());
        Assert.assertTrue("incorrect effective commit timestamp!", t2Check.getCommitTimestamp() >= 0);
    }

    @Test
    @Ignore
    public void dependentWriteFollowedByReadCommittedWriter() throws IOException {
        Txn parent = control.beginTransaction();

        Txn child = control.beginChildTransaction(parent);
        testUtility.insertAge(child, "joe34", 22);
        commit(child);

        Txn other = control.beginTransaction();
        try {
            testUtility.insertAge(other, "joe34", 21);
            Assert.fail("No write conflict detected");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            commit(other);
        }
    }

    @Test
    @Ignore
    public void dependentWriteCommitParentFollowedByReadCommittedWriter() throws IOException {
        Txn parent = control.beginTransaction();

        Txn child = control.beginChildTransaction(parent);
        testUtility.insertAge(child, "joe94", 22);
        commit(child);
        commit(parent);

        Txn other = control.beginTransaction();
        testUtility.insertAge(other, "joe94", 21);
        commit(other);
    }

    @Test
    @Ignore
    public void dependentWriteOverlapWithReadCommittedWriter() throws IOException {
        Txn parent = control.beginTransaction();

        Txn other = control.beginTransaction();

        Txn child = control.beginChildTransaction(parent);
        testUtility.insertAge(child, "joe36", 22);
        commit(child);;

        try {
            testUtility.insertAge(other, "joe36", 21);
            Assert.fail();
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            commit(other);
        }
    }

    @Test
    @Ignore
    public void rollbackUpdate() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe43", 20);
        commit(t1);

        Txn t2 = control.beginTransaction();
        testUtility.insertAge(t2, "joe43", 21);
        rollback(t2);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe43 age=20 job=null", testUtility.read(t3, "joe43"));
    }

    @Test
    @Ignore
    public void rollbackInsert() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe44", 20);
        rollback(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe44 absent", testUtility.read(t2, "joe44"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenCommitCommitCommit() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t3, "joe53", 20);
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t3, "joe53"));
        commit(t3);
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t3, "joe53"));
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t2, "joe53"));
        testUtility.insertAge(t2, "boe53", 21);
        Assert.assertEquals("boe53 age=21 job=null", testUtility.read(t2, "boe53"));
        commit(t2);
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t1, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", testUtility.read(t1, "boe53"));
        commit(t1);
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t4, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", testUtility.read(t4, "boe53"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenCommitCommitCommitParentWriteFirst() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t1, "joe57", 18);
        testUtility.insertAge(t1, "boe57", 19);
        testUtility.insertAge(t2, "boe57", 21);
        testUtility.insertAge(t3, "joe57", 20);
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t3, "joe57"));
        commit(t3);
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t3, "joe57"));
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t2, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", testUtility.read(t2, "boe57"));
        commit(t2);
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t1, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", testUtility.read(t1, "boe57"));
        commit(t1);
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t4, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", testUtility.read(t4, "boe57"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenCommitCommitRollback() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t3, "joe54", 20);
        Assert.assertEquals("joe54 age=20 job=null", testUtility.read(t3, "joe54"));
        rollback(t3);
        Assert.assertEquals("joe54 absent", testUtility.read(t2, "joe54"));
        testUtility.insertAge(t2, "boe54", 21);
        Assert.assertEquals("boe54 age=21 job=null", testUtility.read(t2, "boe54"));
        commit(t2);
        Assert.assertEquals("joe54 absent", testUtility.read(t1, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", testUtility.read(t1, "boe54"));
        commit(t1);
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe54 absent", testUtility.read(t4, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", testUtility.read(t4, "boe54"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenWritesDoNotConflict() throws IOException {
        Txn t1 = control.beginTransaction();
		/*
		 * In order to be transactionally value, we cannot write while we have active
		 * child transactions (otherwise we violate the requirement that events
		 * occur sequentially). Thus, we have to write the data first, then
		 * we can begin the child transactions.
		 */
        testUtility.insertAge(t1, "joe95", 18);
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t3, "joe95", 20);
        Assert.assertEquals("joe95 age=18 job=null", testUtility.read(t1, "joe95"));
        Assert.assertEquals("joe95 age=18 job=null", testUtility.read(t2, "joe95"));
        Assert.assertEquals("joe95 age=20 job=null", testUtility.read(t3, "joe95"));
    }

    @Test
    @Ignore
    public void testCannotCreateWritableChildOfParentTxn() throws Exception {
        Txn t1 = control.beginTransaction();
        error.expect(IOException.class);
        Txn t2 = control.beginChildTransaction(t1);
    }

    @Test
    @Ignore
    public void testParentReadsChildWritesInReadCommittedMode() throws IOException {
		/*
		 * This tests two things:
		 *
		 * 1. that a parent's writes do not conflict with its children's writes, if the
		 * children's writes occurred before the parent.
		 *
		 */
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "joe101", 20);
        commit(t2);
        testUtility.insertAge(t1, "joe101", 21);
        Assert.assertEquals("joe101 age=21 job=null", testUtility.read(t1, "joe101"));
    }

    @Test
    @Ignore
    public void parentWritesDoNotConflictWithPriorChildDelete() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.deleteRow(t2, "joe105");
        commit(t2);
        testUtility.insertAge(t1, "joe105", 21);
        Assert.assertEquals("joe105 age=21 job=null", testUtility.read(t1, "joe105"));
    }

    @Test
    @Ignore
    public void parentWritesDoNotConflictWithPriorChildDelete2() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe141", 20);
        commit(t0);
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.deleteRow(t2, "joe141");
        commit(t2);
        testUtility.insertAge(t1, "joe141", 21);
        Assert.assertEquals("joe141 age=21 job=null", testUtility.read(t1, "joe141"));
    }

    @Test
    @Ignore
    public void parentDeleteDoesNotConflictWithPriorChildDelete() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.deleteRow(t2, "joe109");
        commit(t2);
        testUtility.deleteRow(t1, "joe109");
        Assert.assertEquals("joe109 absent", testUtility.read(t1, "joe109"));
        testUtility.insertAge(t1, "joe109", 21);
        Assert.assertEquals("joe109 age=21 job=null", testUtility.read(t1, "joe109"));
    }

    @Test
    @Ignore
    public void parentWritesDoNotConflictWithPriorActiveChildWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "joe102", 20);
        testUtility.insertAge(t1, "joe102", 21);
        Assert.assertEquals("joe102 age=21 job=null", testUtility.read(t1, "joe102"));
        commit(t2);
        Assert.assertEquals("joe102 age=21 job=null", testUtility.read(t1, "joe102"));
    }

    @Test
    @Ignore
    public void parentWritesDoNotConflictWithPriorActiveChildDelete() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.deleteRow(t2, "joe106");
        testUtility.insertAge(t1, "joe106", 21);
        Assert.assertEquals("joe106 age=21 job=null", testUtility.read(t1, "joe106"));
        commit(t2);
        Assert.assertEquals("joe106 age=21 job=null", testUtility.read(t1, "joe106"));
    }

    @Test
    @Ignore
    public void parentWritesDoNotConflictWithPriorIndependentChildWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "joe103", 20);
        commit(t2);
        testUtility.insertAge(t1, "joe103", 21);
        Assert.assertEquals("joe103 age=21 job=null", testUtility.read(t1, "joe103"));
    }

    @Test
    @Ignore
    public void parentWritesDoNotConflictWithPriorIndependentChildDelete() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.deleteRow(t2, "joe107");
        commit(t2);
        testUtility.insertAge(t1, "joe107", 21);
        Assert.assertEquals("joe107 age=21 job=null", testUtility.read(t1, "joe107"));
    }

    @Test
    @Ignore
    public void parentWritesDoNotConflictWithPriorActiveIndependentChildWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "joe104", 20);
        testUtility.insertAge(t1, "joe104", 21);
        Assert.assertEquals("joe104 age=21 job=null", testUtility.read(t1, "joe104"));
        commit(t2);
        Assert.assertEquals("joe104 age=21 job=null", testUtility.read(t1, "joe104"));
    }

    @Test
    @Ignore
    public void parentWritesDoNotConflictWithPriorActiveIndependentChildDelete() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.deleteRow(t2, "joe108");
        testUtility.insertAge(t1, "joe108", 21);
        Assert.assertEquals("joe108 age=21 job=null", testUtility.read(t1, "joe108"));
        commit(t2);
        Assert.assertEquals("joe108 age=21 job=null", testUtility.read(t1, "joe108"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenCommitCommitRollbackParentWriteFirst() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t1, "joe58", 18);
        testUtility.insertAge(t1, "boe58", 19);
        testUtility.insertAge(t2, "boe58", 21);
        testUtility.insertAge(t3, "joe58", 20);
        Assert.assertEquals("joe58 age=20 job=null", testUtility.read(t3, "joe58"));
        rollback(t3);
        Assert.assertEquals("joe58 age=18 job=null", testUtility.read(t2, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", testUtility.read(t2, "boe58"));
        commit(t2);
        Assert.assertEquals("joe58 age=18 job=null", testUtility.read(t1, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", testUtility.read(t1, "boe58"));
        commit(t1);
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe58 age=18 job=null", testUtility.read(t4, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", testUtility.read(t4, "boe58"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenCommitRollbackCommit() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t3, "joe55", 20);
        Assert.assertEquals("joe55 age=20 job=null", testUtility.read(t3, "joe55"));
        commit(t3);
        Assert.assertEquals("joe55 age=20 job=null", testUtility.read(t3, "joe55"));
        Assert.assertEquals("joe55 age=20 job=null", testUtility.read(t2, "joe55"));
        testUtility.insertAge(t2, "boe55", 21);
        Assert.assertEquals("boe55 age=21 job=null", testUtility.read(t2, "boe55"));
        rollback(t2);
        Assert.assertEquals("joe55 absent", testUtility.read(t1, "joe55"));
        Assert.assertEquals("boe55 absent", testUtility.read(t1, "boe55"));
        commit(t1);
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe55 absent", testUtility.read(t4, "joe55"));
        Assert.assertEquals("boe55 absent", testUtility.read(t4, "boe55"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenCommitRollbackCommitParentWriteFirst() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe59", 18);
        testUtility.insertAge(t1, "boe59", 19);
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t2, "boe59", 21);
        testUtility.insertAge(t3, "joe59", 20);
        Assert.assertEquals("joe59 age=20 job=null", testUtility.read(t3, "joe59"));
        commit(t3);
        Assert.assertEquals("joe59 age=20 job=null", testUtility.read(t2, "joe59"));
        Assert.assertEquals("boe59 age=21 job=null", testUtility.read(t2, "boe59"));
        rollback(t2);
        Assert.assertEquals("joe59 age=18 job=null", testUtility.read(t1, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", testUtility.read(t1, "boe59"));
        commit(t1);
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe59 age=18 job=null", testUtility.read(t4, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", testUtility.read(t4, "boe59"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenRollbackCommitCommitParentWriteFirst() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t1, "joe60", 18);
        testUtility.insertAge(t1, "boe60", 19);
        testUtility.insertAge(t2, "doe60", 21);
        testUtility.insertAge(t3, "moe60", 30);
        Assert.assertEquals("moe60 age=30 job=null", testUtility.read(t3, "moe60"));
        commit(t3);
        Assert.assertEquals("moe60 age=30 job=null", testUtility.read(t3, "moe60"));
        Assert.assertEquals("doe60 age=21 job=null", testUtility.read(t2, "doe60"));
        testUtility.insertAge(t2, "doe60", 22);
        Assert.assertEquals("doe60 age=22 job=null", testUtility.read(t2, "doe60"));
        commit(t2);
        Assert.assertEquals("joe60 age=18 job=null", testUtility.read(t1, "joe60"));
        Assert.assertEquals("boe60 age=19 job=null", testUtility.read(t1, "boe60"));
        Assert.assertEquals("moe60 age=30 job=null", testUtility.read(t1, "moe60"));
        Assert.assertEquals("doe60 age=22 job=null", testUtility.read(t1, "doe60"));
        rollback(t1);
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe60 absent", testUtility.read(t4, "joe60"));
        Assert.assertEquals("boe60 absent", testUtility.read(t4, "boe60"));
    }

    @Test
    @Ignore
    public void childrenOfChildrenRollbackCommitCommit() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t2);
        testUtility.insertAge(t3, "joe56", 20);
        Assert.assertEquals("joe56 age=20 job=null", testUtility.read(t3, "joe56"));
        commit(t3);
        Assert.assertEquals("joe56 age=20 job=null", testUtility.read(t2, "joe56"));
        testUtility.insertAge(t2, "boe56", 21);
        Assert.assertEquals("boe56 age=21 job=null", testUtility.read(t2, "boe56"));
        commit(t2);
        Assert.assertEquals("joe56 age=20 job=null", testUtility.read(t1, "joe56"));
        Assert.assertEquals("boe56 age=21 job=null", testUtility.read(t1, "boe56"));
        rollback(t1);
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe56 absent", testUtility.read(t4, "joe56"));
        Assert.assertEquals("boe56 absent", testUtility.read(t4, "boe56"));
    }

    private void sleep() throws InterruptedException {
        final Clock clock =  testEnv.getClock();
        clock.sleep(2000,TimeUnit.MILLISECONDS);
    }

    @Test
    @Ignore
    public void testWritesWithRolledBackTxnStillThrowWriteConflict() throws IOException, InterruptedException {
        final Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe66", 20);
        final Txn t2 = control.beginTransaction();
        try {
            testUtility.insertAge(t2, "joe66", 22);
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            rollback(t2);
        }
        try {
            testUtility.insertAge(t2, "joe66", 23);
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            rollback(t2);
        }
    }

    @Test
    @Ignore
    public void childIndependentTransactionWriteCommitRollbackRead() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "joe52", 21);
        commit(t2);
        rollback(t2);
        Assert.assertEquals("joe52 age=21 job=null", testUtility.read(t1, "joe52"));
        commit(t1);
    }

    @Test
    @Ignore
    public void childIndependentSeesParentWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe41", 20);
        Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe41 age=20 job=null", testUtility.read(t2, "joe41"));
    }

    @Test
    @Ignore
    public void childIndependentReadOnlySeesParentWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe96", 20);
        final Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe96 age=20 job=null", testUtility.read(t2, "joe96"));
    }

    @Test
    @Ignore
    public void childIndependentReadUncommittedDoesSeeParentWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe99", 20);
        Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe99 age=20 job=null", testUtility.read(t2, "joe99"));
    }

    @Test
    @Ignore
    public void childIndependentReadOnlyUncommittedDoesSeeParentWrites() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe100", 20);
//        final Txn t2 = control.beginChildTransaction(t1, false, false, false, true, true, null);
        final Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe100 age=20 job=null", testUtility.read(t2, "joe100"));
    }

    @Test
    @Ignore
    public void childIndependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe38", 20);
        commit(t0);

        Txn t1 = control.beginTransaction();

        Txn otherTransaction = control.beginTransaction();
        testUtility.insertAge(otherTransaction, "joe38", 30);
        commit(otherTransaction);

//        Txn t2 = control.beginChildTransaction(t1, false, true, false, null, true, null);
        Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe38 age=30 job=null", testUtility.read(t2, "joe38"));
        commit(t2);
        commit(t1);
    }

    @Test
    @Ignore
    public void childIndependentReadOnlyTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe97", 20);
        commit(t0);

        Txn t1 = control.beginTransaction();

        Txn otherTransaction = control.beginTransaction();
        testUtility.insertAge(otherTransaction, "joe97", 30);
        commit(otherTransaction);

//        Txn t2 = control.beginChildTransaction(t1, false, false, false, null, true, null);
        Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe97 age=30 job=null", testUtility.read(t2, "joe97"));
        commit(t2);
        commit(t1);
    }

    @Test
    @Ignore
    public void childIndependentReadOnlyTransactionWithReadCommittedOffWithOtherCommitBetweenParentAndChild() throws IOException {
        Txn t0 = control.beginTransaction();
        testUtility.insertAge(t0, "joe98", 20);
        commit(t0);

        Txn t1 = control.beginTransaction();

        Txn otherTransaction = control.beginTransaction();
        testUtility.insertAge(otherTransaction, "joe98", 30);
        commit(otherTransaction);

        Txn t2 = control.beginChildTransaction(t1);
        Assert.assertEquals("joe98 age=20 job=null", testUtility.read(t2, "joe98"));
        commit(t2);
        commit(t1);
    }

    @Test
    @Ignore
    public void childIndependentTransactionWriteRollbackRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe28", 20);
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "moe28", 21);
        Assert.assertEquals("joe28 age=20 job=null", testUtility.read(t1, "joe28"));
        Assert.assertEquals("moe28 absent", testUtility.read(t1, "moe28"));
        rollback(t2);
        Assert.assertEquals("joe28 age=20 job=null", testUtility.read(t1, "joe28"));
        Assert.assertEquals("moe28 absent", testUtility.read(t1, "moe28"));
        commit(t1);

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe28 age=20 job=null", testUtility.read(t3, "joe28"));
        Assert.assertEquals("moe28 absent", testUtility.read(t3, "moe28"));
    }

    @Test
    @Ignore
    public void multipleChildIndependentConflict() throws IOException {
        Txn t1 = control.beginTransaction();
        Txn t2 = control.beginChildTransaction(t1);
        Txn t3 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "moe31", 21);
        try {
            testUtility.insertJob(t3, "moe31", "baker");
            Assert.fail();
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            rollback(t3);
        }
    }

    @Test
    @Ignore
    public void commitParentOfCommittedIndependent() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe49", 20);
        Txn t2 = control.beginChildTransaction(t1);
        testUtility.insertAge(t2, "moe49", 21);
        commit(t2);
        Txn toCheckA = txnStore.getTransaction(t2.getTxnId());
        commit(t1);
        Txn toCheckB = txnStore.getTransaction(t2.getTxnId());
        Assert.assertEquals("committing parent of independent transaction should not change the commit time of the child",
                toCheckA.getCommitTimestamp(), toCheckB.getCommitTimestamp());
    }


    @Test
    @Ignore
    public void writeAllNullsRead() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe113 absent", testUtility.read(t1, "joe113"));
        t1 = elevate(t1);
        testUtility.insertAge(t1, "joe113", null);
        Assert.assertEquals("joe113 age=null job=null", testUtility.read(t1, "joe113"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe113 age=null job=null", testUtility.read(t2, "joe113"));
    }

    @Test
    @Ignore
    public void writeAllNullsReadOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe114 absent", testUtility.read(t1, "joe114"));
        t1 = elevate(t1);
        testUtility.insertAge(t1, "joe114", null);
        Assert.assertEquals("joe114 age=null job=null", testUtility.read(t1, "joe114"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe114 age=null job=null", testUtility.read(t1, "joe114"));
        Assert.assertEquals("joe114 absent", testUtility.read(t2, "joe114"));
        commit(t1);
        Assert.assertEquals("joe114 absent", testUtility.read(t2, "joe114"));
    }


    @Test
    @Ignore
    public void writeAllNullsWrite() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe115", null);
        Assert.assertEquals("joe115 age=null job=null", testUtility.read(t1, "joe115"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe115 age=null job=null", testUtility.read(t2, "joe115"));
        t2 = elevate(t2);
        testUtility.insertAge(t2, "joe115", 30);
        Assert.assertEquals("joe115 age=30 job=null", testUtility.read(t2, "joe115"));
        commit(t2);
    }

    @Test
    @Ignore
    public void writeAllNullsWriteOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe116 absent", testUtility.read(t1, "joe116"));
        t1 = elevate(t1);
        testUtility.insertAge(t1, "joe116", null);
        Assert.assertEquals("joe116 age=null job=null", testUtility.read(t1, "joe116"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe116 age=null job=null", testUtility.read(t1, "joe116"));
        Assert.assertEquals("joe116 absent", testUtility.read(t2, "joe116"));
        t2 = elevate(t2);
        try {
            testUtility.insertAge(t2, "joe116", 30);
            Assert.fail("Allowed insertion");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            rollback(t2);
        }
        Assert.assertEquals("joe116 age=null job=null", testUtility.read(t1, "joe116"));
        commit(t1);
        error.expect(IsInstanceOf.instanceOf(CannotCommitException.class));
        commit(t2);
        Assert.fail("Was able to comit a rolled back transaction");
    }

    @Test
    @Ignore
    public void readAllNullsAfterCommit() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe117", null);
        commit(t1);
        Assert.assertEquals("joe117 age=null job=null", testUtility.read(t1, "joe117"));
    }

    @Test
    @Ignore
    public void rollbackAllNullAfterCommit() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe118", null);
        commit(t1);
        rollback(t1);
        Assert.assertEquals("joe118 age=null job=null", testUtility.read(t1, "joe118"));
    }

    @Test
    @Ignore
    public void writeAllNullScan() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe119 absent", testUtility.read(t1, "joe119"));
        testUtility.insertAge(t1, "joe119", null);
        Assert.assertEquals("joe119 age=null job=null", testUtility.read(t1, "joe119"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe119 age=null job=null[  ]", testUtility.scan(t2, "joe119"));

        Assert.assertEquals("joe119 age=null job=null", testUtility.read(t2, "joe119"));
    }

    @Test
    @Ignore
    public void batchWriteRead() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe144 absent", testUtility.read(t1, "joe144"));
        testUtility.insertAgeBatch(new Object[]{t1, "joe144", 20}, new Object[]{t1, "bob144", 30});
        Assert.assertEquals("joe144 age=20 job=null", testUtility.read(t1, "joe144"));
        Assert.assertEquals("bob144 age=30 job=null", testUtility.read(t1, "bob144"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe144 age=20 job=null", testUtility.read(t2, "joe144"));
        Assert.assertEquals("bob144 age=30 job=null", testUtility.read(t2, "bob144"));
    }

    @Test
    @Ignore
    public void testDeleteThenInsertWithinChildTransactions() throws Exception {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joel", 20);
        commit(t1);


        Txn p = control.beginTransaction();
        Txn c1 = control.beginChildTransaction(p);
        testUtility.deleteRow(c1, "joel");
        commit(c1);
        Txn c2 = control.beginChildTransaction(p);
        Assert.assertEquals("joel absent", testUtility.read(c2, "joel"));
        testUtility.insertAge(c2, "joel", 22);
        Assert.assertEquals("joel age=22 job=null", testUtility.read(c2, "joel"));
    }

    @Test
    @Ignore
    public void writeDeleteBatchInsertRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "joe145", 10);
        testUtility.deleteRow(t1, "joe145");
        testUtility.insertAgeBatch(new Object[]{t1, "joe145", 20}, new Object[]{t1, "bob145", 30});
        Assert.assertEquals("joe145 age=20 job=null", testUtility.read(t1, "joe145"));
        Assert.assertEquals("bob145 age=30 job=null", testUtility.read(t1, "bob145"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe145 age=20 job=null", testUtility.read(t2, "joe145"));
        Assert.assertEquals("bob145 age=30 job=null", testUtility.read(t2, "bob145"));
    }

    @Test
    @Ignore
    public void writeManyDeleteBatchInsertRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "146joe", 10);
        testUtility.insertAge(t1, "146doe", 20);
        testUtility.insertAge(t1, "146boe", 30);
        testUtility.insertAge(t1, "146moe", 40);
        testUtility.insertAge(t1, "146zoe", 50);

        testUtility.deleteRow(t1, "146joe");
        testUtility.deleteRow(t1, "146doe");
        testUtility.deleteRow(t1, "146boe");
        testUtility.deleteRow(t1, "146moe");
        testUtility.deleteRow(t1, "146zoe");

        testUtility.insertAgeBatch(new Object[]{t1, "146zoe", 51}, new Object[]{t1, "146moe", 41},
                new Object[]{t1, "146boe", 31}, new Object[]{t1, "146doe", 21}, new Object[]{t1, "146joe", 11});
        Assert.assertEquals("146joe age=11 job=null", testUtility.read(t1, "146joe"));
        Assert.assertEquals("146doe age=21 job=null", testUtility.read(t1, "146doe"));
        Assert.assertEquals("146boe age=31 job=null", testUtility.read(t1, "146boe"));
        Assert.assertEquals("146moe age=41 job=null", testUtility.read(t1, "146moe"));
        Assert.assertEquals("146zoe age=51 job=null", testUtility.read(t1, "146zoe"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("146joe age=11 job=null", testUtility.read(t2, "146joe"));
        Assert.assertEquals("146doe age=21 job=null", testUtility.read(t2, "146doe"));
        Assert.assertEquals("146boe age=31 job=null", testUtility.read(t2, "146boe"));
        Assert.assertEquals("146moe age=41 job=null", testUtility.read(t2, "146moe"));
        Assert.assertEquals("146zoe age=51 job=null", testUtility.read(t2, "146zoe"));
    }

    @Test
    @Ignore
    public void writeManyDeleteBatchInsertSomeRead() throws IOException {
        Txn t1 = control.beginTransaction();
        testUtility.insertAge(t1, "147joe", 10);
        testUtility.insertAge(t1, "147doe", 20);
        testUtility.insertAge(t1, "147boe", 30);
        testUtility.insertAge(t1, "147moe", 40);
        testUtility.insertAge(t1, "147zoe", 50);

        testUtility.deleteRow(t1, "147joe");
        testUtility.deleteRow(t1, "147doe");
        testUtility.deleteRow(t1, "147boe");
        testUtility.deleteRow(t1, "147moe");
        testUtility.deleteRow(t1, "147zoe");

        testUtility.insertAgeBatch(new Object[]{t1, "147zoe", 51}, new Object[]{t1, "147boe", 31}, new Object[]{t1, "147joe", 11});
        Assert.assertEquals("147joe age=11 job=null", testUtility.read(t1, "147joe"));
        Assert.assertEquals("147boe age=31 job=null", testUtility.read(t1, "147boe"));
        Assert.assertEquals("147zoe age=51 job=null", testUtility.read(t1, "147zoe"));
        commit(t1);

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("147joe age=11 job=null", testUtility.read(t2, "147joe"));
        Assert.assertEquals("147boe age=31 job=null", testUtility.read(t2, "147boe"));
        Assert.assertEquals("147zoe age=51 job=null", testUtility.read(t2, "147zoe"));
    }

    // Commit & begin together tests
    @Test
    @Ignore
    public void testCommitAndBeginSeparate() throws IOException {
        final Txn t1 = control.beginTransaction();
        final Txn t2 = control.beginTransaction();
        commit(t1);
        final Txn t3 = control.beginChildTransaction(t2);
        Assert.assertEquals(t1.getTxnId() + 1, t2.getTxnId());
        // next ID burned for commit
        Assert.assertEquals(t1.getTxnId() + 2, t3.getTxnId());
    }

    @Test
    @Ignore
    public void testCommitAndBeginTogether() throws IOException {
        final Txn t1 = control.beginTransaction();
        final Txn t2 = control.beginTransaction();
//        final Txn t3 = control.beginChildTransaction(t2, true, true, false, null, null, commit(t1);
        final Txn t3 = control.beginChildTransaction(t2);
        Assert.assertEquals(t1.getTxnId() + 1, t2.getTxnId());
        // no ID burned for commit
        Assert.assertEquals(t1.getTxnId() + 2, t3.getTxnId());
    }

    @Test
    @Ignore
    public void testCommitNonRootAndBeginTogether() throws IOException {
        final Txn t1 = control.beginTransaction();
        final Txn t2 = control.beginChildTransaction(t1);
        final Txn t3 = control.beginTransaction();
//            control.beginChildTransaction(t3, true, true, false, null, null, t2);
        error.expect(IOException.class);
        control.chainTransaction(t3, t2);
        Assert.fail();
    }

    private Txn commit(Txn txn) throws IOException {
        return control.commit(txn);
    }

    private Txn rollback(Txn txn) throws IOException {
        return control.rollback(txn);
    }

    private Txn elevate(Txn txn) throws IOException {
        return control.elevateTransaction(txn);
    }

}
