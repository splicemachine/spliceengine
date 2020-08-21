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

package com.splicemachine.si;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.ReadOnlyModificationException;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.api.txn.lifecycle.CannotCommitException;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.impl.ForwardingTxnView;
import com.splicemachine.si.impl.txn.DDLTxnView;
import com.splicemachine.si.impl.txn.InheritingTxnView;
import com.splicemachine.si.impl.txn.LazyTxnView;
import com.splicemachine.si.testenv.*;
import com.splicemachine.storage.DataPut;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.utils.ByteSlice;
import org.hamcrest.core.IsInstanceOf;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import splice.com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("unchecked")
@Category(ArchitectureSpecific.class)
public class SITransactorTest {
    @Rule public ExpectedException error = ExpectedException.none();
    private static final byte[] DESTINATION_TABLE = Bytes.toBytes("1184");
    private boolean useSimple = true;
    private static SITestEnv testEnv;
    private static TestTransactionSetup transactorSetup;
    private TxnLifecycleManager control;
    private TransactorTestUtility testUtility;
    private final List<Txn> createdParentTxns = Lists.newArrayList();
    private TxnStore txnStore;

    @SuppressWarnings("unchecked")
    private void baseSetUp() {
        control = new ForwardingLifecycleManager(transactorSetup.txnLifecycleManager) {
            @Override
            protected void afterStart(Txn txn) {
                createdParentTxns.add(txn);
            }
        };
        testUtility = new TransactorTestUtility(useSimple,testEnv, transactorSetup);
        txnStore = transactorSetup.txnStore;
    }

    @BeforeClass
    public static void classSetUp() throws IOException {
        if(testEnv == null) {
            testEnv = SITestEnvironment.loadTestEnvironment();
            transactorSetup = new TestTransactionSetup(testEnv,true);
        }
        testEnv.initialize(); // reinitialize from scratch
    }

    @Before
    public void setUp() throws IOException {

        baseSetUp();
    }

    @After
    public void tearDown() throws Exception {
        for (Txn id : createdParentTxns) {
            try {
                TxnView txn = txnStore.getTransaction(id.getTxnId());
                if ((txn != null && txn.getEffectiveState().isFinal()) || id.getState().isFinal())
                    continue;
             } catch (TransactionMissing missing) {
                continue;
            }
            id.rollback();
        }
    }

    @Test
    public void writeRead() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = t1.elevateToWritable(Bytes.toBytes("t"));
        Assert.assertEquals("joe9 absent", testUtility.read(t1, "joe9"));
        testUtility.insertAge(t1, "joe9", 20);
        Assert.assertEquals("joe9 age=20 job=null", testUtility.read(t1, "joe9"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        try {
            Assert.assertEquals("joe9 age=20 job=null", testUtility.read(t2, "joe9"));
        } finally {
            t2.commit();
        }
    }

    @Test
    public void writeReadOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = t1.elevateToWritable(Bytes.toBytes("t"));
        Assert.assertEquals("joe8 absent", testUtility.read(t1, "joe8"));
        testUtility.insertAge(t1, "joe8", 20);
        Assert.assertEquals("joe8 age=20 job=null", testUtility.read(t1, "joe8"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe8 age=20 job=null", testUtility.read(t1, "joe8"));
        Assert.assertEquals("joe8 absent", testUtility.read(t2, "joe8"));
        t1.commit();
        Assert.assertEquals("joe8 absent", testUtility.read(t2, "joe8"));
    }

    @Test
    public void testGetActiveTransactionsFiltersOutChildrenCommit() throws Exception {
        Txn parent = control.beginTransaction(DESTINATION_TABLE);
        Txn child = control.beginChildTransaction(noSubTxns(parent), parent.getIsolationLevel(), DESTINATION_TABLE);
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

        Txn next = control.beginTransaction(DESTINATION_TABLE);
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

    @Test
    public void testChildTransactionsInterleave() throws Exception {
                /*
				 * Similar to what happens when an insertion occurs,
				 */
        Txn parent = control.beginTransaction(DESTINATION_TABLE);

        Txn interleave = control.beginTransaction();

        Txn child = control.beginChildTransaction(parent, DESTINATION_TABLE); //make it writable

        testUtility.insertAge(child, "joe", 20);

        Assert.assertEquals("joe absent", testUtility.read(interleave, "joe"));
    }

    @Test
    public void testTwoChildTransactionTreesIntervleavedWritesAndReads() throws Exception {
        Txn p1 = control.beginTransaction(DESTINATION_TABLE);
        Txn c1 = control.beginChildTransaction(p1, DESTINATION_TABLE);
        Txn g1 = control.beginChildTransaction(c1, DESTINATION_TABLE);

        testUtility.insertAge(g1, "tickle", 20);
        g1.commit();
        c1.commit();

        Txn p2 = control.beginTransaction(DESTINATION_TABLE);
        p1.commit();
        Txn c2 = control.beginChildTransaction(p2, DESTINATION_TABLE);
        Txn g2 = control.beginChildTransaction(c2, DESTINATION_TABLE);

        Assert.assertEquals("tickle absent", testUtility.read(g2, "tickle"));
    }

    @Test
    public void testTwoChildTransactionTreesIntervleavedWritesAndWrites() throws Throwable {
        Txn p1 = control.beginTransaction(DESTINATION_TABLE);
        Txn c1 = control.beginChildTransaction(p1, DESTINATION_TABLE);
        Txn g1 = control.beginChildTransaction(c1, DESTINATION_TABLE);

        testUtility.insertAge(g1, "tickle2", 20);
        g1.commit();
        c1.commit();

        Txn p2 = control.beginTransaction(DESTINATION_TABLE);
        p1.commit();
        Txn c2 = control.beginChildTransaction(p2, DESTINATION_TABLE);
        Txn g2 = control.beginChildTransaction(c2, DESTINATION_TABLE);

        try {
            testUtility.insertAge(g2, "tickle2", 22);
        } catch (IOException re) {
            testUtility.assertWriteConflict(re);
        }
    }

    @Test
    public void testCanRecordWriteTable() throws Exception {
        Txn parent = control.beginTransaction(DESTINATION_TABLE);
        TxnView transaction = txnStore.getTransaction(parent.getTxnId(), true);
        Iterator<ByteSlice> destinationTables = transaction.getDestinationTables();
        Assert.assertArrayEquals("Incorrect write table!", DESTINATION_TABLE, destinationTables.next().getByteCopy());
    }

    @Test
    public void writeWrite() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20 job=null", testUtility.read(t1, "joe"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe age=20 job=null", testUtility.read(t2, "joe"));
        t2 = t2.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30 job=null", testUtility.read(t2, "joe"));
        t2.commit();
    }

    @Test//(expected = CannotCommitException.class)
    public void writeWriteOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe012 absent", testUtility.read(t1, "joe012"));
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe012", 20);
        Assert.assertEquals("joe012 age=20 job=null", testUtility.read(t1, "joe012"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe012 age=20 job=null", testUtility.read(t1, "joe012"));
        Assert.assertEquals("joe012 absent", testUtility.read(t2, "joe012"));
        t2 = t2.elevateToWritable(DESTINATION_TABLE);
        try {
            testUtility.insertAge(t2, "joe012", 30);
            fail("was able to insert age");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            t2.rollback();
        }
        Assert.assertEquals("joe012 age=20 job=null", testUtility.read(t1, "joe012"));
        t1.commit();
        error.expect(IsInstanceOf.instanceOf(CannotCommitException.class));
        t2.commit(); //should not work, probably need to change assertion
        fail("Was able to commit a rolled back transaction");
    }

    @Test
    public void writeWriteOverlapRecovery() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe142 absent", testUtility.read(t1, "joe142"));
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe142", 20);
        Assert.assertEquals("joe142 age=20 job=null", testUtility.read(t1, "joe142"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe142 age=20 job=null", testUtility.read(t1, "joe142"));
        Assert.assertEquals("joe142 absent", testUtility.read(t2, "joe142"));
        t2 = t2.elevateToWritable(DESTINATION_TABLE);
        try {
            testUtility.insertAge(t2, "joe142", 30);
            fail();
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        }
        // can still use a transaction after a write conflict
        testUtility.insertAge(t2, "bob142", 30);
        Assert.assertEquals("bob142 age=30 job=null", testUtility.read(t2, "bob142"));
        Assert.assertEquals("joe142 age=20 job=null", testUtility.read(t1, "joe142"));
        t1.commit();
        t2.commit();
    }

    @Test
    public void readAfterCommit() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe3", 20);
        t1.commit();
        Assert.assertEquals("joe3 age=20 job=null", testUtility.read(t1, "joe3"));
    }

    @Test
    public void testCannotInsertUsingReadOnlyTransaction() throws Throwable {
        Txn t1 = control.beginTransaction();
        try{
            testUtility.insertAge(t1,"scott2",20);
            fail("Was able to insert age!");
        }catch(IOException ioe){
            ioe =testEnv.getExceptionFactory().processRemoteException(ioe);
            Assert.assertTrue("Incorrect exception",ReadOnlyModificationException.class.isAssignableFrom(ioe.getClass()));
        }
    }

    @Test
    public void testRollingBackAfterCommittingDoesNothing() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe50", 20);
        t1.commit();
        t1.rollback();
        Assert.assertEquals("joe50 age=20 job=null", testUtility.read(t1, "joe50"));
    }

    @Test
    public void writeScan() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Assert.assertEquals("joe4 absent", testUtility.read(t1, "joe4"));
        testUtility.insertAge(t1, "joe4", 20);
        Assert.assertEquals("joe4 age=20 job=null", testUtility.read(t1, "joe4"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe4 age=20 job=null[ V.age@~9=20 ]", testUtility.scan(t2, "joe4"));
        Assert.assertEquals("joe4 age=20 job=null", testUtility.read(t2, "joe4"));
    }

    @Test
    public void writeScanWithDeleteActive() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe128", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);

        Txn t3 = control.beginTransaction();

        testUtility.deleteRow(t2, "joe128");

        Assert.assertEquals("joe128 age=20 job=null[ V.age@~9=20 ]", testUtility.scan(t3, "joe128"));
        t2.commit();
    }

    @Test
    public void writeDeleteScanNoColumns() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe85", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe85");
        t2.commit();


        Txn t3 = control.beginTransaction();
        // reading si only (i.e. no data columns) returns the rows but none of the "user" data
        Assert.assertEquals("", testUtility.scanNoColumns(t3, "joe85", true));
    }

    @Test
    public void writeScanMultipleRows() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "17joe", 20);
        testUtility.insertAge(t1, "17bob", 30);
        testUtility.insertAge(t1, "17boe", 40);
        testUtility.insertAge(t1, "17tom", 50);
        t1.commit();

        Txn t2 = control.beginTransaction();
        String expected = "17bob age=30 job=null[ V.age@~9=30 ]\n" +
                "17boe age=40 job=null[ V.age@~9=40 ]\n" +
                "17joe age=20 job=null[ V.age@~9=20 ]\n" +
                "17tom age=50 job=null[ V.age@~9=50 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t2, "17a", "17z", null));
    }

    @Test
    public void writeScanWithFilter() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "91joe", 20);
        testUtility.insertAge(t1, "91bob", 30);
        testUtility.insertAge(t1, "91boe", 40);
        testUtility.insertAge(t1, "91tom", 50);
        t1.commit();

        Txn t2 = control.beginTransaction();
        String expected = "91boe age=40 job=null[ V.age@~9=40 ]\n";
        if (!useSimple) {
            Assert.assertEquals(expected, testUtility.scanAll(t2, "91a", "91z", 40));
        }
    }

    @Test
    public void writeScanWithFilterAndPendingWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "92joe", 20);
        testUtility.insertAge(t1, "92bob", 30);
        testUtility.insertAge(t1, "92boe", 40);
        testUtility.insertAge(t1, "92tom", 50);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t2, "92boe", 41);

        Txn t3 = control.beginTransaction();
        String expected = "92boe age=40 job=null[ V.age@~9=40 ]\n";
        if (!useSimple) {
            Assert.assertEquals(expected, testUtility.scanAll(t3, "92a", "92z", 40));
        }
    }

    @Test
    public void writeWriteRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe5", 20);
        Assert.assertEquals("joe5 age=20 job=null", testUtility.read(t1, "joe5"));
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        Assert.assertEquals("joe5 age=20 job=null", testUtility.read(t2, "joe5"));
        testUtility.insertJob(t2, "joe5", "baker");
        Assert.assertEquals("joe5 age=20 job=baker", testUtility.read(t2, "joe5"));
        t2.commit();

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=baker", testUtility.read(t3, "joe5"));
    }

    @Test
    public void multipleWritesSameTransaction() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe16 absent", testUtility.read(t1, "joe16"));
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe16", 20);
        Assert.assertEquals("joe16 age=20 job=null", testUtility.read(t1, "joe16"));

        testUtility.insertAge(t1, "joe16", 21);
        Assert.assertEquals("joe16 age=21 job=null", testUtility.read(t1, "joe16"));

        testUtility.insertAge(t1, "joe16", 22);
        Assert.assertEquals("joe16 age=22 job=null", testUtility.read(t1, "joe16"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe16 age=22 job=null", testUtility.read(t2, "joe16"));
        t2.commit();
    }

    @Test
    public void manyWritesManyRollbacksRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe6", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t2, "joe6", "baker");
        t2.commit();

        Txn t3 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t3, "joe6", "butcher");
        t3.commit();

        Txn t4 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t4, "joe6", "blacksmith");
        t4.commit();

        Txn t5 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t5, "joe6", "carter");
        t5.commit();

        Txn t6 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t6, "joe6", "farrier");
        t6.commit();

        Txn t7 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t7, "joe6", 27);
        t7.rollback();

        Txn t8 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t8, "joe6", 28);
        t8.rollback();

        Txn t9 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t9, "joe6", 29);
        t9.rollback();

        Txn t10 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t10, "joe6", 30);
        t10.rollback();

        Txn t11 = control.beginTransaction();
        Assert.assertEquals("joe6 age=20 job=farrier", testUtility.read(t11, "joe6"));
        t11.commit();
    }

    @Test
    public void writeDelete() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe10", 20);
        Assert.assertEquals("joe10 age=20 job=null", testUtility.read(t1, "joe10"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe10 age=20 job=null", testUtility.read(t2, "joe10"));
        t2 = t2.elevateToWritable(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe10");
        Assert.assertEquals("joe10 absent", testUtility.read(t2, "joe10"));
        t2.commit();
    }

    @Test
    public void writeDeleteRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe11", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe11");
        t2.commit();

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe11 absent", testUtility.read(t3, "joe11"));
        t3.commit();
    }

    @Test
    public void writeDeleteRollbackRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe90", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe90");
        t2.rollback();

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe90 age=20 job=null", testUtility.read(t3, "joe90"));
        t3.commit();
    }

    @Test
    public void writeChildDeleteParentRollbackDelete() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe93", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(t2, t2.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.deleteRow(t3, "joe93");
        t2.rollback();

        Txn t4 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t4, "joe93");
        Assert.assertEquals("joe93 absent", testUtility.read(t4, "joe93"));
        t4.commit();
    }

    @Test
    public void writeDeleteOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe2", 20);

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        try {
            testUtility.deleteRow(t2, "joe2");
            fail("No Write conflict was detected!");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            t2.rollback();
        }
        Assert.assertEquals("joe2 age=20 job=null", testUtility.read(t1, "joe2"));
        t1.commit();
        error.expect(IsInstanceOf.instanceOf(CannotCommitException.class));
        error.expectMessage(String.format("[%1$d]Transaction %1$d cannot be committed--it is in the %2$s state",t2.getTxnId(),Txn.State.ROLLEDBACK));
        t2.commit();
        fail("Did not throw CannotCommit exception!");
    }

    @Test
    public void writeWriteDeleteOverlap() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe013", 20);
        t0.commit();

        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t1, "joe013");

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        Assert.assertEquals("joe013 age=20 job=null",testUtility.read(t2,"joe013"));
        try {
            testUtility.insertAge(t2, "joe013", 21);
            fail("No WriteConflict thrown!");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            t2.rollback();
        }
        Assert.assertEquals("joe013 absent", testUtility.read(t1, "joe013"));
        t1.commit();
        try {
            t2.commit();
            fail();
        } catch (IOException dnrio) {
            Assert.assertTrue("Was not a CannotCommitException!",dnrio instanceof CannotCommitException);
            CannotCommitException cce = (CannotCommitException)dnrio;
            Assert.assertEquals("Incorrect transaction id!",t2.getTxnId(),cce.getTxnId());
            Assert.assertEquals("Incorrect cannot-commit state",Txn.State.ROLLEDBACK,cce.getActualState());
        }

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe013 absent", testUtility.read(t3, "joe013"));
        t3.commit();
    }

    @Test
    public void writeWriteDeleteWriteRead() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe14", 20);
        t0.commit();

        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t1, "joe14", "baker");
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe14");
        t2.commit();

        Txn t3 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t3, "joe14", "smith");
        Assert.assertEquals("joe14 age=null job=smith", testUtility.read(t3, "joe14"));
        t3.commit();

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe14 age=null job=smith", testUtility.read(t4, "joe14"));
        t4.commit();
    }

    @Test
    public void writeWriteDeleteWriteDeleteWriteRead() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe15", 20);
        t0.commit();

        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t1, "joe15", "baker");
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe15");
        t2.commit();

        Txn t3 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(t3, "joe15", "smith");
        Assert.assertEquals("joe15 age=null job=smith", testUtility.read(t3, "joe15"));
        t3.commit();

        Txn t4 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t4, "joe15");
        t4.commit();

        Txn t5 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t5, "joe15", 21);
        t5.commit();

        Txn t6 = control.beginTransaction();
        Assert.assertEquals("joe15 age=21 job=null", testUtility.read(t6, "joe15"));
        t6.commit();
    }

    @Test
    public void writeManyDeleteOneGets() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe47", 20);
        testUtility.insertAge(t1, "toe47", 30);
        testUtility.insertAge(t1, "boe47", 40);
        testUtility.insertAge(t1, "moe47", 50);
        testUtility.insertAge(t1, "zoe47", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "moe47");
        t2.commit();

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe47 age=20 job=null", testUtility.read(t3, "joe47"));
        Assert.assertEquals("toe47 age=30 job=null", testUtility.read(t3, "toe47"));
        Assert.assertEquals("boe47 age=40 job=null", testUtility.read(t3, "boe47"));
        Assert.assertEquals("moe47 absent", testUtility.read(t3, "moe47"));
        Assert.assertEquals("zoe47 age=60 job=null", testUtility.read(t3, "zoe47"));
    }

    @Test
    public void writeManyDeleteOneScan() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "48joe", 20);
        testUtility.insertAge(t1, "48toe", 30);
        testUtility.insertAge(t1, "48boe", 40);
        testUtility.insertAge(t1, "48moe", 50);
        testUtility.insertAge(t1, "48xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "48moe");
        t2.commit();

        Txn t3 = control.beginTransaction();
        String expected = "48boe age=40 job=null[ V.age@~9=40 ]\n" +
                "48joe age=20 job=null[ V.age@~9=20 ]\n" +
                "48toe age=30 job=null[ V.age@~9=30 ]\n" +
                "48xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "48a", "48z", null));
    }

    @Test
    public void writeManyDeleteOneScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "110joe", 20);
        testUtility.insertAge(t1, "110toe", 30);
        testUtility.insertAge(t1, "110boe", 40);
        testUtility.insertAge(t1, "110moe", 50);
        testUtility.insertAge(t1, "110xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "110moe");
        t2.commit();

        Txn t3 = control.beginTransaction();
        String expected = "110boe age=40 job=null[ V.age@~9=40 ]\n" +
                "110joe age=20 job=null[ V.age@~9=20 ]\n" +
                "110toe age=30 job=null[ V.age@~9=30 ]\n" +
                "110xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "110a", "110z", null));
    }


    @Test
    public void writeManyDeleteOneScanWithIncludeSIColumnSameTransaction() throws IOException {
        Txn t1 = control.beginTransaction();
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "143joe", 20);
        testUtility.insertAge(t1, "143toe", 30);
        testUtility.insertAge(t1, "143boe", 40);
        testUtility.insertAge(t1, "143moe", 50);
        testUtility.insertAge(t1, "143xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "143moe");
        String expected = "143boe age=40 job=null[ V.age@~9=40 ]\n" +
                "143joe age=20 job=null[ V.age@~9=20 ]\n" +
                "143toe age=30 job=null[ V.age@~9=30 ]\n" +
                "143xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t2, "143a", "143z", null));
    }

    @Test
    public void writeManyDeleteOneSameTransactionScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "135joe", 20);
        testUtility.insertAge(t1, "135toe", 30);
        testUtility.insertAge(t1, "135boe", 40);
        testUtility.insertAge(t1, "135xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "135moe", 50);
        testUtility.deleteRow(t2, "135moe");
        t2.commit();

        Txn t3 = control.beginTransaction();
        String expected = "135boe age=40 job=null[ V.age@~9=40 ]\n" +
                "135joe age=20 job=null[ V.age@~9=20 ]\n" +
                "135toe age=30 job=null[ V.age@~9=30 ]\n" +
                "135xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "135a", "135z", null));
    }

    @Test
    public void writeManyDeleteOneAllNullsScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "137joe", 20);
        testUtility.insertAge(t1, "137toe", 30);
        testUtility.insertAge(t1, "137boe", 40);
        testUtility.insertAge(t1, "137moe", null);
        testUtility.insertAge(t1, "137xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "137moe");
        t2.commit();

        Txn t3 = control.beginTransaction();
        String expected = "137boe age=40 job=null[ V.age@~9=40 ]\n" +
                "137joe age=20 job=null[ V.age@~9=20 ]\n" +
                "137toe age=30 job=null[ V.age@~9=30 ]\n" +
                "137xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "137a", "137z", null));
    }

    @Test
    public void writeManyDeleteOneAllNullsSameTransactionScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "138joe", 20);
        testUtility.insertAge(t1, "138toe", 30);
        testUtility.insertAge(t1, "138boe", 40);
        testUtility.insertAge(t1, "138xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t2, "138moe", null);
        testUtility.deleteRow(t2, "138moe");
        t2.commit();

        Txn t3 = control.beginTransaction();
        String expected = "138boe age=40 job=null[ V.age@~9=40 ]\n" +
                "138joe age=20 job=null[ V.age@~9=20 ]\n" +
                "138toe age=30 job=null[ V.age@~9=30 ]\n" +
                "138xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "138a", "138z", null));
    }

    @Test
    public void writeManyDeleteOneBeforeWriteSameTransactionAsWriteScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "136joe", 20);
        testUtility.insertAge(t1, "136toe", 30);
        testUtility.insertAge(t1, "136boe", 40);
        testUtility.insertAge(t1, "136moe", 50);
        testUtility.insertAge(t1, "136xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "136moe");
        testUtility.insertAge(t2, "136moe", 51);
        t2.commit();

        Txn t3 = control.beginTransaction();
        String expected = "136boe age=40 job=null[ V.age@~9=40 ]\n" +
                "136joe age=20 job=null[ V.age@~9=20 ]\n" +
                "136moe age=51 job=null[ V.age@~9=51 ]\n" +
                "136toe age=30 job=null[ V.age@~9=30 ]\n" +
                "136xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "136a", "136z", null));
    }

    @Test
    public void writeManyDeleteOneBeforeWriteAllNullsSameTransactionAsWriteScanWithIncludeSIColumn() throws IOException {
				/*
				 * Steps here:
				 * 1. write a bunch of rows (writeMany)
				 * 2. delete a row (DeleteOne)
				 * 3. Write all nulls (WriteAllNulls)
				 * 4. Scan the data, including SI Column
				 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        //insert some data
        testUtility.insertAge(t1, "139joe", 20);
        testUtility.insertAge(t1, "139toe", 30);
        testUtility.insertAge(t1, "139boe", 40);
        testUtility.insertAge(t1, "139moe", 50);
        testUtility.insertAge(t1, "139xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        //delete a row, then insert all nulls into it
        testUtility.deleteRow(t2, "139moe");
        testUtility.insertAge(t2, "139moe", null);
        t2.commit();

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
    public void writeManyWithOneAllNullsDeleteOneScan() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "112joe", 20);
        testUtility.insertAge(t1, "112toe", 30);
        testUtility.insertAge(t1, "112boe", null);
        testUtility.insertAge(t1, "112moe", 50); //
        testUtility.insertAge(t1, "112xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "112moe");
        t2.commit();

        Txn t3 = control.beginTransaction();
        String expected = "112boe age=null job=null[  ]\n" +
                "112joe age=20 job=null[ V.age@~9=20 ]\n" +
                "112toe age=30 job=null[ V.age@~9=30 ]\n" +
                "112xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "112a", "112z", null));
    }

    @Test
    public void writeManyWithOneAllNullsDeleteOneScanWithIncludeSIColumn() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "111joe", 20);
        testUtility.insertAge(t1, "111toe", 30);
        testUtility.insertAge(t1, "111boe", null);
        testUtility.insertAge(t1, "111moe", 50);
        testUtility.insertAge(t1, "111xoe", 60);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t2, "111moe");
        t2.commit();

        Txn t3 = control.beginTransaction();
        String expected = "111boe age=null job=null[  ]\n" +
                "111joe age=20 job=null[ V.age@~9=20 ]\n" +
                "111toe age=30 job=null[ V.age@~9=30 ]\n" +
                "111xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, testUtility.scanAll(t3, "111a", "111z", null));
    }

    @Test
    public void writeDeleteSameTransaction() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe81", 19);
        t0.commit();

        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe81", 20);
        testUtility.deleteRow(t1, "joe81");
        Assert.assertEquals("joe81 absent", testUtility.read(t1, "joe81"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe81 absent", testUtility.read(t2, "joe81"));
        t2.commit();
    }

    @Test
    public void deleteWriteSameTransaction() throws IOException {
        Txn t0 = control.beginTransaction();
        t0 = t0.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe82", 19);
        t0.commit();

        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(t1, "joe82");
        testUtility.insertAge(t1, "joe82", 20);
        Assert.assertEquals("joe82 age=20 job=null", testUtility.read(t1, "joe82"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe82 age=20 job=null", testUtility.read(t2, "joe82"));
        t2.commit();
    }

    @Test
    public void fourTransactions() throws Exception {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe7", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        Assert.assertEquals("joe7 age=20 job=null", testUtility.read(t2, "joe7"));
        testUtility.insertAge(t2, "joe7", 30);
        Assert.assertEquals("joe7 age=30 job=null", testUtility.read(t2, "joe7"));

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe7 age=20 job=null", testUtility.read(t3, "joe7"));

        t2.commit();

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe7 age=30 job=null", testUtility.read(t4, "joe7"));
    }

    @Test
    public void writeReadOnly() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe18", 20);
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe18 age=20 job=null", testUtility.read(t2, "joe18"));
        try {
            testUtility.insertAge(t2, "joe18", 21);
            fail("expected exception performing a write on a read-only transaction");
        } catch (IOException e) {
        }
    }

    @Test
    public void testReadCommittedVisibleWithHappensAfter() throws IOException {
				/*
				 * Tests that a read-committed transaction is able to see rows that were committed
				 *  before the transaction was started
				 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe19", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(Txn.IsolationLevel.READ_COMMITTED);
        Assert.assertEquals("joe19 age=20 job=null", testUtility.read(t2, "joe19"));
    }

    @Test
    public void testReadCommittedVisibleWithSimultaneousAction() throws IOException {
				/*
				 * Tests that a read-committed transaction is able to see rows that were committed,
				 * even if the commit TS is after the RC begin timestamp
				 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe20", 20);

        Txn t2 = control.beginTransaction(Txn.IsolationLevel.READ_COMMITTED);

        Assert.assertEquals("joe20 absent", testUtility.read(t2, "joe20"));
        t1.commit();
        Assert.assertEquals("joe20 age=20 job=null", testUtility.read(t2, "joe20"));
    }

    @Test
    public void testReadUncommittedRowsVisibleHappensAfter() throws IOException {
				/*
				 * Tests that read-uncommitted transactions are able to see rows that were committed
				 * before the RU transaction was started
				 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe22", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(Txn.IsolationLevel.READ_UNCOMMITTED);
        Assert.assertEquals("joe22 age=20 job=null", testUtility.read(t2, "joe22"));
    }

    @Test
    public void testsReadUncommittedRowsVisibleIfNotRolledBack() throws IOException {
				/*
				 * Tests that a read-uncommitted transaction is able to see rows, even if the
				 * writing transaction has not committed
				 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe21", 20);

        Txn t2 = control.beginTransaction(Txn.IsolationLevel.READ_UNCOMMITTED);

        Assert.assertEquals("joe21 age=20 job=null", testUtility.read(t2, "joe21"));
        t1.commit();
        Assert.assertEquals("joe21 age=20 job=null", testUtility.read(t2, "joe21"));
    }

    @Test
    public void testReadUncommittedCannotSeeRolledBackRows() throws IOException {
				/*
				 * Tests that a read-uncommitted transaction is not able to see
				 * rolled back rows
				 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe23", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe23", 21);

        Txn t3 = control.beginTransaction(Txn.IsolationLevel.READ_UNCOMMITTED);
        Assert.assertEquals("joe23 age=21 job=null", testUtility.read(t3, "joe23"));

        t2.rollback();
        Assert.assertEquals("joe23 age=20 job=null", testUtility.read(t3, "joe23"));

        Txn t4 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t4, "joe23", 22);
        Assert.assertEquals("joe23 age=22 job=null", testUtility.read(t3, "joe23"));
    }

    @Test
    public void childDependentTransactionWriteRollbackRead() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe24", 19);
        t0.commit();
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe24", 20);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        testUtility.insertAge(t2, "moe24", 21);
        Assert.assertEquals("joe24 age=20 job=null", testUtility.read(t1, "joe24"));
        Assert.assertEquals("moe24 absent", testUtility.read(t1, "moe24"));
        t2.rollback();
        Assert.assertEquals("joe24 age=20 job=null", testUtility.read(t1, "joe24"));
        Assert.assertEquals("moe24 absent", testUtility.read(t1, "moe24"));
        t1.commit();

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe24 age=20 job=null", testUtility.read(t3, "joe24"));
        Assert.assertEquals("moe24 absent", testUtility.read(t3, "moe24"));
    }

    @Test
    public void testRollbackDoesNothingToCommittedChildTransactions() throws IOException {
				/*
				 * Tests that rolling back a child transaction does nothing if the child transaction
				 * is already committed.
				 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe51", 21);
        t2.commit();
        t2.rollback();
        Assert.assertEquals("joe51 age=21 job=null", testUtility.read(t1, "joe51"));
        t1.commit();
    }

    @Test
    public void testRollbackWorksForCommittedChildSubTransactions() throws IOException {
				/*
				 * Tests that rolling back a child transaction works even if the child transaction
				 * is already committed when it's in memory.
				 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(t1, Txn.IsolationLevel.SNAPSHOT_ISOLATION, false, DESTINATION_TABLE, true);
        testUtility.insertAge(t2, "joe51", 21);
        t2.commit();
        t2.rollback();
        Assert.assertEquals("joe51 absent", testUtility.read(t1, "joe51"));
        t1.commit();
    }

    @Test
    public void testDependentChildSeesParentsWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe40", 20);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), null);
        Assert.assertEquals("joe40 age=20 job=null", testUtility.read(t2, "joe40"));
    }

    @Test
    public void childDependentTransactionWriteRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe25", 20);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "moe25", 21);
        Assert.assertEquals("joe25 age=20 job=null", testUtility.read(t1, "joe25"));
        Assert.assertEquals("moe25 absent", testUtility.read(t1, "moe25"));
        t2.commit();

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe25 absent", testUtility.read(t3, "joe25"));
        Assert.assertEquals("moe25 absent", testUtility.read(t3, "moe25"));

        Assert.assertEquals("joe25 age=20 job=null", testUtility.read(t1, "joe25"));
        Assert.assertEquals("moe25 age=21 job=null", testUtility.read(t1, "moe25"));
        t1.commit();

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe25 age=20 job=null", testUtility.read(t4, "joe25"));
        Assert.assertEquals("moe25 age=21 job=null", testUtility.read(t4, "moe25"));
    }

    @Test
    public void childDependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe37", 20);
        t0.commit();

        Txn t1 = control.beginTransaction();

        Txn otherTransaction = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(otherTransaction, "joe37", 30);
        otherTransaction.commit();

        Txn t2 = control.beginChildTransaction(noSubTxns(t1), null);
        Assert.assertEquals("joe37 age=20 job=null", testUtility.read(t2, "joe37"));
        t2.commit();
        t1.commit();
    }

    @Test
    public void multipleChildDependentTransactionWriteRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe26", 20);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "moe26", 21);
        testUtility.insertJob(t3, "boe26", "baker");
        Assert.assertEquals("joe26 age=20 job=null", testUtility.read(t1, "joe26"));
        Assert.assertEquals("moe26 absent", testUtility.read(t1, "moe26"));
        Assert.assertEquals("boe26 absent", testUtility.read(t1, "boe26"));

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe26 absent", testUtility.read(t4, "joe26"));
        Assert.assertEquals("moe26 absent", testUtility.read(t4, "moe26"));
        Assert.assertEquals("boe26 absent", testUtility.read(t4, "boe26"));

        Assert.assertEquals("joe26 age=20 job=null", testUtility.read(t1, "joe26"));
        Assert.assertEquals("boe26 absent", testUtility.read(t1, "boe26"));
        t3.commit();

        Txn t5 = control.beginTransaction();
        Assert.assertEquals("joe26 absent", testUtility.read(t5, "joe26"));
        Assert.assertEquals("moe26 absent", testUtility.read(t5, "moe26"));
        Assert.assertEquals("boe26 absent", testUtility.read(t5, "boe26"));

        Assert.assertEquals("boe26 age=null job=baker", testUtility.read(t1, "boe26"));
        t2.commit();
        t1.commit();

        Txn t6 = control.beginTransaction();
        Assert.assertEquals("joe26 age=20 job=null", testUtility.read(t6, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", testUtility.read(t6, "moe26"));
        Assert.assertEquals("boe26 age=null job=baker", testUtility.read(t6, "boe26"));
    }

    @Test
    public void multipleChildDependentTransactionsRollbackThenWrite() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe45", 20);
        testUtility.insertAge(t1, "boe45", 19);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe45", 21);
        Txn t3 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertJob(t3, "boe45", "baker");
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("joe45 age=21 job=null", testUtility.read(t2, "joe45"));
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t3, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));
        Assert.assertEquals("boe45 age=19 job=baker", testUtility.read(t3, "boe45"));
        t2.rollback();
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t3, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));
        Assert.assertEquals("boe45 age=19 job=baker", testUtility.read(t3, "boe45"));
        t3.rollback();
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));
        Txn t4 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t4, "joe45", 24);
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("joe45 age=24 job=null", testUtility.read(t4, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t4, "boe45"));
        t4.rollback();
        Assert.assertEquals("joe45 age=20 job=null", testUtility.read(t1, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", testUtility.read(t1, "boe45"));

        Txn t5 = control.beginTransaction(Txn.IsolationLevel.READ_COMMITTED);
        Assert.assertEquals("joe45 absent", testUtility.read(t5, "joe45"));
        Assert.assertEquals("boe45 absent", testUtility.read(t5, "boe45"));
        t1.commit();
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
//        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
//        testUtility.insertAge(t1, "joe46", 20);
//        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), DESTINATION_TABLE);
//        testUtility.insertJob(t2, "moe46", "baker");
//        t2.commit();
//				Assert.assertEquals("joe46 age=20 job=null", testUtility.read(t1, "joe46"));
//				Assert.assertEquals("moe46 age=null job=baker", testUtility.read(t1, "moe46"));
//				t1.rollback();
//        Txn t3 = control.beginChildTransaction(t1,t1.getIsolationLevel(), DESTINATION_TABLE);
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
    public void childDependentTransactionWriteRollbackParentRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe27", 20);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "moe27", 21);
        t2.commit();
        t1.rollback();

        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe27 absent", testUtility.read(t4, "joe27"));
        Assert.assertEquals("moe27 absent", testUtility.read(t4, "moe27"));
    }

    @Test
    public void commitParentOfCommittedDependent() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe32", 20);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "moe32", 21);
        t2.commit();

//        final Transaction transactionStatusA = transactorSetup.transactionStore.getTransaction(t2.commit();
        TxnView t2Check = txnStore.getTransaction(t2.getTxnId());
        Assert.assertNotEquals("committing a child does not set a local commit timestamp", -1l, t2Check.getCommitTimestamp());
        Assert.assertEquals("child has effective commit timestamp even though parent has not committed!", -1l, t2Check.getEffectiveCommitTimestamp());
        long earlyCommit = t2Check.getCommitTimestamp();
        t1.commit();

        t2Check = txnStore.getTransaction(t2.getTxnId());
        Assert.assertEquals("committing parent of dependent transaction should not change the commit time of the child",
                earlyCommit, t2Check.getCommitTimestamp());
        Assert.assertTrue("incorrect effective commit timestamp!", t2Check.getEffectiveCommitTimestamp() >= 0);
    }

    @Test
    public void dependentWriteFollowedByReadCommittedWriter() throws IOException {
        Txn parent = control.beginTransaction(DESTINATION_TABLE);

        Txn child = control.beginChildTransaction(parent, parent.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(child, "joe34", 22);
        child.commit();

        Txn other = control.beginTransaction(Txn.IsolationLevel.READ_COMMITTED, DESTINATION_TABLE);
        try {
            testUtility.insertAge(other, "joe34", 21);
            fail("No write conflict detected");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            other.rollback();
        }
    }

    @Test
    public void dependentWriteCommitParentFollowedByReadCommittedWriter() throws IOException {
        Txn parent = control.beginTransaction(DESTINATION_TABLE);

        Txn child = control.beginChildTransaction(parent, DESTINATION_TABLE);
        testUtility.insertAge(child, "joe94", 22);
        child.commit();
        parent.commit();

        Txn other = control.beginTransaction(Txn.IsolationLevel.READ_COMMITTED, DESTINATION_TABLE);
        testUtility.insertAge(other, "joe94", 21);
        other.commit();
    }

    @Test
    public void dependentWriteOverlapWithReadCommittedWriter() throws IOException {
        Txn parent = control.beginTransaction(DESTINATION_TABLE);

        Txn other = control.beginTransaction(Txn.IsolationLevel.READ_COMMITTED, DESTINATION_TABLE);

        Txn child = control.beginChildTransaction(parent, DESTINATION_TABLE);
        testUtility.insertAge(child, "joe36", 22);
        child.commit();

        try {
            testUtility.insertAge(other, "joe36", 21);
            fail();
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            other.rollback();
        }
    }

    @Test
    public void taskRetryTest() throws IOException {
        CountingConstraintChecker ccc = new CountingConstraintChecker(testEnv.getOperationStatusFactory());
        transactorSetup.transactor.setDefaultConstraintChecker(ccc);
        try {
            Txn t1 = control.beginTransaction(DESTINATION_TABLE);
            TaskId task1 = new TaskId(1, 1, 0);
            Txn t11 = control.beginChildTransaction(t1, t1.getIsolationLevel(), true, DESTINATION_TABLE, false, task1);
            testUtility.insertAge(t11, "joe43", 20);
            TaskId task2 = new TaskId(1, 1, 1);
            Txn t12 = control.beginChildTransaction(t1, t1.getIsolationLevel(), true, DESTINATION_TABLE, false, task2);
            testUtility.insertAge(t12, "joe43", 25);
            t11.rollback();
            t12.commit();

            assertEquals("Constraints checked, transactions 'conflicted'",0,  ccc.count);

            Assert.assertEquals("joe43 age=25 job=null", testUtility.read(t1, "joe43"));
            t1.commit();
        } finally {
            transactorSetup.transactor.setDefaultConstraintChecker(testEnv.getOperationStatusFactory().getNoOpConstraintChecker());
        }
    }

    private static class CountingConstraintChecker implements ConstraintChecker {

        private final OperationStatusFactory opFactory;
        private int count = 0;

        CountingConstraintChecker(OperationStatusFactory opFactory) {
            this.opFactory = opFactory;
        }

        @Override
        public MutationStatus checkConstraint(KVPair mutation, DataResult existingRow) throws IOException {
            count++;
            return opFactory.success();
        }
    }

    @Ignore
    @Test
    public void taskRetryConflict() throws IOException {
        CountingConstraintChecker ccc = new CountingConstraintChecker(testEnv.getOperationStatusFactory());
        transactorSetup.transactor.setDefaultConstraintChecker(ccc);
        try {
            Txn t1 = control.beginTransaction(DESTINATION_TABLE);
            TaskId task2 = new TaskId(1, 1, 1);
            Txn t12 = control.beginChildTransaction(t1, t1.getIsolationLevel(), true, DESTINATION_TABLE, false, task2);
            testUtility.insertAge(t12, "joe133", 25);
            TaskId task1 = new TaskId(1, 1, 0);
            Txn t11 = control.beginChildTransaction(t1, t1.getIsolationLevel(), true, DESTINATION_TABLE, false, task1);
            testUtility.insertAge(t11, "joe133", 20);
            t12.commit();
            t11.rollback();

            assertEquals("No constraints checked, transactions didn't 'conflict'",1, ccc.count);

            Assert.assertEquals("joe133 age=25 job=null", testUtility.read(t1, "joe133"));
            t1.commit();
        } finally {
            transactorSetup.transactor.setDefaultConstraintChecker(testEnv.getOperationStatusFactory().getNoOpConstraintChecker());
        }
    }

    @Test
    public void rollbackUpdate() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe43", 20);
        t1.commit();

        Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe43", 21);
        t2.rollback();

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe43 age=20 job=null", testUtility.read(t3, "joe43"));
    }

    @Test
    public void rollbackInsert() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe44", 20);
        t1.rollback();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe44 absent", testUtility.read(t2, "joe44"));
    }

    @Test
    public void childrenOfChildrenCommitCommitCommit() throws IOException {
        Txn t1 = noSubTxns(control.beginTransaction(DESTINATION_TABLE));
        Txn t2 = noSubTxns(control.beginChildTransaction(t1, DESTINATION_TABLE));
        Txn t3 = control.beginChildTransaction(t2, DESTINATION_TABLE);
        testUtility.insertAge(t3, "joe53", 20);
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t3, "joe53"));
        t3.commit();
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t3, "joe53"));
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t2, "joe53"));
        testUtility.insertAge(t2, "boe53", 21);
        Assert.assertEquals("boe53 age=21 job=null", testUtility.read(t2, "boe53"));
        t2.commit();
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t1, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", testUtility.read(t1, "boe53"));
        t1.commit();
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe53 age=20 job=null", testUtility.read(t4, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", testUtility.read(t4, "boe53"));
    }

    @Test
    public void childrenOfChildrenCommitCommitCommitParentWriteFirst() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(t2, t2.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe57", 18);
        testUtility.insertAge(t1, "boe57", 19);
        testUtility.insertAge(t2, "boe57", 21);
        testUtility.insertAge(t3, "joe57", 20);
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t3, "joe57"));
        t3.commit();
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t3, "joe57"));
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t2, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", testUtility.read(t2, "boe57"));
        t2.commit();
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t1, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", testUtility.read(t1, "boe57"));
        t1.commit();
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe57 age=20 job=null", testUtility.read(t4, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", testUtility.read(t4, "boe57"));
    }

    @Test
    public void childrenOfChildrenCommitCommitRollback() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(t1, DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(t2, DESTINATION_TABLE);
        testUtility.insertAge(t3, "joe54", 20);
        Assert.assertEquals("joe54 age=20 job=null", testUtility.read(t3, "joe54"));
        t3.rollback();
        Assert.assertEquals("joe54 absent", testUtility.read(t2, "joe54"));
        testUtility.insertAge(t2, "boe54", 21);
        Assert.assertEquals("boe54 age=21 job=null", testUtility.read(t2, "boe54"));
        t2.commit();
        Assert.assertEquals("joe54 absent", testUtility.read(t1, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", testUtility.read(t1, "boe54"));
        t1.commit();
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe54 absent", testUtility.read(t4, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", testUtility.read(t4, "boe54"));
    }

    @Test
    public void childrenOfChildrenWritesDoNotConflict() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
		/*
		 * In order to be transactionally value, we cannot write while we have active
		 * child transactions (otherwise we violate the requirement that events
		 * occur sequentially). Thus, we have to write the data first, then
		 * we can begin the child transactions.
		 */
        testUtility.insertAge(t1, "joe95", 18);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(noSubTxns(t2), DESTINATION_TABLE);
        testUtility.insertAge(t3, "joe95", 20);
        Assert.assertEquals("joe95 age=18 job=null", testUtility.read(t1, "joe95"));
        Assert.assertEquals("joe95 age=18 job=null", testUtility.read(t2, "joe95"));
        Assert.assertEquals("joe95 age=20 job=null", testUtility.read(t3, "joe95"));
    }

    @Test
    public void testCannotCreateWritableChildOfParentTxn() throws Exception {
        Txn t1 = control.beginTransaction();
        error.expect(IOException.class);
        Txn t2 = control.beginChildTransaction(t1, DESTINATION_TABLE);
    }

    @Test
    public void testParentReadsChildWritesInReadCommittedMode() throws IOException {
		/*
		 * This tests two things:
		 *
		 * 1. that a parent's writes do not conflict with its children's writes, if the
		 * children's writes occurred before the parent.
		 *
		 */
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe101", 20);
        t2.commit();
        testUtility.insertAge(t1, "joe101", 21);
        Assert.assertEquals("joe101 age=21 job=null", testUtility.read(t1, "joe101"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorChildDelete() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe105");
        t2.commit();
        testUtility.insertAge(t1, "joe105", 21);
        Assert.assertEquals("joe105 age=21 job=null", testUtility.read(t1, "joe105"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorChildDelete2() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe141", 20);
        t0.commit();
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe141");
        t2.commit();
        testUtility.insertAge(t1, "joe141", 21);
        Assert.assertEquals("joe141 age=21 job=null", testUtility.read(t1, "joe141"));
    }

    @Test
    public void parentDeleteDoesNotConflictWithPriorChildDelete() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe109");
        t2.commit();
        testUtility.deleteRow(t1, "joe109");
        Assert.assertEquals("joe109 absent", testUtility.read(t1, "joe109"));
        testUtility.insertAge(t1, "joe109", 21);
        Assert.assertEquals("joe109 age=21 job=null", testUtility.read(t1, "joe109"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveChildWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe102", 20);
        testUtility.insertAge(t1, "joe102", 21);
        Assert.assertEquals("joe102 age=21 job=null", testUtility.read(t1, "joe102"));
        t2.commit();
        Assert.assertEquals("joe102 age=21 job=null", testUtility.read(t1, "joe102"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveChildDelete() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe106");
        testUtility.insertAge(t1, "joe106", 21);
        Assert.assertEquals("joe106 age=21 job=null", testUtility.read(t1, "joe106"));
        t2.commit();
        Assert.assertEquals("joe106 age=21 job=null", testUtility.read(t1, "joe106"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorIndependentChildWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe103", 20);
        t2.commit();
        testUtility.insertAge(t1, "joe103", 21);
        Assert.assertEquals("joe103 age=21 job=null", testUtility.read(t1, "joe103"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorIndependentChildDelete() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe107");
        t2.commit();
        testUtility.insertAge(t1, "joe107", 21);
        Assert.assertEquals("joe107 age=21 job=null", testUtility.read(t1, "joe107"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveIndependentChildWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe104", 20);
        testUtility.insertAge(t1, "joe104", 21);
        Assert.assertEquals("joe104 age=21 job=null", testUtility.read(t1, "joe104"));
        t2.commit();
        Assert.assertEquals("joe104 age=21 job=null", testUtility.read(t1, "joe104"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveIndependentChildDelete() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), DESTINATION_TABLE);
        testUtility.deleteRow(t2, "joe108");
        testUtility.insertAge(t1, "joe108", 21);
        Assert.assertEquals("joe108 age=21 job=null", testUtility.read(t1, "joe108"));
        t2.commit();
        Assert.assertEquals("joe108 age=21 job=null", testUtility.read(t1, "joe108"));
    }

    @Test
    public void childrenOfChildrenCommitCommitRollbackParentWriteFirst() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(t2, t2.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe58", 18);
        testUtility.insertAge(t1, "boe58", 19);
        testUtility.insertAge(t2, "boe58", 21);
        testUtility.insertAge(t3, "joe58", 20);
        Assert.assertEquals("joe58 age=20 job=null", testUtility.read(t3, "joe58"));
        t3.rollback();
        Assert.assertEquals("joe58 age=18 job=null", testUtility.read(t2, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", testUtility.read(t2, "boe58"));
        t2.commit();
        Assert.assertEquals("joe58 age=18 job=null", testUtility.read(t1, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", testUtility.read(t1, "boe58"));
        t1.commit();
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe58 age=18 job=null", testUtility.read(t4, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", testUtility.read(t4, "boe58"));
    }

    @Test
    public void childrenOfChildrenCommitRollbackCommit() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(t2, t2.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t3, "joe55", 20);
        Assert.assertEquals("joe55 age=20 job=null", testUtility.read(t3, "joe55"));
        t3.commit();
        Assert.assertEquals("joe55 age=20 job=null", testUtility.read(t3, "joe55"));
        Assert.assertEquals("joe55 age=20 job=null", testUtility.read(t2, "joe55"));
        testUtility.insertAge(t2, "boe55", 21);
        Assert.assertEquals("boe55 age=21 job=null", testUtility.read(t2, "boe55"));
        t2.rollback();
        Assert.assertEquals("joe55 absent", testUtility.read(t1, "joe55"));
        Assert.assertEquals("boe55 absent", testUtility.read(t1, "boe55"));
        t1.commit();
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe55 absent", testUtility.read(t4, "joe55"));
        Assert.assertEquals("boe55 absent", testUtility.read(t4, "boe55"));
    }

    @Test
    public void childrenOfChildrenCommitRollbackCommitParentWriteFirst() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe59", 18);
        testUtility.insertAge(t1, "boe59", 19);
        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(t2, t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "boe59", 21);
        testUtility.insertAge(t3, "joe59", 20);
        Assert.assertEquals("joe59 age=20 job=null", testUtility.read(t3, "joe59"));
        t3.commit();
        Assert.assertEquals("joe59 age=20 job=null", testUtility.read(t2, "joe59"));
        Assert.assertEquals("boe59 age=21 job=null", testUtility.read(t2, "boe59"));
        t2.rollback();
        Assert.assertEquals("joe59 age=18 job=null", testUtility.read(t1, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", testUtility.read(t1, "boe59"));
        t1.commit();
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe59 age=18 job=null", testUtility.read(t4, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", testUtility.read(t4, "boe59"));
    }

    @Test
    public void childrenOfChildrenRollbackCommitCommitParentWriteFirst() throws IOException {
        Txn t1 = noSubTxns(control.beginTransaction(DESTINATION_TABLE));
        Txn t2 = noSubTxns(control.beginChildTransaction(t1, DESTINATION_TABLE));
        Txn t3 = control.beginChildTransaction(t2, DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe60", 18);
        testUtility.insertAge(t1, "boe60", 19);
        testUtility.insertAge(t2, "doe60", 21);
        testUtility.insertAge(t3, "moe60", 30);
        Assert.assertEquals("moe60 age=30 job=null", testUtility.read(t3, "moe60"));
        t3.commit();
        Assert.assertEquals("moe60 age=30 job=null", testUtility.read(t3, "moe60"));
        Assert.assertEquals("doe60 age=21 job=null", testUtility.read(t2, "doe60"));
        testUtility.insertAge(t2, "doe60", 22);
        Assert.assertEquals("doe60 age=22 job=null", testUtility.read(t2, "doe60"));
        t2.commit();
        Assert.assertEquals("joe60 age=18 job=null", testUtility.read(t1, "joe60"));
        Assert.assertEquals("boe60 age=19 job=null", testUtility.read(t1, "boe60"));
        Assert.assertEquals("moe60 age=30 job=null", testUtility.read(t1, "moe60"));
        Assert.assertEquals("doe60 age=22 job=null", testUtility.read(t1, "doe60"));
        t1.rollback();
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe60 absent", testUtility.read(t4, "joe60"));
        Assert.assertEquals("boe60 absent", testUtility.read(t4, "boe60"));
    }

    private static Txn noSubTxns(Txn txn) {
        return new ForwardingTxnView(txn) {
            @Override
            public boolean allowsSubtransactions() {
                return false;
            }
        };
    }

    @Test
    public void childrenOfChildrenRollbackCommitCommit() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(t2, t2.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t3, "joe56", 20);
        Assert.assertEquals("joe56 age=20 job=null", testUtility.read(t3, "joe56"));
        t3.commit();
        Assert.assertEquals("joe56 age=20 job=null", testUtility.read(t2, "joe56"));
        testUtility.insertAge(t2, "boe56", 21);
        Assert.assertEquals("boe56 age=21 job=null", testUtility.read(t2, "boe56"));
        t2.commit();
        Assert.assertEquals("joe56 age=20 job=null", testUtility.read(t1, "joe56"));
        Assert.assertEquals("boe56 age=21 job=null", testUtility.read(t1, "boe56"));
        t1.rollback();
        Txn t4 = control.beginTransaction();
        Assert.assertEquals("joe56 absent", testUtility.read(t4, "joe56"));
        Assert.assertEquals("boe56 absent", testUtility.read(t4, "boe56"));
    }

    private void sleep() throws InterruptedException {
        final Clock clock =  testEnv.getClock();
        clock.sleep(2000,TimeUnit.MILLISECONDS);
    }

    @Test
    public void testWritesWithRolledBackTxnStillThrowWriteConflict() throws IOException, InterruptedException {
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe66", 20);
        final Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        try {
            testUtility.insertAge(t2, "joe66", 22);
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            t2.rollback();
        }
        try {
            testUtility.insertAge(t2, "joe66", 23);
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            t2.rollback();
        }
    }


    @Test
    public void testRolledBackTxnDoesntMaskEarlierConflicts() throws IOException, InterruptedException {
        try {
            final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
            final Txn t2 = control.beginTransaction(DESTINATION_TABLE);
            final Txn t3 = control.beginTransaction(DESTINATION_TABLE);
            testUtility.insertAge(t2, "joe66", 20);
            t2.rollback();
            testUtility.insertAge(t1, "joe66", 22);
            testUtility.insertAge(t3, "joe66", 23);

            fail("Expected WW conflict");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        }
    }


    @Test
    public void testRolledBackTxnDoesntMaskConflictsWithChildTxn() throws IOException, InterruptedException {
        try {
            final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
            final Txn t2 = control.beginTransaction(DESTINATION_TABLE);
            testUtility.insertAge(t1, "joe66", 22);
            t1.commit();

            final Txn t3 = control.beginTransaction(DESTINATION_TABLE);
            testUtility.insertAge(t3, "joe66", 23);
            t3.rollback();

            final Txn t22 = control.beginChildTransaction(t2, DESTINATION_TABLE);

            testUtility.insertAge(t22, "joe66", 20);
            t2.commit();

            fail("Expected WW conflict");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        }
    }

    @Test
    public void childIndependentTransactionWriteCommitRollbackRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe52", 21);
        t2.commit();
        t2.rollback();
        Assert.assertEquals("joe52 age=21 job=null", testUtility.read(t1, "joe52"));
        t1.commit();
    }

    @Test
    public void childIndependentSeesParentWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe41", 20);
        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), DESTINATION_TABLE);
        Assert.assertEquals("joe41 age=20 job=null", testUtility.read(t2, "joe41"));
    }

    @Test
    public void childIndependentReadOnlySeesParentWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe96", 20);
        final Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), null);
        Assert.assertEquals("joe96 age=20 job=null", testUtility.read(t2, "joe96"));
    }

    @Test
    public void childIndependentReadUncommittedDoesSeeParentWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe99", 20);
//        Txn t2 = control.beginChildTransaction(t1, false, true, false, true, true, null);
        Txn t2 = control.beginChildTransaction(t1, Txn.IsolationLevel.READ_UNCOMMITTED, false, DESTINATION_TABLE);
        Assert.assertEquals("joe99 age=20 job=null", testUtility.read(t2, "joe99"));
    }

    @Test
    public void childIndependentReadOnlyUncommittedDoesSeeParentWrites() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe100", 20);
//        final Txn t2 = control.beginChildTransaction(t1, false, false, false, true, true, null);
        final Txn t2 = control.beginChildTransaction(t1, Txn.IsolationLevel.READ_UNCOMMITTED, false, null);
        Assert.assertEquals("joe100 age=20 job=null", testUtility.read(t2, "joe100"));
    }

    @Test
    public void childIndependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe38", 20);
        t0.commit();

        Txn t1 = control.beginTransaction(DESTINATION_TABLE);

        Txn otherTransaction = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(otherTransaction, "joe38", 30);
        otherTransaction.commit();

//        Txn t2 = control.beginChildTransaction(t1, false, true, false, null, true, null);
        Txn t2 = control.beginChildTransaction(t1, Txn.IsolationLevel.READ_COMMITTED, false, DESTINATION_TABLE);
        Assert.assertEquals("joe38 age=30 job=null", testUtility.read(t2, "joe38"));
        t2.commit();
        t1.commit();
    }

    @Test
    public void childIndependentReadOnlyTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe97", 20);
        t0.commit();

        Txn t1 = control.beginTransaction(DESTINATION_TABLE);

        Txn otherTransaction = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(otherTransaction, "joe97", 30);
        otherTransaction.commit();

//        Txn t2 = control.beginChildTransaction(t1, false, false, false, null, true, null);
        Txn t2 = control.beginChildTransaction(t1, Txn.IsolationLevel.READ_COMMITTED, false, DESTINATION_TABLE);
        Assert.assertEquals("joe97 age=30 job=null", testUtility.read(t2, "joe97"));
        t2.commit();
        t1.commit();
    }

    @Test
    public void childIndependentReadOnlyTransactionWithReadCommittedOffWithOtherCommitBetweenParentAndChild() throws IOException {
        Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t0, "joe98", 20);
        t0.commit();

        Txn t1 = control.beginTransaction();

        Txn otherTransaction = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(otherTransaction, "joe98", 30);
        otherTransaction.commit();

        Txn t2 = control.beginChildTransaction(t1, t1.getIsolationLevel(), null);
        Assert.assertEquals("joe98 age=20 job=null", testUtility.read(t2, "joe98"));
        t2.commit();
        t1.commit();
    }

    @Test
    public void childIndependentTransactionWriteRollbackRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe28", 20);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "moe28", 21);
        Assert.assertEquals("joe28 age=20 job=null", testUtility.read(t1, "joe28"));
        Assert.assertEquals("moe28 absent", testUtility.read(t1, "moe28"));
        t2.rollback();
        Assert.assertEquals("joe28 age=20 job=null", testUtility.read(t1, "joe28"));
        Assert.assertEquals("moe28 absent", testUtility.read(t1, "moe28"));
        t1.commit();

        Txn t3 = control.beginTransaction();
        Assert.assertEquals("joe28 age=20 job=null", testUtility.read(t3, "joe28"));
        Assert.assertEquals("moe28 absent", testUtility.read(t3, "moe28"));
    }

    @Test
    public void multipleChildIndependentConflict() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        TxnView t1v = new DDLTxnView(t1, t1.getBeginTimestamp());
        Txn t2 = control.beginChildTransaction(t1v, t1.getIsolationLevel(), DESTINATION_TABLE);
        Txn t3 = control.beginChildTransaction(t1v, t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "moe31", 21);
        try {
            testUtility.insertJob(t3, "moe31", "baker");
            fail();
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            t3.rollback();
        }
    }

    @Test
    public void commitParentOfCommittedIndependent() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe49", 20);
        Txn t2 = control.beginChildTransaction(noSubTxns(t1), t1.getIsolationLevel(), DESTINATION_TABLE);
        testUtility.insertAge(t2, "moe49", 21);
        t2.commit();
        TxnView toCheckA = txnStore.getTransaction(t2.getTxnId());
        t1.commit();
        TxnView toCheckB = txnStore.getTransaction(t2.getTxnId());
        Assert.assertEquals("committing parent of independent transaction should not change the commit time of the child",
                toCheckA.getCommitTimestamp(), toCheckB.getCommitTimestamp());
    }


    @Test
    public void writeAllNullsRead() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe113 absent", testUtility.read(t1, "joe113"));
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe113", null);
        Assert.assertEquals("joe113 age=null job=null", testUtility.read(t1, "joe113"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe113 age=null job=null", testUtility.read(t2, "joe113"));
    }

    @Test
    public void writeAllNullsReadOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe114 absent", testUtility.read(t1, "joe114"));
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe114", null);
        Assert.assertEquals("joe114 age=null job=null", testUtility.read(t1, "joe114"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe114 age=null job=null", testUtility.read(t1, "joe114"));
        Assert.assertEquals("joe114 absent", testUtility.read(t2, "joe114"));
        t1.commit();
        Assert.assertEquals("joe114 absent", testUtility.read(t2, "joe114"));
    }


    @Test
    public void writeAllNullsWrite() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe115", null);
        Assert.assertEquals("joe115 age=null job=null", testUtility.read(t1, "joe115"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe115 age=null job=null", testUtility.read(t2, "joe115"));
        t2 = t2.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t2, "joe115", 30);
        Assert.assertEquals("joe115 age=30 job=null", testUtility.read(t2, "joe115"));
        t2.commit();
    }

    @Test
    public void writeAllNullsWriteOverlap() throws IOException {
        Txn t1 = control.beginTransaction();
        Assert.assertEquals("joe116 absent", testUtility.read(t1, "joe116"));
        t1 = t1.elevateToWritable(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe116", null);
        Assert.assertEquals("joe116 age=null job=null", testUtility.read(t1, "joe116"));

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe116 age=null job=null", testUtility.read(t1, "joe116"));
        Assert.assertEquals("joe116 absent", testUtility.read(t2, "joe116"));
        t2 = t2.elevateToWritable(DESTINATION_TABLE);
        try {
            testUtility.insertAge(t2, "joe116", 30);
            fail("Allowed insertion");
        } catch (IOException e) {
            testUtility.assertWriteConflict(e);
        } finally {
            t2.rollback();
        }
        Assert.assertEquals("joe116 age=null job=null", testUtility.read(t1, "joe116"));
        t1.commit();
        error.expect(IsInstanceOf.instanceOf(CannotCommitException.class));
        t2.commit();
        fail("Was able to comit a rolled back transaction");
    }

    @Test
    public void readAllNullsAfterCommit() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe117", null);
        t1.commit();
        Assert.assertEquals("joe117 age=null job=null", testUtility.read(t1, "joe117"));
    }

    @Test
    public void rollbackAllNullAfterCommit() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe118", null);
        t1.commit();
        t1.rollback();
        Assert.assertEquals("joe118 age=null job=null", testUtility.read(t1, "joe118"));
    }

    @Test
    public void writeAllNullScan() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Assert.assertEquals("joe119 absent", testUtility.read(t1, "joe119"));
        testUtility.insertAge(t1, "joe119", null);
        Assert.assertEquals("joe119 age=null job=null", testUtility.read(t1, "joe119"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe119 age=null job=null[  ]", testUtility.scan(t2, "joe119"));

        Assert.assertEquals("joe119 age=null job=null", testUtility.read(t2, "joe119"));
    }

    @Test
    public void batchWriteRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        Assert.assertEquals("joe144 absent", testUtility.read(t1, "joe144"));
        testUtility.insertAgeBatch(new Object[]{t1, "joe144", 20}, new Object[]{t1, "bob144", 30});
        Assert.assertEquals("joe144 age=20 job=null", testUtility.read(t1, "joe144"));
        Assert.assertEquals("bob144 age=30 job=null", testUtility.read(t1, "bob144"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe144 age=20 job=null", testUtility.read(t2, "joe144"));
        Assert.assertEquals("bob144 age=30 job=null", testUtility.read(t2, "bob144"));
    }

    @Test
    public void testDeleteThenInsertWithinChildTransactions() throws Exception {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joel", 20);
        t1.commit();


        Txn p = control.beginTransaction(DESTINATION_TABLE);
        Txn c1 = control.beginChildTransaction(p, DESTINATION_TABLE);
        testUtility.deleteRow(c1, "joel");
        c1.commit();
        Txn c2 = control.beginChildTransaction(p, DESTINATION_TABLE);
        Assert.assertEquals("joel absent", testUtility.read(c2, "joel"));
        testUtility.insertAge(c2, "joel", 22);
        Assert.assertEquals("joel age=22 job=null", testUtility.read(c2, "joel"));
    }

    @Test
    public void writeDeleteBatchInsertRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertAge(t1, "joe145", 10);
        testUtility.deleteRow(t1, "joe145");
        testUtility.insertAgeBatch(new Object[]{t1, "joe145", 20}, new Object[]{t1, "bob145", 30});
        Assert.assertEquals("joe145 age=20 job=null", testUtility.read(t1, "joe145"));
        Assert.assertEquals("bob145 age=30 job=null", testUtility.read(t1, "bob145"));
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("joe145 age=20 job=null", testUtility.read(t2, "joe145"));
        Assert.assertEquals("bob145 age=30 job=null", testUtility.read(t2, "bob145"));
    }

    @Test
    public void writeManyDeleteBatchInsertRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
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
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("146joe age=11 job=null", testUtility.read(t2, "146joe"));
        Assert.assertEquals("146doe age=21 job=null", testUtility.read(t2, "146doe"));
        Assert.assertEquals("146boe age=31 job=null", testUtility.read(t2, "146boe"));
        Assert.assertEquals("146moe age=41 job=null", testUtility.read(t2, "146moe"));
        Assert.assertEquals("146zoe age=51 job=null", testUtility.read(t2, "146zoe"));
    }

    @Test
    public void writeManyDeleteBatchInsertSomeRead() throws IOException {
        Txn t1 = control.beginTransaction(DESTINATION_TABLE);
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
        t1.commit();

        Txn t2 = control.beginTransaction();
        Assert.assertEquals("147joe age=11 job=null", testUtility.read(t2, "147joe"));
        Assert.assertEquals("147boe age=31 job=null", testUtility.read(t2, "147boe"));
        Assert.assertEquals("147zoe age=51 job=null", testUtility.read(t2, "147zoe"));
    }

    // Commit & begin together tests
    @Test
    public void testCommitAndBeginSeparate() throws IOException {
        final Txn t1 = control.beginTransaction();
        final Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        t1.commit();
        final Txn t3 = control.beginChildTransaction(t2, t2.getIsolationLevel(), false, DESTINATION_TABLE);
        Assert.assertEquals(t1.getTxnId() + SIConstants.TRASANCTION_INCREMENT, t2.getTxnId());
        // next ID burned for commit
        Assert.assertEquals(t1.getTxnId() + SIConstants.TRASANCTION_INCREMENT*2, t3.getTxnId());
    }

    @Test
    public void testInmemorySubtransactionsConsecutive() throws IOException {
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        final Txn t2 = control.beginChildTransaction(t1, Txn.IsolationLevel.SNAPSHOT_ISOLATION, false, DESTINATION_TABLE, true);
        final Txn t3 = control.beginChildTransaction(t2, Txn.IsolationLevel.SNAPSHOT_ISOLATION, false, DESTINATION_TABLE, true);
        final Txn t4 = control.beginChildTransaction(t3, Txn.IsolationLevel.SNAPSHOT_ISOLATION, false, DESTINATION_TABLE, true);

        Assert.assertEquals(t1.getTxnId() + 1, t2.getTxnId());
        Assert.assertEquals(t1.getTxnId() + 2, t3.getTxnId());
        Assert.assertEquals(t1.getTxnId() + 3, t4.getTxnId());
        // next ID burned for commit
        t1.rollback();
    }

    @Test
    public void testCommitAndBeginTogether() throws IOException {
        final Txn t1 = control.beginTransaction();
        final Txn t2 = control.beginTransaction(DESTINATION_TABLE);
//        final Txn t3 = control.beginChildTransaction(t2, true, true, false, null, null, t1.commit();
        final Txn t3 = control.beginChildTransaction(t2, t2.getIsolationLevel(), false, DESTINATION_TABLE, true);
        Assert.assertEquals(t1.getTxnId() + SIConstants.TRASANCTION_INCREMENT, t2.getTxnId());
        // no ID burned for commit
        Assert.assertEquals(t1.getTxnId() + SIConstants.TRASANCTION_INCREMENT+1, t3.getTxnId());
    }

    @Test
    public void testCommitNonRootAndBeginTogether() throws IOException {
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        final Txn t2 = control.beginChildTransaction(t1, DESTINATION_TABLE);
        final Txn t3 = control.beginTransaction();
//            control.beginChildTransaction(t3, true, true, false, null, null, t2);
        error.expect(IOException.class);
        control.chainTransaction(t3, t3.getIsolationLevel(), true, DESTINATION_TABLE, t2);
        fail();
    }
}
