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

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TransactionMissing;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.impl.ForwardingTxnView;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.si.testenv.*;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import splice.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Tests that indicate the expected behavior of transactions, particularly
 * with respect to child transactions.
 *
 * This is mainly in place due to frustrations in attempting to conceptually deal
 * with the tests present in SITransactorTest, and thus some behavioral testing
 * may be duplicated here (but hopefully in a clearer and more precise manner).
 *
 * Some of the tests here do NOT reflect expected SQL reality (for example, you cannot
 * create a delete operation until a prior insert operation has either committed or rolled back);
 * however, they serve to illustrate the transactional semantics at the code level (like a tutorial
 * on how to use the transaction system).
 *
 * @author Scott Fines
 * Date: 8/21/14
 */
@Category(ArchitectureSpecific.class)
public class TransactionInteractionTest {
    @Rule public ExpectedException error = ExpectedException.none();
    private static final byte[] DESTINATION_TABLE = Bytes.toBytes("1184");

    private static SITestEnv testEnv;
    private static TestTransactionSetup transactorSetup;
    private TxnLifecycleManager control;
    private TransactorTestUtility testUtility;
    private final List<Txn> createdParentTxns = Lists.newArrayList();
    private TxnStore txnStore;

    @SuppressWarnings("unchecked")
    private void baseSetUp() {
        control = new ForwardingLifecycleManager(transactorSetup.txnLifecycleManager){
            @Override
            protected void afterStart(Txn txn) {
                createdParentTxns.add(txn);
            }
        };
        testUtility = new TransactorTestUtility(true,testEnv,transactorSetup);
        txnStore = transactorSetup.txnStore;
    }

    @Before
    public void setUp() throws IOException {
        if(testEnv==null){
            testEnv=SITestEnvironment.loadTestEnvironment();
            transactorSetup=new TestTransactionSetup(testEnv,true);
        }
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
    public void testCanElevateGrandchildCorrectly() throws Exception {
        Txn user = control.beginTransaction();
        Txn child = control.beginChildTransaction(user, null);
        Txn grandchild = control.beginChildTransaction(child,null);

        //now elevate in sequence, and verify that the hierarchy remains correct
        user = user.elevateToWritable(DESTINATION_TABLE);
        ((ReadOnlyTxn)child).parentWritable(user);
        child = child.elevateToWritable(DESTINATION_TABLE);
        Assert.assertEquals("Incorrect parent transaction for child!",user,child.getParentTxnView());
        ((ReadOnlyTxn)grandchild).parentWritable(child);
        grandchild = grandchild.elevateToWritable(DESTINATION_TABLE);
        Assert.assertEquals("Incorrect parent transaction for grandchild!",child,grandchild.getParentTxnView());
        Assert.assertEquals("Incorrect grandparent transaction for grandchild!", user, grandchild.getParentTxnView().getParentTxnView());
        //now check that fetching the grandchild is still correct
        TxnView userView = testEnv.getTxnStore().getTransaction(user.getTxnId());
        TxnView childView = testEnv.getTxnStore().getTransaction(child.getTxnId());
        Assert.assertEquals("Incorrect parent transaction for child!",userView.getParentTxnView(),childView.getParentTxnView());
        TxnView grandChildView = testEnv.getTxnStore().getTransaction(grandchild.getTxnId());
        Assert.assertEquals("Incorrect parent transaction for grandchild!", userView.getParentTxnView(), grandChildView.getParentTxnView());
    }

    @Test
    public void testCanReadDataWithACommittedTransaction() throws Exception {
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn svp0Txn = control.beginChildTransaction(userTxn, DESTINATION_TABLE);
        Txn svp1Txn = control.beginChildTransaction(svp0Txn, DESTINATION_TABLE);
        testUtility.insertAge(svp1Txn, "scott", 29);
        svp1Txn.commit();
        Txn selectTxn = control.beginChildTransaction(svp0Txn, null);
        svp0Txn.commit();
        Assert.assertEquals("Incorrect results", "scott age=29 job=null", testUtility.read(selectTxn, "scott"));
    }

    @Test
    public void testInsertThenDeleteWithinSameParentTransactionIsCorrect() throws Exception {
        /*
          * This tests the scenario as follows:
          *
          * 1. start user transaction
          * 2. insert data
          * 3. delete data
          * 4. Read data (no rows seen)
          *
          * With an attempt to duplicate the transactionally correct behavior of
          * the sql engine--that is, complete with proper child transactions etc.
          *
          * In this case, there should be no Write/Write conflict, and at the
          * end of the test no data is visible in the table(e.g. the delete succeeds).
          *
          * Note that, to preserve ACID properties and to obey the principle of least
          * surprise, the normal situation of writes is that writes always occur
          * within a child transaction (which may create its own child transactions
          * as need be); as a result, we use two levels of child transactions
          * to test the behavior
          */

        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);

        //insert the row
        transactionalInsert("scott", userTxn, 29);

        Assert.assertEquals("Incorrect results", "scott age=29 job=null", testUtility.read(userTxn, "scott"));

        transactionalDelete("scott", userTxn);

        Assert.assertEquals("Incorrect results","scott absent",testUtility.read(userTxn,"scott"));
    }

    @Test
    public void testInsertThenRollbackThenInsertNewRowThenScanWillOnlyReturnOneRow() throws Exception {
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);

        Txn child = control.beginChildTransaction(userTxn,DESTINATION_TABLE);

        Txn grandChild = control.beginChildTransaction(child,DESTINATION_TABLE);
        //insert the row
        transactionalInsert("scott10", grandChild, 29);

        Assert.assertEquals("Incorrect results", "scott10 age=29 job=null", testUtility.read(grandChild, "scott10"));

        //rollback
        grandChild.rollback();

        //now insert a second row with a new grandChild

        Txn grandChild2 = control.beginChildTransaction(child,DESTINATION_TABLE);
        transactionalInsert("scott11",grandChild2,29);

        grandChild2.commit();

        child.commit();

        String actual = testUtility.scanAll(userTxn, "scott10", "scott12", null).trim();
        String expected = "scott11 age=29 job=null[ V.age@~9=29 ] ".trim();
        Assert.assertEquals("Incorrect scan results", expected, actual);
    }

    @Test
    public void testInsertAndDeleteBeforeInsertTransactionCommitsThrowsWriteWriteConflict() throws Throwable {
        /*
         * The scenario is
         *
         * 1. start user transaction
         * 2. insert data in child transaction. Commit grandchild by not insert transaction
         * 3. attempt to delete data.
         * 4. Observe Write/Write conflict
         */

        String name = "scott3";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);

        Txn insertTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn insertChild = control.beginChildTransaction(insertTxn,DESTINATION_TABLE);
        testUtility.insertAge(insertChild,name,29);
        insertChild.commit();

        Txn deleteTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn deleteChild = control.beginChildTransaction(deleteTxn,DESTINATION_TABLE);
        try{
            testUtility.deleteRow(deleteChild,name);
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    @Test
    public void testInsertAndDeleteBeforeInsertChildTransactionCommitsThrowsWriteWriteConflict() throws Throwable {
        /*
         * The scenario is
         *
         * 1. start user transaction
         * 2. insert data in child transaction. Commit grandchild by not insert transaction
         * 3. attempt to delete data.
         * 4. Observe Write/Write conflict
         */

        String name = "scott3";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);

        Txn insertTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn insertChild = control.beginChildTransaction(insertTxn,DESTINATION_TABLE);
        testUtility.insertAge(insertChild,name,29);

        Txn deleteTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn deleteChild = control.beginChildTransaction(deleteTxn,DESTINATION_TABLE);
        try{
            testUtility.deleteRow(deleteChild,name);
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    @Test
    public void testInsertAndDeleteInterleavedCommitAndCreationStillThrowsWriteWriteConflict() throws Throwable {
        /*
         * The scenario is
         *
         * 1. start user transaction
         * 2. insert data in child transaction. Commit grandchild but not insert transaction
         * 3. create the delete txn
         * 4. commit the insert txn
         * 5. create the delete child txn
         * 6. attempt to delete the row
         * 4. Observe Write/Write conflict
         */

        String name = "scott4";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);

        Txn insertTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn insertChild = control.beginChildTransaction(insertTxn,DESTINATION_TABLE);
        testUtility.insertAge(insertChild,name,29);
        insertChild.commit();

        Txn deleteTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        insertTxn.commit();
        Txn deleteChild = control.beginChildTransaction(deleteTxn,DESTINATION_TABLE);
        try{
            testUtility.deleteRow(deleteChild, name);
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    /*
     * The following tests use the following write sequence:
     *
     * 1. start user transaction
     * 2. insert data
     * 3. commit user transaction (set up the data)
     * 4. start user transaction
     * 5. delete data
     * 6. insert data
     * 7. read data
     *
     * where the sequence of events of interest are focused within steps 4-7.
     */
    @Test
    public void testDeleteThenInsertWithinSameUserTransactionIsCorrect() throws Exception {
        String name = "scott2";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        transactionalInsert(name, userTxn, 29);
        userTxn.commit();

        userTxn = control.beginTransaction(DESTINATION_TABLE); //get a new transaction

        Assert.assertEquals("Incorrect results",name+" age=29 job=null",testUtility.read(userTxn, name));

        transactionalDelete(name, userTxn);

        transactionalInsert(name, userTxn, 28);

        Assert.assertEquals("Incorrect results",name+" age=28 job=null",testUtility.read(userTxn, name));
    }

    @Test
    public void testDeleteAndInsertBeforeDeleteTransactionCommitsThrowsWriteWriteConflict() throws Throwable {
        String name = "scott5";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        transactionalInsert(name, userTxn, 29);
        userTxn.commit();
        userTxn = control.beginTransaction(DESTINATION_TABLE);

        Txn deleteTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn deleteChild = control.beginChildTransaction(deleteTxn,DESTINATION_TABLE);
        testUtility.deleteRow(deleteChild, name);
        deleteChild.commit();

        Txn insertTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn insertChild = control.beginChildTransaction(insertTxn,DESTINATION_TABLE);
        try{
            testUtility.insertAge(insertChild, name, 29);
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    @Test
    public void testDeleteAndInsertInterleavedCommitAndCreatedThrowsWriteWriteConflict() throws Throwable {
        String name = "scott6";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        transactionalInsert(name, userTxn, 29);
        userTxn.commit();
        userTxn = control.beginTransaction(DESTINATION_TABLE);

        Txn deleteTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn deleteChild = control.beginChildTransaction(deleteTxn,DESTINATION_TABLE);
        testUtility.deleteRow(deleteChild, name);
        deleteChild.commit();

        Txn insertTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        deleteTxn.commit();
        Txn insertChild = control.beginChildTransaction(insertTxn,DESTINATION_TABLE);
        try{
            testUtility.insertAge(insertChild,name,29);
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    @Test
    public void testDeleteAndInsertBeforeDeleteChildTransactionCommitsThrowsWriteWriteConflict() throws Throwable {
        String name = "scott6";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        transactionalInsert(name, userTxn, 29);
        userTxn.commit();
        userTxn = control.beginTransaction(DESTINATION_TABLE);

        Txn deleteTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn deleteChild = control.beginChildTransaction(deleteTxn,DESTINATION_TABLE);
        testUtility.deleteRow(deleteChild, name);

        Txn insertTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn insertChild = control.beginChildTransaction(insertTxn,DESTINATION_TABLE);
        try{
            testUtility.insertAge(insertChild,name,29);
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    /*
     * The following test the "additive" features.
     *
     * Additivity between transactions loosely means that multiple child transactions are
     * expected to (potentially) trample on each other's writes, but that doesn't imply a Write-Write conflict.
     * The most obvious example of this is a bulk insert operation on to a table with a primary key constraint.
     *
     * The canonical example of this is a bulk import operation into a table with a primary key constraint.
     * In that scenario, we have two separate files being imported in parallel, each under their own child transactions,
     * and each file has an entry for a specific row. What we *WANT* to happen is that the system throws
     * a PrimaryKey constraint violation. However, without additivity, what we would GET is a Write/Write conflict.
     *
     * Thus, we want a transactional mode that will not throw the Write/Write conflict, so that instead we can
     * throw the appropriate constraint violation.
     *
     * Typically, you only want to use this feature with Inserts and Deletes--Updates could result in highly
     * non-deterministic results.
     *
     * Now, to the mathematical structure of Additive transactions:
     *
     * Two transactions T1 and T2 are considered additive if and only if the following criteria hold:
     *
     * 1. T1 has been marked as additive
     * 2. T2 has been marked as additive
     * 3. T1.parent = T2.parent (e.g. they are direct relatives)
     *
     *
     * Write characteristics of an Additive Transaction:
     *
     * For writes, we have the following cases:
     *
     * 1. T1 additive:
     *  A. T2 additive:
     *      1. T1.parent == T2.parent => NO WWConflict  (only time this is true)
     *      2. T1.parent != T2.parent => WWConflict
     *  B. T2 not additive => WWConflict
     * 2. T2 additive, T1 not additive => WWConflict
     *
     * Read characteristics of an additive transaction:
     *
     * In general, we would *like* to use normal visibility semantics with additive transactions.
     * However, that is not a possibility, because of the following case:
     *
     * Suppose you are attempting the following sql:
     *
     * insert into t select * from t;
     *
     * With this query, we will construct 1 transaction per region; this means that it will be possible
     * for the following sequence to occur:
     *
     * 1. Child 1 writes row R
     * 2. Child 1 commits
     * 3. Child 2 begins
     * 4. Child 2 reads row R
     *
     * Because Child 1 and Child 2 are at the same level, the normal process would treat them as if they
     * were two individual operations. In this case, since Child 1 has committed before Child 2 began, Snapshot
     * Isolation semantics would allow that child to see writes. In practice, this leads to inconsistent iteration.
     *
     * We want to make it so that Child2 does not see the writes from Child1, even though they are independent
     * from one another. We realize that insertions are "additive", so we discover the second major feature
     * of additive child transactions: writes from one additive child transaction are NOT VISIBLE to
     * any other additive child transaction (as long as they are additive with respect to one another).
     *
     */
    @Test
    public void testTwoAdditiveTransactionsDoNotConflict() throws Exception {
        String name = "scott7";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(userTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);

        testUtility.insertAge(child1,name,29);
        testUtility.insertJob(child2,name,"plumber");
        child1.commit();
        child2.commit();

        //parent txn can now operate
        Assert.assertEquals("Incorrectly written data!",name+" age=29 job=plumber",testUtility.read(userTxn,name));
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
    public void testTwoAdditiveTransactionsCannotSeeEachOthersWritesEvenAfterCommit() throws Exception {
        /*
         * The purpose of this test is to ensure consistent iteration between two additive transactions.
         *
         * Imagine the following scenario:
         *
         * You have two regions, R1 and R2 with a primary key on column a. You issue "update foo set a = newA". In
         * this case, the update deletes the row at location a and inserts a new record at location newA. So imagine
         * that a is in R1, and newA is in R2; further imagine that the scanner for R2 is behind that of R1. If
         * the additive transaction managing the scan on R2 could see the writes of R1, then R2 would see
         * the entry for newA, and update it to something else, which would result in incorrect results.
         *
         * Therefore, we need to ensure that two additive transactions are NEVER able to see one another, to
         * ensure that we have consistent iteration. This has negative consequences (like detecting write conflicts
         * during writes and so forth), but is necessary.
         */
        String name = "scott10";
        Txn userTxn = noSubTxns(control.beginTransaction(DESTINATION_TABLE));
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(userTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,null);

        testUtility.insertAge(child1,name,29);
        child1.commit();
        Assert.assertEquals("Additive transaction cannot see sibling's writes",name+" absent",testUtility.read(child2,name));
    }

    @Test
    public void testOnlyOneAdditiveTransactionConflicts() throws Throwable {
        String name = "scott9";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(userTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,false,DESTINATION_TABLE);

        testUtility.insertAge(child1,name,29);
        try{
            testUtility.insertJob(child2,name,"plumber");
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    @Test
    public void testTwoAdditiveTransactionsWithDifferentParentsConflicts() throws Throwable {
        String name = "scott12";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn u2 = control.beginTransaction(DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(u2,Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);

        testUtility.insertAge(child1,name,29);
        try{
            testUtility.insertJob(child2,name,"plumber");
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    @Test
    public void testAdditiveGrandchildConflictsWithAdditiveChild() throws Throwable {
        String name = "scott8";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(userTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,false,DESTINATION_TABLE);
        Txn grandChild = control.beginChildTransaction(child2,Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);

        testUtility.insertAge(child1,name,29);
        try{
            testUtility.insertJob(grandChild,name,"plumber");
        }catch(IOException re){
            testUtility.assertWriteConflict(re);
        }
    }

    /**
     * Transactional structure to test:
     * 
     * User Txn
     *   CALL Txn
     *     INSERT Txn
     *     SELECT Txn
     *
     * @throws Exception
     */
    @Test
    public void testInsertThenScanWithinSameParentTransactionIsCorrect() throws Exception {
    	Txn userTxn = control.beginTransaction(DESTINATION_TABLE);

    	Txn callTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE); //create the CallStatement Txn
    	Txn insertTxn = control.beginChildTransaction(callTxn,DESTINATION_TABLE); //create the insert txn
    	testUtility.insertAge(insertTxn,"scott",29); //insert the row
    	insertTxn.commit();

    	Txn selectTxn = control.beginChildTransaction(callTxn,null); //create the select savepoint
    	Assert.assertEquals("Incorrect results", "scott age=29 job=null",testUtility.read(selectTxn, "scott")); //validate it's visible

    	callTxn.rollback();
    }

    /**
     * Transactional structure to test:
     * 
     * User Txn
     *   CALL, SELECT Txn
     *     INSERT Txn
     *
     * @throws Exception
     */
    @Test
    public void testInsertWithChildTransactionThenScanWithParentTransactionIsCorrect() throws Exception {
    	Txn userTxn = control.beginTransaction(DESTINATION_TABLE);

    	Txn callTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE); //create the CallStatement Txn
    	Txn insertTxn = control.beginChildTransaction(callTxn,DESTINATION_TABLE); //create the insert txn
    	testUtility.insertAge(insertTxn,"scott",29); //insert the row
    	insertTxn.commit();

    	Assert.assertEquals("Incorrect results", "scott age=29 job=null",testUtility.read(callTxn, "scott")); //validate it's visible to the parent

    	callTxn.rollback();
    }

    /**
     * This is testing what has been observed happening within stored procedures (callable statements) in Splice/Derby
     * that INSERT a row and then SELECT the row.  The SELECT statement was being wrapped with a SAVEPOINT that was
     * inherited from the CALL statement since the CALL statement was attached to the UserTransaction by Splice, and Derby
     * attempted to wrap the CALL statement with a SAVEPOINT also.  This caused an extra transaction (savepoint) to be
     * created which was then associated with the SELECT statement.  The savepoint around the SELECT was released which
     * was causing the ResultSet to fail scanning the new row since the state of the transaction (savepoint) was COMMITTED.
     * Confusing, eh?
     *
     * Transactional structure to test:
     * 
     * ROOT Txn
     *   CALL Txn (UserTransaction)
     *     SELECT Txn (SAVEPT0)
     *       INSERT Txn (SAVEPT1)
     *
     * @throws Exception
     */
    @Test
    public void testInsertWithGrandchildTransactionThenScanWithParentTransactionIsCorrect() throws Exception {
    	Txn rootTxn = control.beginTransaction(DESTINATION_TABLE);

    	Txn callTxn = control.beginChildTransaction(rootTxn,DESTINATION_TABLE); //create the CallStatement Txn
    	Txn selectTxn = control.beginChildTransaction(callTxn,DESTINATION_TABLE); //create the select savepoint
    	Txn insertTxn = control.beginChildTransaction(selectTxn,DESTINATION_TABLE); //create the insert txn
    	testUtility.insertAge(insertTxn,"scott",29); //insert the row
    	insertTxn.commit();
    	selectTxn.commit();

    	Assert.assertEquals("Incorrect results", "scott age=29 job=null",testUtility.read(selectTxn, "scott")); //validate it's visible

    	callTxn.rollback();
    }

    @Test
    public void rollbackDeleteThenUpdateIsCorrect() throws Exception{
        /*
         * Regression test for DB-4324. Turns out, if you delete a row, then roll it back, then
         * perform an update you screw up the row. That's bad news.
         *
         * The problem was that the transactor was using "conflicts()" to determine if it should place
         * an anti-tombstone or not, rather than "canSee()". Switching the two resolves the issue, and
         * this test proves the fix.
         */
        Txn insert = control.beginTransaction(DESTINATION_TABLE);

        testUtility.insertAge(insert,"scott",29);
        Assert.assertEquals("Incorrect results","scott age=29 job=null",testUtility.read(insert,"scott"));
        insert.commit();

        Txn delete = control.beginTransaction(DESTINATION_TABLE);
        testUtility.deleteRow(delete,"scott");
        delete.rollback();

        Txn update = control.beginTransaction(DESTINATION_TABLE);
        testUtility.insertJob(update,"scott","baker");

        Assert.assertEquals("Incorrect results","scott age=29 job=baker",testUtility.read(update,"scott"));
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void transactionalDelete(String name, Txn userTxn) throws IOException {
        Txn deleteTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn deleteChild = control.beginChildTransaction(deleteTxn,DESTINATION_TABLE);
        testUtility.deleteRow(deleteChild, name);
        deleteChild.commit();
        deleteTxn.commit();
    }

    private void transactionalInsert(String name, Txn userTxn, int age) throws IOException {
        //insert the row
        Txn insertTxn = control.beginChildTransaction(userTxn,DESTINATION_TABLE);
        Txn insertChild = control.beginChildTransaction(insertTxn,DESTINATION_TABLE);
        testUtility.insertAge(insertChild, name, age);
        insertChild.commit(); //make the data visible to the insert parent
        insertTxn.commit();
    }
}
