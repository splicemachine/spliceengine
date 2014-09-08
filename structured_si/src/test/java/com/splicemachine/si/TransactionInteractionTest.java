package com.splicemachine.si;

import com.google.common.collect.Lists;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.impl.WriteConflict;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

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
public class TransactionInteractionTest {
    public static final byte[] DESTINATION_TABLE = Bytes.toBytes("1184");
    boolean useSimple = true;
    static StoreSetup storeSetup;
    static TestTransactionSetup transactorSetup;
    Transactor transactor;
    TxnLifecycleManager control;
    TransactorTestUtility testUtility;
    final List<Txn> createdParentTxns = Lists.newArrayList();

    @SuppressWarnings("unchecked")
    void baseSetUp() {
        transactor = transactorSetup.transactor;
        control = new ForwardingLifecycleManager(transactorSetup.txnLifecycleManager){
            @Override
            protected void afterStart(Txn txn) {
                createdParentTxns.add(txn);
            }
        };
        testUtility = new TransactorTestUtility(useSimple,storeSetup,transactorSetup,transactor,control);
    }

    @BeforeClass
    public static void setupClass(){
        storeSetup = new LStoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, true);
    }
    @Before
    public void setUp() throws IOException {
        baseSetUp();
    }

    @After
    public void tearDown() throws Exception {
        for(Txn id:createdParentTxns){
            id.rollback();
        }
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

    @Test(expected = WriteConflict.class)
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
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
        }
    }

    @Test(expected = WriteConflict.class)
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
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
        }
    }

    @Test(expected = WriteConflict.class)
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
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
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

    @Test(expected = WriteConflict.class)
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
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
        }
    }

    @Test(expected = WriteConflict.class)
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
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
        }
    }

    @Test(expected = WriteConflict.class)
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
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
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
     * Thus, the following tests a few situations:
     *
     * 1. T1 additive:
     *  A. T2 additive:
     *      1. T1.parent == T2.parent => NO WWConflict  (only time this is true)
     *      2. T1.parent != T2.parent => WWConflict
     *  B. T2 not additive => WWConflict
     * 2. T2 additive, T1 not additive => WWConflict
     *
     * When these cases hold, we also have an additional feature: that reads between two additive transactions
     * (assuming the above rules) operate with a READ_UNCOMMITTED isolation level, even if they are both
     * at a normally higher level.
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

    @Test
    public void testTwoAdditiveTransactionsUseReadUncommittedIsolationLevel() throws Exception {
        String name = "scott10";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(userTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,null);

        testUtility.insertAge(child1,name,29);
        Assert.assertEquals("Additive transaction cannot see sibling's writes",name+" age=29 job=null",testUtility.read(child2,name));
    }

    @Test(expected=WriteConflict.class)
    public void testOnlyOneAdditiveTransactionConflicts() throws Throwable {
        String name = "scott9";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(userTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,false,DESTINATION_TABLE);

        testUtility.insertAge(child1,name,29);
        try{
            testUtility.insertJob(child2,name,"plumber");
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
        }
    }

    @Test(expected=WriteConflict.class)
    public void testTwoAdditiveTransactionsWithDifferentParentsConflicts() throws Throwable {
        String name = "scott12";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn u2 = control.beginTransaction(DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(u2,Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);

        testUtility.insertAge(child1,name,29);
        try{
            testUtility.insertJob(child2,name,"plumber");
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
        }
    }

    @Test(expected=WriteConflict.class)
    public void testAdditiveGrandchildConflictsWithAdditiveChild() throws Throwable {
        String name = "scott8";
        Txn userTxn = control.beginTransaction(DESTINATION_TABLE);
        Txn child1 = control.beginChildTransaction(userTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);
        Txn child2 = control.beginChildTransaction(userTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,false,DESTINATION_TABLE);
        Txn grandChild = control.beginChildTransaction(child2,Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,DESTINATION_TABLE);

        testUtility.insertAge(child1,name,29);
        try{
            testUtility.insertJob(grandChild,name,"plumber");
        }catch(RetriesExhaustedWithDetailsException re){
            throw re.getCauses().get(0);
        }
    }

    /************************************************************************************************************/
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
