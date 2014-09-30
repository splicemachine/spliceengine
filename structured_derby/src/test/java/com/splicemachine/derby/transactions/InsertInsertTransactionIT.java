package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.derby.utils.ErrorState;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Tests around two connections performing insertions concurrently.
 *
 * This class is intended to test that we properly handle insertion
 * operations correctly.
 *
 * @author Scott Fines
 * Date: 8/25/14
 */
public class InsertInsertTransactionIT {

    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(InsertInsertTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int, primary key (a))");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table);

    private static TestConnection conn1;
    private static TestConnection conn2;

    private long conn1Txn;
    private long conn2Txn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn1 = classWatcher.getOrCreateConnection();
        conn2 = classWatcher.createConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        conn1.close();
        conn2.close();
    }

    @After
    public void tearDown() throws Exception {
        conn1.rollback();
        conn1.reset();
        conn2.rollback();
        conn2.reset();
    }

    @Before
    public void setUp() throws Exception {
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
        conn1Txn = conn1.getCurrentTransactionId();
        conn2Txn = conn2.getCurrentTransactionId();
    }

    @Test(expected = SQLException.class)
    public void testWritesDoNotConflictWithinSameTransaction() throws Exception {
        //should get a UniqueConstraint, NOT a Write/Write conflict here
        int a = 4;
        int b = 4;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        preparedStatement.setInt(1,a);
        preparedStatement.setInt(2,b);

        preparedStatement.execute();

        long conn1Count = conn1.count("select * from "+ table+" where a = "+a);
        Assert.assertEquals("Rows are not visible to my own transaction!",1l,conn1Count);

        try{
            preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
            preparedStatement.setInt(1,a);
            preparedStatement.setInt(2,b);

            preparedStatement.execute();
        }catch(SQLException se){
            Assert.assertEquals("Incorrect exception thrown!", ErrorState.LANG_DUPLICATE_KEY_CONSTRAINT.getSqlState(),se.getSQLState());
            throw se;
        }
    }

    @Test
    public void testCommitsBeforeVisibleWithSnapshotIsolation() throws Exception {
        int a = 3;
        int b = 3;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        preparedStatement.setInt(1,a);
        preparedStatement.setInt(2,b);

        preparedStatement.execute();

        long conn1Count = conn1.count("select * from "+ table+" where a = "+a);
        Assert.assertEquals("Rows are not visible to my own transaction!",1l,conn1Count);
        long conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);
        //rows won't be visible until conn1 commits AND conn2 commits
        conn1.commit();
        conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);
        conn2.commit();
        conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 1, conn2Count);
    }

    @Test
    public void testRollbacksNeverVisibleSnapshotIsolation() throws Exception {
        int a = 1;
        int b = 1;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        preparedStatement.setInt(1,a);
        preparedStatement.setInt(2, b);

        preparedStatement.execute();

        long conn1Count = conn1.count("select * from "+ table+" where a = "+a);
        Assert.assertEquals("Rows are not visible to my own transaction!",1l,conn1Count);
        long conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);
        //rows won't be visible until conn1 commits AND conn2 commits
        conn1.rollback();
        conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);
        conn2.commit();
        conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);
    }

    @Test(expected = SQLException.class)
    public void testTwoWritesThrowWriteWriteConflictSnapshotIsolation() throws Exception {
        int a = 2;
        int b = 2;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        preparedStatement.setInt(1,a);
        preparedStatement.setInt(2,b);

        preparedStatement.execute();

        long conn1Count = conn1.count("select * from "+ table+" where a = "+a);
        Assert.assertEquals("Rows are not visible to my own transaction!",1l,conn1Count);
        long conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);

        try{
            preparedStatement = conn2.prepareStatement("insert into " + table + " (a,b) values (?,?)");
            preparedStatement.setInt(1,a);
            preparedStatement.setInt(2,b);

            preparedStatement.execute();
        }catch(SQLException se){
            Assert.assertTrue("Incorrect exception thrown! Error state=" + se.getSQLState(), se.getSQLState().equals("SE014"));
            throw se;
        }
    }

    @Test
    public void testFailedInsertWillNotBeVisibleEvenInSameTransaction() throws Exception {
        int a = 5;
        int b = 5;
        PreparedStatement ps = conn1.prepareStatement("insert into "+ table + " (a,b) values (?,?),(?,?)");
        ps.setInt(1,a);
        ps.setInt(2,b);
        ps.setInt(3,a);
        ps.setInt(4,b);

        try{
            ps.execute();
            Assert.fail("Unique exception not thrown!");
        }catch(SQLException se){
            System.out.printf("%s:%s",se.getSQLState(),se.getMessage());
            Assert.assertEquals("Incorrect error message",ErrorState.LANG_DUPLICATE_KEY_CONSTRAINT.getSqlState(),se.getSQLState());
        }

        long count = conn1.count("select * from "+ table+" where a = "+ 5);
        Assert.assertEquals("Data is visible!",0,count);
    }
}
