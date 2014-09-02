package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.derby.utils.ErrorState;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 * Date: 8/27/14
 */
public class IndexTransactionIT {

    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(IndexTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int,c int, primary key (a))");

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
    public void testCannotCreateIfActiveWritesOutstanding() throws Exception {
        int a = 1;
        int b = 1;
        int c = 1;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b,c) values(?,?,?)");
        preparedStatement.setInt(1,a);
        preparedStatement.setInt(2,b);
        preparedStatement.setInt(3,c);

        preparedStatement.executeUpdate();

        preparedStatement = conn2.prepareStatement("create index c_idx on " + table + "(c)");
        try{
            preparedStatement.execute();
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message!", ErrorState.DDL_ACTIVE_TRANSACTIONS.getSqlState(),se.getSQLState());
            String errorMessage = se.getMessage();
            Assert.assertTrue("Error message does not refer to table!",errorMessage.contains("C_IDX")||errorMessage.contains("C_idx"));
            throw se;
        }
    }

    @Test
    public void testInsertionAfterPopulatePhaseIsPickedUp() throws Exception{
        /*
         * This is a test to ensure that the following sequence holds:
         *
         * 0. let transaction A be the transaction with the lowest begin timestamp, and B be the other transaction
         * 1. (with txn A) create index t_idx on <table> (a);
         * 2. (with txn B) insert into <table> (a,b) values (..); commit;
         * 3. (with txn A) commit;
         * 4. (with either txn) select * from <table> --SPLICE-PROPERTIES index=T_IDX
         * where a = 1;
         * 5. Ensure that the data is picked up
         */
        TestConnection a;
        TestConnection b;
        if(conn1Txn<conn2Txn){
            a = conn1;
            b = conn2;
        }else{
            a = conn2;
            b = conn1;
        }

        PreparedStatement preparedStatement = a.prepareStatement("create index a_idx on "+table+"(a)");
        preparedStatement.execute();
        int aInt = 3;
        int bInt = 3;
        int cInt = 3;

        preparedStatement = b.prepareStatement("insert into " + table + " (a,b,c) values (?,?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        preparedStatement.setInt(3,cInt);
        preparedStatement.execute();
        b.commit(); //need to force commit to make sure that the data is visible

        a.commit(); //force to a new timestamp for visibility check

        long count = a.count("select * from " + table + " --SPLICE-PROPERTIES index=A_IDX \n" +
                "where a = " + aInt);

        Assert.assertEquals("Index is corrupt!",1,count);
    }

    @Test
    public void testInsertionBeforeCreatePhaseIsPickedUp() throws Exception{
        /*
         * This is a test to ensure that the following sequence holds:
         *
         * 0. let transaction A be the transaction with the lowest begin timestamp, and B be the other transaction
         * 1. (with txn B) insert into <table> (a,b) values (..); commit;
         * 2. (with txn A) create index t_idx on <table> (a); commit;
         * 3. (with either txn) select * from <table> --SPLICE-PROPERTIES index=T_IDX
         * where a = 1;
         * 4. Ensure that the data is picked up
         */
        TestConnection a;
        TestConnection b;
        if(conn1Txn<conn2Txn){
            a = conn1;
            b = conn2;
        }else{
            a = conn2;
            b = conn1;
        }

        int aInt = 2;
        int bInt = 2;
        int cInt = 2;
        PreparedStatement preparedStatement = b.prepareStatement("insert into " + table + " (a,b,c) values (?,?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        preparedStatement.setInt(3,cInt);
        preparedStatement.execute();
        b.commit(); //need to force commit to make sure that the data is visible

        preparedStatement = a.prepareStatement("create index b_idx on "+table+"(b)");
        preparedStatement.execute();
        a.commit(); //force to a new timestamp for visibility check

        long count = a.count("select * from " + table + " --SPLICE-PROPERTIES index=B_IDX \n" +
                "where b = " + bInt);

        Assert.assertEquals("Index is corrupt!",1,count);
    }

    @Test
    public void testDropIsIgnoredByOtherTransaction() throws Exception {
        int aInt = 4;
        int bInt = 4;
        int cInt = 4;

        PreparedStatement preparedStatement = conn1.prepareStatement("create index ab_idx on "+table+"(a,b)");
        preparedStatement.execute();
        conn1.commit(); //force to a new timestamp for visibility check
        conn2.rollback(); //move other transaction forward so that index is visible

        String query = "select * from " + table + " --SPLICE-PROPERTIES index=AB_IDX \n" +
                "where a = " + aInt + " and b = " + bInt;
        long count = conn2.count(query);
        Assert.assertEquals("conn2 has incorrect index count!",0l,count);

        /*
         * Now, the real test:
         *
         * 1. drop index in one transaction
         * 2. in other transaction, insert some data, then ensure that it's readable from that transaction.
         * 3. commit the drop transaction
         * 4. commit the insert transaction
         * 5. check that the data is not visible any longer
         */
        conn2.createStatement().execute("drop index "+schemaWatcher.schemaName+".ab_idx");
        try{
            conn2.count(query);
            Assert.fail("Should have thrown an IndexNotFoundException");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message returned!",ErrorState.LANG_INVALID_FORCED_INDEX1.getSqlState(),se.getSQLState());
        }

        //insert some data with the other transaction
        preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b,c) values (?,?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        preparedStatement.setInt(3, cInt);
        preparedStatement.execute();

        count = conn1.count(query);
        Assert.assertEquals("conn1 has incorrect index count!",1l,count);

        //commit the drop transaction
        conn2.commit();

        //move the other txn forward for visibility
        conn1.rollback();

        try{
            conn1.count(query);
            Assert.fail("Should have thrown an IndexNotFoundException");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message returned!",ErrorState.LANG_INVALID_FORCED_INDEX1.getSqlState(),se.getSQLState());
        }
    }
}
