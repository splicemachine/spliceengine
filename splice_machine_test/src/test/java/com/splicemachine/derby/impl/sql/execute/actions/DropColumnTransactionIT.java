package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.test.SerialTest;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Tests around dropping a column (transactionally and otherwise).
 *
 * @author Scott Fines
 * Date: 9/3/14
 */
public class DropColumnTransactionIT {
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(DropColumnTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher commitTable = new SpliceTableWatcher("B",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher beforeTable = new SpliceTableWatcher("C",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher afterTable = new SpliceTableWatcher("D",schemaWatcher.schemaName,"(a int, b int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    public static final String query = "select * from " + table+" where a = ";
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table)
            .around(commitTable)
            .around(beforeTable)
            .around(afterTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = classWatcher.prepareStatement("insert into "+ table+" values (?,?)");
                        ps.setInt(1,1);
                        ps.setInt(2,1);
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    try {
                        PreparedStatement ps = classWatcher.prepareStatement("insert into "+ commitTable+" values (?,?)");
                        ps.setInt(1,1);
                        ps.setInt(2,1);
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

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

    @Test
    public void testDropColumnWorksWithOneConnection() throws Exception {
        conn1.createStatement().execute("alter table "+commitTable+" drop column b");
        conn1.commit();

        ResultSet rs = conn1.query("select * from "+ commitTable);
        Assert.assertEquals("Metadata returning incorrect column count!", 1, rs.getMetaData().getColumnCount());

        try{
            conn1.query("select b from "+ commitTable);
            Assert.fail("Was able to find column b");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message returned:"+se.getMessage(), ErrorState.LANG_COLUMN_NOT_FOUND.getSqlState(),se.getSQLState());
        }
    }

    @Test
    public void testDropColumnWorksWithinSingleTransaction() throws Exception {
        conn1.createStatement().execute("alter table "+ table+" drop column b");

        ResultSet rs = conn1.query("select * from "+ table);
        Assert.assertEquals("Metadata returning incorrect column count!", 1, rs.getMetaData().getColumnCount());

        try{
            conn1.query("select b from "+ table);
            Assert.fail("Was able to find column b");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message returned:"+se.getMessage(), ErrorState.LANG_COLUMN_NOT_FOUND.getSqlState(),se.getSQLState());
        }
    }

    @Test
    public void testDropColumnIsNotVisibleToOtherTransaction() throws Exception {
        testDropColumnWorksWithinSingleTransaction(); //dropped in conn1, but not in conn2

        ResultSet rs = conn2.query("select * from "+ table);
        Assert.assertEquals("Metadata returning incorrect column count!", 2, rs.getMetaData().getColumnCount());

        rs = conn2.query("select b from "+ table);
        Assert.assertEquals("Metadata for b-only query returning incorrect column count!", 1, rs.getMetaData().getColumnCount());

        //insert some data with a b field, and make sure that it's there
        int aInt = 2;
        int bInt = 2;
        PreparedStatement preparedStatement = conn2.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        preparedStatement.execute();

        long count = conn2.count("select b from "+ table);
        Assert.assertEquals("incorrect row count!",2,count);
    }

    @Test
    public void testDropColumnAfterInsertionWorks() throws Exception {
         /*
         * This is a test to ensure that the following sequence holds:
         *
         * 0. let transaction A be the transaction with the lowest begin timestamp, and B be the other transaction
         * 1. (with txn B) insert into <table> (a,b) values (..); commit;
         * 2. (with txn A) alter table <table> drop column b; commit;
         * 3. (with either txn) select * from <table> --SPLICE-PROPERTIES index=T_IDX
         * where a = 1;
         * 4. Ensure that the data is picked up and correct
         */
        TestConnection a;
        TestConnection b;
        if(conn1Txn>conn2Txn){
            a = conn2;
            b = conn1;
        }else{
            a = conn1;
            b = conn2;
        }

        int aInt = 3;
        int bInt = 3;
        b.createStatement().execute("insert into "+ afterTable+" (a,b) values ("+aInt+","+bInt+")");
        b.commit();

        a.createStatement().execute("alter table "+afterTable+" drop column b");
        a.commit();

        long count = conn1.count("select * from "+ afterTable+ " where a="+aInt);
        Assert.assertEquals("Data was not picked up!",1,count);
    }

    @Test
    public void testDropColumnBeforeInsertionWorks() throws Exception {
        /*
         * This is a test to ensure that the following sequence holds:
         *
         * 0. let transaction A be the transaction with the lowest begin timestamp, and B be the other transaction
         * 1. (with txn A) alter table <table> drop column b;
         * 2. (with txn B) insert into <table> (a,b) values (..); commit;
         * 3. (with txn A) commit;
         * 4. (with either txn) select * from <table> --SPLICE-PROPERTIES index=T_IDX
         * where a = <a>;
         * 5. Ensure that the data is picked up
         */
        TestConnection a;
        TestConnection b;
        if(conn1Txn>conn2Txn){
            a = conn2;
            b = conn1;
        }else{
            a = conn1;
            b = conn2;
        }

        a.createStatement().execute("alter table " + beforeTable + " drop column b");

        int aInt = 3;
        int bInt = 3;
        b.createStatement().execute("insert into "+ beforeTable+" (a,b) values ("+aInt+","+bInt+")");
        b.commit();
        a.commit();

        long count = conn1.count("select * from "+ beforeTable+ " where a="+aInt);
        Assert.assertEquals("Data was not picked up!",1,count);
    }

    @Test
    public void testDropColumnFromtwoTransactionsThrowsWriteConflict() throws Exception {
        conn1.createStatement().execute("alter table "+ table+" drop column b");
        try{
            conn2.createStatement().execute("alter table " + table+" drop column b");
            Assert.fail("No write conflict detected!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error type: "+ se.getMessage(),ErrorState.WRITE_WRITE_CONFLICT.getSqlState(),se.getSQLState());
        }
    }
}
