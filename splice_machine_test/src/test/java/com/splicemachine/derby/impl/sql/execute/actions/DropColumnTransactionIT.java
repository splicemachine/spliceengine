package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.test.SerialTest;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
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
    public static final SpliceTableWatcher afterTable2 = new SpliceTableWatcher("E",schemaWatcher.schemaName,"(a int, b int, c int)");
    public static final SpliceTableWatcher aTable = new SpliceTableWatcher("F",schemaWatcher.schemaName,"(a int, b int, c int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    public static final String query = "select * from " + table+" where a = ";
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table)
            .around(commitTable)
            .around(beforeTable)
            .around(afterTable)
            .around(afterTable2)
            .around(aTable)
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
        conn1.createStatement().execute("alter table " + commitTable + " drop column b");
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
        conn1.createStatement().execute("alter table " + table + " drop column b");

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

        long count = conn2.count("select b from " + table);
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

        a.createStatement().execute("alter table " + afterTable + " drop column b");
        a.commit();

        long count = conn1.count("select * from "+ afterTable+ " where a="+aInt);
        Assert.assertEquals("Data was not picked up!",1,count);
    }

    @Test
    public void testDropColumnAfterAddColumnWorks() throws Exception {
         /*
         * This is a test to ensure that the following sequence holds:
         *
         * 0. let transaction A be the transaction with the lowest begin timestamp, and B be the other transaction
         * 1. (with txn B) insert into <table> (a,b) values (..); commit;
         * 2. (with txn A) alter table <table> drop column b; commit;
         * 3. (with new txn) select * from <table>
         * 4. Ensure that there are no nulls
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

        int aInt = 1;
        int bInt = 2;
        int cInt = 3;
        b.createStatement().execute("insert into " + afterTable2 + " (a,b,c) values (" + aInt + "," + bInt + "," +
                                        cInt + ")");
        b.commit();
        b.createStatement().execute("alter table " + afterTable2 + " add column d decimal(2,1) not null default 2.0");
        b.commit();
        b.createStatement().execute("alter table " + afterTable2 + " add column e decimal(2,1) not null default 3.0");
        b.commit();

        a.commit();  // a commits here so it can see changes b has made since a started
        a.createStatement().execute("alter table " + afterTable2 + " drop column b");
        a.commit();

        long count = classWatcher.createConnection().count("select * from " + afterTable2 + " where a=" + aInt);
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
        b.createStatement().execute("insert into " + beforeTable + " (a,b) values (" + aInt + "," + bInt + ")");
        b.commit();
        a.commit();

        long count = conn1.count("select * from " + beforeTable + " where a=" + aInt);
        Assert.assertEquals("Data was not picked up!",1,count);
    }

    @Test
    public void testDropColumnFromtwoTransactionsThrowsActiveTransactions() throws Exception {
        conn1.createStatement().execute("alter table " + table + " drop column b");
        try{
            conn2.createStatement().execute("alter table " + table+" drop column b");
            Assert.fail("No write conflict detected!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error type: "+ se.getMessage(),
                                ErrorState.DDL_ACTIVE_TRANSACTIONS.getSqlState(),se.getSQLState());
        }
    }

    @Test
    public void testDropMiddleColumn() throws Exception {
        int aInt = 1;
        int bInt = 2;
        int cInt = 3;
        BigDecimal dDec = BigDecimal.valueOf(4.0);
        BigDecimal eDec = BigDecimal.valueOf(5.0);

        conn1.createStatement().execute(
            String.format("insert into %s (a,b,c) values (%s,%s,%s)", aTable, aInt, bInt, cInt));
        conn1.createStatement().execute(
            String.format("alter table %s add column d decimal(2,1) not null default %s", aTable, dDec));
        conn1.createStatement().execute(
            String.format("alter table %s add column e decimal(2,1) not null default %s", aTable, eDec));

        conn1.createStatement().execute(
            String.format("alter table %s drop column b", aTable));

        ResultSet rs = conn1.query("select * from "+ aTable);
        int count=0;
        while(rs.next()){
            int a = rs.getInt("A");
            Assert.assertFalse("Got a null value for A!",rs.wasNull());
            Assert.assertEquals("Incorrect value for A!", aInt, a);

            int c = rs.getInt("C");
            Assert.assertFalse("Got a null value for C!", rs.wasNull());
            Assert.assertEquals("Incorrect value for C!",cInt,c);

            BigDecimal d = rs.getBigDecimal("D");
            Assert.assertFalse("Got a null value for D!",rs.wasNull());
            Assert.assertEquals("Incorrect value for D!",dDec,d);

            BigDecimal e = rs.getBigDecimal("E");
            Assert.assertFalse("Got a null value for E!",rs.wasNull());
            Assert.assertEquals("Incorrect value for E!", eDec, e);

            count++;
        }
        Assert.assertEquals("Incorrect returned row count", 1, count);

    }
}
