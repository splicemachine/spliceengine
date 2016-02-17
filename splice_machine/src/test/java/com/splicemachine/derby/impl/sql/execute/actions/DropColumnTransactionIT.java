package com.splicemachine.derby.impl.sql.execute.actions;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.test.Transactions;

/**
 * Tests around dropping a column (transactionally and otherwise).
 *
 * @author Scott Fines
 * Date: 9/3/14
 */
@Category({Transactions.class})
public class DropColumnTransactionIT {
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(DropColumnTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher commitTable = new SpliceTableWatcher("B",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher beforeTable = new SpliceTableWatcher("C",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher afterTable = new SpliceTableWatcher("D",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher afterTable2 = new SpliceTableWatcher("E",schemaWatcher.schemaName,"(a int, b int, c int)");
    public static final SpliceTableWatcher aTable = new SpliceTableWatcher("F",schemaWatcher.schemaName,"(a int, b int, c int)");
    public static final SpliceTableWatcher table2 = new SpliceTableWatcher("G",schemaWatcher.schemaName,"(a int, b int)");

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
            .around(table2)
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
                        PreparedStatement ps = classWatcher.prepareStatement("insert into "+ table2+" values (?,?)");
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

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

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
            Assert.assertEquals("Incorrect error message returned:"+se.getMessage(),
                                ErrorState.LANG_COLUMN_NOT_FOUND.getSqlState(), se.getSQLState());
        }
    }

    @Test
    public void testDropColumnWorksWithinSingleTransaction() throws Exception {
        conn1.createStatement().execute("alter table " + table2 + " drop column b");

        ResultSet rs = conn1.query("select * from "+ table2);
        Assert.assertEquals("Metadata returning incorrect column count!", 1, rs.getMetaData().getColumnCount());

        try{
            conn1.query("select b from "+ table2);
            Assert.fail("Was able to find column b");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message returned:"+se.getMessage(),
                                ErrorState.LANG_COLUMN_NOT_FOUND.getSqlState(),se.getSQLState());
        }
    }

    @Test
    public void testDropColumnIsNotVisibleToOtherTransaction() throws Exception {
        testDropColumnWorksWithinSingleTransaction(); //dropped in conn1, but not in conn2

        ResultSet rs = conn2.query("select * from "+ table2);
        Assert.assertEquals("Metadata returning incorrect column count!", 2, rs.getMetaData().getColumnCount());

        rs = conn2.query("select b from "+ table2);
        Assert.assertEquals("Metadata for b-only query returning incorrect column count!", 1, rs.getMetaData().getColumnCount());

        //insert some data with a b field, and make sure that it's there
        int aInt = 2;
        int bInt = 2;
        PreparedStatement preparedStatement = conn2.prepareStatement("insert into " + table2 + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        preparedStatement.execute();

        long count = conn2.count("select b from " + table2);
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

        // conn1 sees column B dropped
        ResultSet rs = conn1.query("select * from "+ table);
        Assert.assertEquals("Metadata returning incorrect column count!", 1, rs.getMetaData().getColumnCount());

        // conn2 still sees column B, since conn1 has not committed
        rs = conn2.query("select * from "+ table);
        Assert.assertEquals("Metadata returning incorrect column count!", 2, rs.getMetaData().getColumnCount());

        // new connection still sees column B, since conn1 has not committed
        rs = classWatcher.createConnection().query("select * from "+ table);
        Assert.assertEquals("Metadata returning incorrect column count!", 2, rs.getMetaData().getColumnCount());

        conn1.commit();

        // new connection does not see column B, since conn1 has committed
        rs = classWatcher.createConnection().query("select * from "+ table);
        Assert.assertEquals("Metadata returning incorrect column count!", 1, rs.getMetaData().getColumnCount());
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

    @Test
    public void testDropColumnAfterUpdateWithPK() throws Exception {
        String tableName = "dropcolpk".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        methodWatcher.executeUpdate(String.format("create table %s (Field1 INT NOT NULL, Field2 CHAR(3), " +
                                                      "Field3 decimal(2,1), PRIMARY KEY(Field1))", tableRef));

        TestConnection conn = methodWatcher.createConnection();
        conn.createStatement().execute(String.format("insert into %s (Field1,Field2,Field3) VALUES (1,'abc',1.2)", tableRef));
        conn.createStatement().execute(String.format("insert into %s (Field1,Field2,Field3) VALUES (2,'efg',3.4)", tableRef));
        conn.createStatement().execute(String.format("insert into %s (Field1,Field2,Field3) VALUES (3,'hij',5.6)", tableRef));

        String query = String.format("select Field1, Field2, Field3 from %s", tableRef);
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.printResult(query, rs, System.out);

        conn.createStatement().execute(String.format("alter table %s add column Field4 BIGINT", tableRef));

        query = String.format("select Field1, Field2, Field3, Field4 from %s", tableRef);
        rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.printResult(query, rs, System.out);

        // updates hose the table
        conn.createStatement().execute(String.format("update %s set Field4 = 11", tableRef));
        // inserts are ok
//        conn.createStatement().execute(String.format("insert into %s (Field1,Field2,Field3,Field4) VALUES (4,'klm',7.8,22)", tableRef));

        query = String.format("select Field1, Field2, Field3, Field4 from %s", tableRef);
        rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.printResult(query, rs, System.out);

        conn.createStatement().execute(String.format("alter table %s drop column Field3", tableRef));

        query = String.format("select Field1, Field2, Field4 from %s", tableRef);
        rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.printResult(query, rs, System.out);

        rs = methodWatcher.getStatement().executeQuery(query);
        int count = 0;
        while (rs.next()) {
            String field2 = rs.getString("Field2");
            Assert.assertNotNull("Expected non-null valued for field2.", field2);
            ++count;
        }
        Assert.assertEquals("Incorrect returned row count", 3, count);
    }

    @Test
    public void testDropColumnAfterUpdateWithOutPK() throws Exception {
        String tableName = "dropcol".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        methodWatcher.executeUpdate(String.format("create table %s (Field1 INT NOT NULL, Field2 CHAR(3), " +
                                                      "Field3 decimal(2,1))", tableRef));

        TestConnection conn = methodWatcher.createConnection();
        conn.createStatement().execute(String.format("insert into %s (Field1,Field2,Field3) VALUES (1,'abc',1.2)", tableRef));
        conn.createStatement().execute(String.format("insert into %s (Field1,Field2,Field3) VALUES (2,'efg',3.4)", tableRef));
        conn.createStatement().execute(String.format("insert into %s (Field1,Field2,Field3) VALUES (3,'hij',5.6)", tableRef));

        conn.createStatement().execute(String.format("alter table %s add column Field4 BIGINT", tableRef));
        // updates hose the table (DB-3202)
        conn.createStatement().execute(String.format("update %s set Field4 = 11", tableRef));

        String query = String.format("select Field1, Field2, Field3, Field4 from %s", tableRef);
        ResultSet rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.printResult(query, rs, System.out);

        conn.createStatement().execute(String.format("alter table %s drop column Field3", tableRef));

        query = String.format("select Field1, Field2, Field4 from %s", tableRef);
        rs = methodWatcher.getStatement().executeQuery(query);
        TestUtils.printResult(query, rs, System.out);

        rs = methodWatcher.getStatement().executeQuery(query);
        int count = 0;
        while (rs.next()) {
            String field2 = rs.getString("Field2");
            Assert.assertNotNull("Expected non-null valued for field2.", field2);
            ++count;
        }
        Assert.assertEquals("Incorrect returned row count", 3, count);
    }
}
