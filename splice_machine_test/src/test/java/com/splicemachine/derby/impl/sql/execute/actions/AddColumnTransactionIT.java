package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.pipeline.exception.ErrorState;

import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 9/3/14
 */
public class AddColumnTransactionIT {
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(AddColumnTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher commitTable = new SpliceTableWatcher("B",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher beforeTable = new SpliceTableWatcher("C",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher afterTable = new SpliceTableWatcher("D",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher addedTable = new SpliceTableWatcher("E",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher addedTable2 = new SpliceTableWatcher("F",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher addedTable3 = new SpliceTableWatcher("G",schemaWatcher.schemaName,"(a int, b int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    public static final String query = "select * from " + table+" where a = ";
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table)
            .around(commitTable)
            .around(beforeTable)
            .around(afterTable)
            .around(addedTable)
            .around(addedTable2)
            .around(addedTable3);

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
    public void testAddColumnWorksWithOneConnectionAndCommitDefaultNull() throws Exception {
        int aInt = 1;
        int bInt = 1;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + commitTable + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        conn1.commit();

        conn1.createStatement().execute("alter table "+ commitTable+" add column c int");
        conn1.commit();

        ResultSet rs = conn1.query("select * from " + commitTable);

        while(rs.next()){
            rs.getInt("C");
            Assert.assertTrue("Column C is not null!",rs.wasNull());
        }
    }

    @Test
    public void testAddColumnWorksWithOneConnectionAndCommitDefaultValue() throws Exception {
        int aInt = 2;
        int bInt = 2;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + commitTable + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        conn1.commit();

        conn1.createStatement().execute("alter table "+ commitTable+" add column d int with default 2");
        conn1.commit();

        ResultSet rs = conn1.query("select * from " + commitTable +" where a = "+aInt);
        Assert.assertEquals("Incorrect metadata reporting!",3,rs.getMetaData().getColumnCount());

        while(rs.next()){
            int anInt = rs.getInt("D");
            Assert.assertEquals("Incorrect value for column D",2,anInt);
            Assert.assertTrue("Column D is null!",!rs.wasNull());
        }
    }

    @Test
    public void testAddColumnWorksWithOneTransaction() throws Exception {
        int aInt = 3;
        int bInt = 3;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2, bInt);

        conn1.createStatement().execute("alter table " + commitTable + " add column e int with default 2");

        ResultSet rs = conn1.query("select * from " + commitTable +" where a = "+aInt);

        while(rs.next()){
            int anInt = rs.getInt("E");
            Assert.assertEquals("Incorrect value for column E",2,anInt);
            Assert.assertTrue("Column E is null!",!rs.wasNull());
        }
    }

    @Test
    public void testAddColumnRemovedWhenRolledBack() throws Exception {
        int aInt = 4;
        testAddColumnWorksWithOneTransaction();

        conn1.rollback();

        ResultSet rs = conn1.query("select * from " + commitTable+ " where a = "+ aInt);

        if(rs.next()){
            try{
                int anInt = rs.getInt("E");
                Assert.fail("did not fail!");
            }catch(SQLException se){
                Assert.assertEquals("Incorrect error message!", ErrorState.COLUMN_NOT_FOUND.getSqlState(),se.getSQLState());
            }
        }
    }

    @Test
    public void testAddColumnIgnoredByOtherTransaction() throws Exception {
        int aInt = 4;
        int bInt = 4;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);

        conn1.createStatement().execute("alter table "+ commitTable+" add column f int with default 2");

        ResultSet rs = conn1.query("select * from " + commitTable +" where a = "+aInt);

        while(rs.next()){
            int anInt = rs.getInt("F");
            Assert.assertEquals("Incorrect value for column f",2,anInt);
            Assert.assertTrue("Column f is null!",!rs.wasNull());
        }

        conn2.query("select * from " + commitTable + " where a = " + aInt);

        if(rs.next()){
            try{
                int anInt = rs.getInt("F");
                Assert.fail("did not fail!");
            }catch(SQLException se){
                Assert.assertEquals("Incorrect error message!", ErrorState.COLUMN_NOT_FOUND.getSqlState(),se.getSQLState());
            }
        }
    }

    @Test
    public void testAddColumnCannotProceedWithOpenDMLOperations() throws Exception {
        TestConnection a;
        TestConnection b;
        if(conn1Txn>conn2Txn){
            a = conn1;
            b = conn2;
        }else{
            a = conn2;
            b = conn1;
        }

        int aInt = 7;
        int bInt = 7;
        PreparedStatement ps = b.prepareStatement("insert into "+addedTable3+" (a,b) values (?,?)");
        ps.setInt(1,aInt);ps.setInt(2,bInt); ps.execute();

        try{
            a.createStatement().execute("alter table "+ addedTable3+" add column c int with default 2");
            Assert.fail("Did not catch an exception!");
        }catch(SQLException se){
            System.out.printf("%s:%s%n",se.getSQLState(),se.getMessage());
            Assert.assertEquals("Incorrect error message!",ErrorState.DDL_ACTIVE_TRANSACTIONS.getSqlState(),se.getSQLState());
        }
    }

    @Test
    public void testAddColumnAfterInsertionIsCorrect() throws Exception {
        TestConnection a;
        TestConnection b;
        if(conn1Txn>conn2Txn){
            a = conn2;
            b = conn1;
        }else{
            a = conn1;
            b = conn2;
        }

        int aInt = 8;
        int bInt = 8;
        PreparedStatement ps = b.prepareStatement("insert into "+addedTable+" (a,b) values (?,?)");
        ps.setInt(1,aInt);ps.setInt(2,bInt); ps.execute();
        b.commit();

        a.createStatement().execute("alter table "+ addedTable+" add column f int not null with default 2");
        a.commit();

        ResultSet rs = a.query("select * from "+ addedTable+" where a = "+ aInt);
        int count=0;
        while(rs.next()){
            int f = rs.getInt("F");
            Assert.assertFalse("Got a null value for f!",rs.wasNull());
            Assert.assertEquals("Incorrect default value!",2,f);
            count++;
        }
        Assert.assertEquals("Incorrect returned row count",1,count);
    }

    @Test
    public void testAddColumnBeforeInsertionIsCorrect() throws Exception {
        TestConnection a;
        TestConnection b;
        if(conn1Txn>conn2Txn){
            a = conn2;
            b = conn1;
        }else{
            a = conn1;
            b = conn2;
        }

        int aInt = 10;
        int bInt = 10;

        //alter the table
        a.createStatement().execute("alter table " + addedTable2 + " add column f int not null default 2");
        //now insert some data
        PreparedStatement ps = b.prepareStatement("insert into "+addedTable2+" (a,b) values (?,?)");
        ps.setInt(1,aInt);ps.setInt(2,bInt); ps.execute();
        b.commit();

        a.commit();
        ResultSet rs = a.query("select * from "+ addedTable2+" where a = "+ aInt);
        int count=0;
        while(rs.next()){
            int f = rs.getInt("F");
            Assert.assertFalse("Got a null value for f!",rs.wasNull());
            Assert.assertEquals("Incorrect default value!",2,f);
            count++;
        }
        Assert.assertEquals("Incorrect returned row count",1,count);
    }
}
