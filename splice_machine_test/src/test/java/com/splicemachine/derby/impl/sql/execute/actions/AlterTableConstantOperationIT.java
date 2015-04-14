package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;

import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class AlterTableConstantOperationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = AlterTableConstantOperationIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME_1 = "A";
    public static final String TABLE_NAME_2 = "B";
    public static final String TABLE_NAME_3 = "C";
    public static final String TABLE_NAME_4 = "D";
    public static final String TABLE_NAME_5 = "E";
    public static final String TABLE_NAME_6 = "F";
    public static final String TABLE_NAME_7 = "G";
    public static final String TABLE_NAME_8 = "H";
    public static final String TABLE_NAME_9 = "I";
    public static final String TABLE_NAME_10 = "J";
    public static final String TABLE_NAME_11 = "K";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(TaskId INT NOT NULL, empId Varchar(3) NOT NULL, StartedAt INT NOT NULL, FinishedAt INT NOT NULL)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME, "(i int)");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,CLASS_NAME, "(i int)");
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,CLASS_NAME, "(i int)");
    protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7,CLASS_NAME, "(i int, primary key (i))");
    protected static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher(TABLE_NAME_8,CLASS_NAME, "(i int)");
    protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_NAME_9,CLASS_NAME, "(i int)");
    protected static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher(TABLE_NAME_10,CLASS_NAME, "(i int)");
    protected static SpliceTableWatcher spliceTableWatcher11 = new SpliceTableWatcher(TABLE_NAME_11,CLASS_NAME,
            "(num varchar(4) not null, name char(20), " +
                    "grade decimal(4) not null check (grade in (100,150,200)), city char(15), " +
                    "primary key (grade,num))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(spliceTableWatcher7)
            .around(spliceTableWatcher8)
            .around(spliceTableWatcher9)
            .around(spliceTableWatcher10)
            .around(spliceTableWatcher11);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * Test basic alter table - insert, read, alter, write, read - make sure rows inserted after
     * alter are visible.
     * @throws Exception
     */
    @Test
    public void testInsertAlterTableInsert() throws Exception{
        Connection connection1 = methodWatcher.createConnection();
        for (int i=0; i<10; i++) {
            connection1.createStatement().execute(
                    String.format("insert into %1$s (TaskId, empId, StartedAt, FinishedAt) values (123%2$d,'JC',05%3$d,06%4$d)",
                            this.getTableReference(TABLE_NAME_1), i, i, i));
        }
        ResultSet resultSet = connection1.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_1)));
        Assert.assertEquals(10, resultSetSize(resultSet));
        connection1.createStatement().execute(String.format("alter table %s add column date timestamp", this.getTableReference(TABLE_NAME_1)));
        connection1.createStatement().execute(
                String.format("insert into %1$s (TaskId, empId, StartedAt, FinishedAt, Date) values (%2$d,'JC',09%3$d,09%4$d, '2013-05-28 13:03:20')",
                        this.getTableReference(TABLE_NAME_1), 1244, 0, 30));
        resultSet = connection1.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_1)));
        Assert.assertEquals("Expected to see another column", 5, columnWidth(resultSet));
        Assert.assertEquals("Expected to see an additional row.",11, resultSetSize(resultSet));
    }

    /**
     * Test basic thread isolation for alter table - 2 connections begin txns;
     * c1 adds a column, c2 should not be able to see it until after both c1 and c2 commit.
     * @throws Exception
     */
    @Test
    public void testAlterTableIsolation() throws Exception{
        Connection connection1 = methodWatcher.createConnection();
        Connection connection2 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        connection2.setAutoCommit(false);
        connection1.createStatement().execute(String.format("alter table %s add column date timestamp", this.getTableReference(TABLE_NAME_2)));
        connection1.createStatement().execute(
                String.format("insert into %1$s (TaskId, empId, StartedAt, FinishedAt, Date) values (%2$d,'JC',09%3$d,09%4$d, '2013-05-28 13:03:20')",
                        this.getTableReference(TABLE_NAME_2), 1244, 0, 30));
        ResultSet resultSet = connection1.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_2)));
        Assert.assertEquals("Can't read own write",1, resultSetSize(resultSet));
        resultSet = connection2.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_2)));
        Assert.assertEquals("Read Committed Violated",0, resultSetSize(resultSet));
        Assert.assertEquals("Didn't expect to see another column until after commit.", 4, columnWidth(resultSet));

        connection2.commit();
        resultSet = connection2.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_2)));
        Assert.assertEquals("Read Committed Violated",0, resultSetSize(resultSet));
        Assert.assertEquals("Didn't expect to see another column until after commit.", 4, columnWidth(resultSet));

        connection2.commit();
        resultSet = connection2.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_2)));
        Assert.assertEquals("Read Committed Violated",0, resultSetSize(resultSet));
        Assert.assertEquals("Didn't expect to see another column until after commit.", 4, columnWidth(resultSet));

        connection1.commit();
        connection2.commit();
        resultSet = connection2.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_2)));
        Assert.assertEquals("Expected to see an additional row.",1, resultSetSize(resultSet));
        Assert.assertEquals("Expected to see another column.", 5, columnWidth(resultSet));
    }

    @Test
    public void testUpdatingAlteredColumns() throws Exception {
        Statement s = methodWatcher.getStatement();
        s.executeUpdate(String.format("insert into %s values 1,2,3,4,5",this.getTableReference(TABLE_NAME_3)));
        s.executeUpdate(String.format("alter table %s add column j int",this.getTableReference(TABLE_NAME_3)));
        s.executeUpdate(String.format("update %s set j = 5",this.getTableReference(TABLE_NAME_3)));
        ResultSet rs = methodWatcher.executeQuery(String.format("select j from %s",this.getTableReference(TABLE_NAME_3)));
        int i=0;
        while (rs.next()) {
            i++;
            Assert.assertEquals("Update did not happen", 5, rs.getInt(1));
        }
        Assert.assertEquals("returned values must match", 5,i);
    }

    @Test
    public void testAlterTableIsolationInactiveTransaction() throws Exception{
        Connection connection1 = methodWatcher.createConnection();
        Connection connection2 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        connection2.setAutoCommit(false);
        ResultSet resultSet = connection2.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_4)));
        Assert.assertEquals("Read Committed Violated",0, resultSetSize(resultSet));
        connection2.commit();
        Assert.assertEquals("Didn't expect to see another column until after commit.", 4, columnWidth(resultSet));
        connection1.createStatement().execute(String.format("alter table %s add column date timestamp", this.getTableReference(TABLE_NAME_4)));
        connection1.createStatement().execute(
                String.format("insert into %1$s (TaskId, empId, StartedAt, FinishedAt, Date) values (%2$d,'JC',09%3$d,09%4$d, '2013-05-28 13:03:20')",
                        this.getTableReference(TABLE_NAME_4), 1244, 0, 30));
        resultSet = connection1.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_4)));
        Assert.assertEquals("Can't read own write", 1, resultSetSize(resultSet));
        connection1.commit();
        resultSet = connection2.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_4)));
        Assert.assertEquals("Expected to see an additional row.",1, resultSetSize(resultSet));
        Assert.assertEquals("Expected to see another column.", 5, columnWidth(resultSet));
        connection2.commit();
    }

    @Test
    public void testAddColumnDefaultIsReadable() throws Exception {
        Connection conn = methodWatcher.createConnection();
        try(Statement statement=conn.createStatement()){
            statement.execute(String.format("insert into %s values 1,2,3",spliceTableWatcher5));

            try(ResultSet resultSet=statement.executeQuery("select * from "+ spliceTableWatcher5)){
                Assert.assertEquals("Should have only 1 column.",1,columnWidth(resultSet));
            }
            statement.execute(String.format("alter table %s add column j int default 5",spliceTableWatcher5));

            try(ResultSet resultSet=statement.executeQuery("select * from "+ spliceTableWatcher5)){
                Assert.assertEquals("Should have 2 columns.",2,columnWidth(resultSet));
                int count=0;
                while(resultSet.next()){
                    Assert.assertEquals("Second column should have the default value",5,resultSet.getInt(2));
                    count++;
                }
                Assert.assertEquals("Wrong number of results",3,count);
            }
        }finally{
            conn.close();
        }
    }

    @Test
    public void testTruncate() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_6)));

        PreparedStatement ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_6)));
        ResultSet resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));

        connection1.createStatement().execute(String.format("truncate table %s", this.getTableReference(TABLE_NAME_6)));

        ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_6)));
        resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 0 rows.", 0, resultSetSize(resultSet));

        connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_6)));

        ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_6)));
        resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));
    }

    @Test
    public void testTruncatePK() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_7)));

        PreparedStatement ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_7)));
        ResultSet resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));

        connection1.createStatement().execute(String.format("truncate table %s", this.getTableReference(TABLE_NAME_7)));

        ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_7)));
        resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 0 rows.", 0, resultSetSize(resultSet));

        connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_7)));

        ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_7)));
        resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));
    }


    @Test
    public void testTruncateWithIndex() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.createStatement().execute(String.format("create index trunc_idx on %s (i)", this.getTableReference(TABLE_NAME_8)));
        try {
            connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_8)));

            PreparedStatement ps = connection1.prepareStatement(String.format("select i from %s --splice-properties index=trunc_idx", this.getTableReference(TABLE_NAME_8)));
            ResultSet resultSet = ps.executeQuery();
            Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));

            connection1.createStatement().execute(String.format("truncate table %s", this.getTableReference(TABLE_NAME_8)));

            ps = connection1.prepareStatement(String.format("select i from %s --splice-properties index=trunc_idx", this.getTableReference(TABLE_NAME_8)));
            resultSet = ps.executeQuery();

            Assert.assertEquals("Should have 0 rows.", 0, resultSetSize(resultSet));

            connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_8)));

            ps = connection1.prepareStatement(String.format("select i from %s --splice-properties index=trunc_idx", this.getTableReference(TABLE_NAME_8)));
            resultSet = ps.executeQuery();
            Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));
        } finally {
            connection1.createStatement().execute(String.format("drop index %s", this.getTableReference("trunc_idx")));
        }
    }


    @Test
    public void testTruncateIsolation() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_9)));

        PreparedStatement ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_9)));
        ResultSet resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));

        Connection connection2 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        connection1.createStatement().execute(String.format("truncate table %s", this.getTableReference(TABLE_NAME_9)));

        ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_9)));
        resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 0 rows.", 0, resultSetSize(resultSet));

        PreparedStatement ps2 = connection2.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_9)));
        ResultSet resultSet2 = ps2.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet2));

        connection2.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_9)));
        ps2 = connection2.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_9)));
        resultSet2 = ps2.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 6, resultSetSize(resultSet2));

        connection1.commit();
        connection2.rollback();

        ps2 = connection2.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_9)));
        resultSet2 = ps2.executeQuery();
        Assert.assertEquals("Should have 0 rows.", 0, resultSetSize(resultSet2));

        connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_9)));

        ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_9)));
        resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));
        connection1.commit();
    }



    @Test
    public void testTruncateRollback() throws Exception {
        Connection connection1 = methodWatcher.createConnection();
        connection1.createStatement().execute(String.format("insert into %s values 1,2,3", this.getTableReference(TABLE_NAME_10)));

        PreparedStatement ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_10)));
        ResultSet resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));

        connection1.setAutoCommit(false);
        connection1.createStatement().execute(String.format("truncate table %s", this.getTableReference(TABLE_NAME_10)));

        ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_10)));
        resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 0 rows.", 0, resultSetSize(resultSet));

        connection1.rollback();

        ps = connection1.prepareStatement(String.format("select * from %s", this.getTableReference(TABLE_NAME_10)));
        resultSet = ps.executeQuery();
        Assert.assertEquals("Should have 3 rows.", 3, resultSetSize(resultSet));
        connection1.commit();
    }

    @Test
    public void testAddCheckConstraint() throws Exception {
        // test for DB-2705: NPE when altering table adding check constraint.
        // Does not check that check constraint works - they don't - just tests the NPE is fixed.
        TestConnection connection = methodWatcher.createConnection();

        connection.createStatement().execute(String.format("alter table %s add constraint numck check (num < '999')",
                spliceTableWatcher11));
        connection.commit();

        connection.createStatement().execute(String.format("insert into %s values ('01', 'Jeff', 100, " +
                        "'St. Louis')",
                spliceTableWatcher11));

        long count = connection.count(String.format("select * from %s", spliceTableWatcher11));
        Assert.assertEquals("incorrect row count!", 1, count);

    }
}
