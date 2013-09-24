package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
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
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(TaskId INT NOT NULL, empId Varchar(3) NOT NULL, StartedAt INT NOT NULL, FinishedAt INT NOT NULL)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME, "(i int)");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3);
//            .around(spliceTableWatcher4)
//            .around(spliceTableWatcher5);

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
    @Ignore("Bug 542. Connection 2 is seeing new column before connection 1 commits after adding it.")
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
    
}
