package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Index tests. Using more manual SQL, rather than SpliceIndexWatcher.
 * @see NonUniqueIndexTest
 * @see UniqueIndexTest
 */
public class CreateIndexConstantOperationTest extends SpliceUnitTest {
    public static final String CLASS_NAME = CreateIndexConstantOperationTest.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME_1 = "A";
    public static final String TABLE_NAME_2 = "B";
    public static final String TABLE_NAME_3 = "C";
    public static final String TABLE_NAME_4 = "D";
    public static final String TABLE_NAME_5 = "E";
    public static final String TABLE_NAME_6 = "F";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(TaskId INT NOT NULL, empId Varchar(3) NOT NULL, StartedAt INT NOT NULL, FinishedAt INT NOT NULL)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCreateIndexInsertGoodRow() throws Exception {
        methodWatcher.getStatement().execute("create index empIndex on "+this.getPaddedTableReference(TABLE_NAME_1)+" (empId)");
        methodWatcher.getStatement().execute("insert into "+this.getPaddedTableReference(TABLE_NAME_1)+" (TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
    }

    @Test
    public void testCreateUniqueIndexInsert() throws Exception {
        methodWatcher.getStatement().execute("create unique index taskIndex on "+this.getPaddedTableReference(TABLE_NAME_2)+" (taskid)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_2)+" (TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_2)+" (TaskId, empId, StartedAt, FinishedAt) values (1235,'JC',0601,0630)");
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where taskId = 1234",this.getPaddedTableReference(TABLE_NAME_2)));
        List<String> results = new ArrayList<String>();
        while(resultSet.next()){
            int taskId = resultSet.getInt(1);
            String empId = resultSet.getString(2);
            Assert.assertEquals("Incorrect taskId returned!", 1234, taskId);
            Assert.assertEquals("Incorrect empId returned!","JC",empId);
            results.add(String.format("TaskId:%d,empId:%s",taskId,empId));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
    }

    @Test(expected=SQLIntegrityConstraintViolationException.class)
    public void testCreateUniqueIndexInsertDupRow() throws Exception {
        methodWatcher.getStatement().execute("create unique index taskIndex on "+this.getPaddedTableReference(TABLE_NAME_3)+"(taskid)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_3)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_3)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0601,0630)");
    }

    @Test
    public void testCreateIndexInsertGoodRowDropIndex() throws Exception {
        methodWatcher.getStatement().execute("create index empIndex on "+this.getPaddedTableReference(TABLE_NAME_4)+"(empId)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_4)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
        methodWatcher.getStatement().execute("drop index "+this.getSchemaName()+".empIndex");
    }

    @Test
    public void testIndexDrop() throws Exception {
        methodWatcher.getStatement().execute("create index empIndex on "+this.getPaddedTableReference(TABLE_NAME_5)+"(empId)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_5)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
        methodWatcher.getStatement().execute("drop index "+this.getSchemaName()+".empIndex");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_5)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
    }

    @Test
    public void testCreateUniqueIndexInsertDupRowConcurrent() throws Exception {
        methodWatcher.getStatement().execute("create unique index taskIndex on "+this.getPaddedTableReference(TABLE_NAME_6)+"(empId,StartedAt)");
        Connection c1 = methodWatcher.createConnection();
        c1.setAutoCommit(false);
        c1.createStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_6)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");

        Connection c2 = methodWatcher.createConnection();
        c2.setAutoCommit(false);
        try {
            c2.createStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_6)+"(TaskId, empId, StartedAt, FinishedAt) values (1235,'JC',0500,0630)");
            Assert.fail("Didn't raise write-conflict exception");
        } catch (Exception e) {
            // ignore;
        }
        Connection c3 = methodWatcher.createConnection();
        c3.setAutoCommit(false);
        try {
            c3.createStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_6)+"(TaskId, empId, StartedAt, FinishedAt) values (1236,'JC',0500,0630)");
            Assert.fail("Didn't raise write-conflict exception");
        } catch (SQLException e) {
            Assert.assertTrue("Didn't detect write-write conflict", e.getCause().getCause().getMessage().contains("WriteConflict"));
        }
    }
}
