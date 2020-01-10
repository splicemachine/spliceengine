/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Index tests. Using more manual SQL, rather than SpliceIndexWatcher.
 * @see NonUniqueIndexIT
 * @see UniqueIndexIT
 */
public class CreateIndexConstantOperationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = CreateIndexConstantOperationIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME_1 = "A";
    public static final String TABLE_NAME_2 = "B";
    public static final String TABLE_NAME_3 = "C";
    public static final String TABLE_NAME_4 = "D";
    public static final String TABLE_NAME_5 = "E";
    public static final String TABLE_NAME_6 = "F";
    public static final String TABLE_NAME_7 = "G";
    public static final String TABLE_NAME_8 = "H";
    public static final String TABLE_NAME_9 = "T1";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static final String WRITE_WRITE_CONFLICT="SE014";

    private static String tableDef = "(TaskId INT NOT NULL, empId Varchar(3) NOT NULL, StartedAt INT NOT NULL, FinishedAt INT NOT NULL)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7,CLASS_NAME, 
    		"(col1 int, col2 int, col3 int, col4 int, col5 int, col6 int, col7 int, col8 int, col9 int,"
    		+ "col10 int, col11 int, col12 int, col13 int, col14 int, col15 int, col16 int, col17 int, col18 int,"
    		+ "col19 int, col20 int)");
    protected static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher(TABLE_NAME_8,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_NAME_9, CLASS_NAME, "(a1 int default 999, b1 varchar(20) default ' ', c1 int)");

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
            .around(spliceTableWatcher9);

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
        methodWatcher.getStatement().execute("create unique index taskIndex2 on "+this.getPaddedTableReference(TABLE_NAME_3)+"(taskid)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_3)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_3)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0601,0630)");
    }

    @Test
    public void testCreateIndexInsertGoodRowDropIndex() throws Exception {
        methodWatcher.getStatement().execute("create index empIndex2 on "+this.getPaddedTableReference(TABLE_NAME_4)+"(empId)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_4)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
        methodWatcher.getStatement().execute("drop index "+this.getSchemaName()+".empIndex2");
    }

    @Test
    public void testIndexDrop() throws Exception {
        methodWatcher.getStatement().execute("create index empIndex2 on "+this.getPaddedTableReference(TABLE_NAME_5)+"(empId)");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_5)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
        methodWatcher.getStatement().execute("drop index "+this.getSchemaName()+".empIndex2");
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_5)+"(TaskId, empId, StartedAt, FinishedAt) values (1234,'JC',0500,0600)");
    }

    @Test
    public void testCreateUniqueIndexInsertDupRowConcurrent() throws Exception {
        methodWatcher.getStatement().execute("create unique index taskIndex3 on "+this.getPaddedTableReference(TABLE_NAME_6)+"(empId,StartedAt)");
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
            Assert.assertEquals("Didn't detect write-write conflict", WRITE_WRITE_CONFLICT,e.getSQLState());
        }
    }

    @Test
    public void testCreateIndexWithMoreThan20Columns() throws Exception {
        methodWatcher.getStatement().execute("create index twentyplusindex on "+this.getPaddedTableReference(TABLE_NAME_7)+
                "(col1, col2, col3, col4, col5, col6, col7, col8, col9,"
                + "col10, col11, col12, col13, col14, col15, col16, col17, col18,"
                + "col19, col20)");
        Connection c1 = methodWatcher.createConnection();
        c1.setAutoCommit(false);
        c1.createStatement().execute("insert into"+this.getPaddedTableReference(TABLE_NAME_7)+
                "(col1, col2, col3, col4, col5, col6, col7, col8, col9,"
                + "col10, col11, col12, col13, col14, col15, col16, col17, col18,"
                + "col19, col20) values (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)");
        PreparedStatement ps = methodWatcher.prepareStatement("select * from "+this.getPaddedTableReference(TABLE_NAME_7) +
                "--SPLICE-PROPERTIES index=twentyplusindex\n" +
                "where col1=1 and col2=2 and col3=3 and col4=4 and col5=5 and col6=6 and col7=7 and col8=8"
                + " and col9=9 and col10=10");
        ResultSet resultSet = ps.executeQuery();
        Assert.assertTrue("index did not return result",resultSet.next());
    }

    @Test
    public void testCantCreateIndexWithSameName() throws Exception {
        methodWatcher.getStatement().execute("create index duplicateIndex on "+this.getPaddedTableReference(TABLE_NAME_8)+
                "(taskId)");
        try {
            methodWatcher.getStatement().execute("create index duplicateIndex on "+this.getPaddedTableReference(TABLE_NAME_8)+
                    "(empId)");
            Assert.fail("Shouldn't create an index with a duplicate name");
        } catch (SQLException e) {
            Assert.assertTrue(e.getCause().getMessage().contains("already exists"));
        }
    }

    @Test
    public void testCreateIndexExecludingDefaults() throws Exception {
        methodWatcher.execute(format("insert into %s values (1, ' ', 1), (2, ' ', 2), (3, 'xxxxxxxxxxxxxxxxxxxx', 3)", this.getPaddedTableReference(TABLE_NAME_9)));
        methodWatcher.execute(format("create index IX_T1_EXCL_DEFAULTS on %s(b1 desc, a1) exclude default keys", this.getPaddedTableReference(TABLE_NAME_9)));
        methodWatcher.execute(format("insert into %s values (4, ' ', 4), (5, ' ', 5), (6, 'yyyyyyyyyyyyyyyyyyyy', 6)", this.getPaddedTableReference(TABLE_NAME_9)));

        try {
            methodWatcher.executeQuery(format("SELECT * FROM %s --SPLICE-PROPERTIES index=IX_T1_EXCL_DEFAULTS", this.getPaddedTableReference(TABLE_NAME_9)));
            Assert.fail("did not throw exception");
        }
        catch (SQLException sqle) {
            Assert.assertEquals("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.", sqle.getMessage());
        }

        ResultSet rs = methodWatcher.executeQuery(format("SELECT * FROM %s --SPLICE-PROPERTIES index=IX_T1_EXCL_DEFAULTS\n where b1>' '", this.getPaddedTableReference(TABLE_NAME_9)));
        Assert.assertEquals("A1 |         B1          |C1 |\n" +
                "------------------------------\n" +
                " 3 |xxxxxxxxxxxxxxxxxxxx | 3 |\n" +
                " 6 |yyyyyyyyyyyyyyyyyyyy | 6 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }
}
