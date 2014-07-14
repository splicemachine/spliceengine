package com.splicemachine.management;

import com.splicemachine.derby.management.XPlainTrace;
import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceNetConnection;

import java.sql.*;
import java.util.Deque;

/**
 * Created by jyuan on 7/7/14.
 */
public class XPlainTraceIT extends XPlainTrace {

    private static final String STATEMENT_TABLE = "SYS.SYSSTATEMENTHISTORY";
    private static final String TASK_TABLE = "SYS.SYSTASKHISTORY";

    private static final String SCROLLINSENSITIVEOPERATION = "ScrollInsensitive";
    private static final String TABLESCANOPERATION = "TableScan";
    private static final String BULKTABLESCANOPERATION = "BulkTableScan";
    private static final String PROJECTRESTRICTOPERATION = "ProjectRestrict";
    private static final String SCALARAGGREGATEOPERATION = "ScalarAggregate";
    private static final String NESTEDLOOPJOINOPERATION = "NestedLoopJoin";

    public static final String CLASS_NAME = XPlainTraceIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE1 = "TAB1";
    public static final String TABLE2 = "TAB2";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2,CLASS_NAME, tableDef);
    private static final int nrows = 10;
    private Connection connection = null;
    private Statement statement = null;

    public XPlainTraceIT() {
        super();
    }


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher1));
                        for(int i=0;i<nrows;i++){
                            ps.setInt(1,i);
                            ps.execute();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher2).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher2));
                        for(int i=0;i<nrows;i++){
                            ps.setInt(1,i);
                            ps.execute();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            ;

    private void waitForStatement(long statementId) throws Exception{
        String stmt = "select count(*) from " + STATEMENT_TABLE + " where statementid = " + statementId;
        int count = 0;
        // Wait util statementHistory table is populated
        while (count == 0) {
            ResultSet rs = methodWatcher.executeQuery(stmt);
            if (rs.next()) {
                count = rs.getInt(1);
            }
        }

        // Wait until taskHistory table is populated
        int count_before = -1;
        count = -1;
        stmt = "select count(*) from " + TASK_TABLE + " where statementid = " + statementId;
        while (count_before != count && count > 0) {
            count_before = count;
            ResultSet rs = methodWatcher.executeQuery(stmt);
            if (rs.next()) {
                count = rs.getInt(1);
            }
        }
    }

    // Turn on xplain trace
    private void turnOnTrace() throws Exception {
        connection = SpliceNetConnection.getConnection();
        statement = connection.createStatement();
        statement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        statement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
    }

    // Turn off xplain trace
    private void turnOffTrace() throws Exception {
        statement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        statement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");
    }

    @Test
    public void testXPlainTraceOnOff() throws Exception {
        connection = SpliceNetConnection.getConnection();
        statement = connection.createStatement();

        // Turn on explain trace
        statement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        statement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");

        String sql = "select * from " + CLASS_NAME + "." + TABLE1;
        ResultSet rs = statement.executeQuery(sql);
        int c = 0;
        while (rs.next()) {
            ++c;
        }
        Assert.assertEquals(c, nrows);
        statement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        statement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");

        // Count the #of traced sql statements
        sql = "select * from sys.sysstatementhistory";
        rs = statement.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            ++count;
        }

        // Execute the same statement. It should not be traced
        sql = "select * from " + CLASS_NAME + "." + TABLE1;
        rs = statement.executeQuery(sql);
        c = 0;
        while (rs.next()) {
            ++c;
        }
        Assert.assertEquals(c, nrows);

        sql = "select * from sys.sysstatementhistory";
        rs = statement.executeQuery(sql);
        c = 0;
        while (rs.next()) {
            ++c;
        }
        // # of traced statement should not change
        Assert.assertEquals(c, count);

        // Turn on xplain trace and run the same sql statement. It should be traced
        statement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        statement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        sql = "select * from " + CLASS_NAME + "." + TABLE1;
        rs = statement.executeQuery(sql);
        c = 0;
        while (rs.next()) {
            ++c;
        }
        Assert.assertEquals(c, nrows);
        long statementId = 0;
        rs = statement.executeQuery("call SYSCS_UTIL.SYSCS_GET_XPLAIN_STATEMENTID()");
        if (rs.next()) {
            statementId = rs.getLong(1);
        }
        waitForStatement(statementId);
        statement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        statement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");

        sql = "select * from sys.sysstatementhistory";
        rs = statement.executeQuery(sql);
        c = 0;
        while (rs.next()) {
            ++c;
        }
        // # of traced statement should increase by 2:
        // 1 for the sql statement, 1 for call SYSCS_UTIL.SYSCS_GET_XPLAIN_STATEMENTID()
        Assert.assertEquals(c, count+2);
    }

    @Test
    public void testTableScan() throws Exception {
        turnOnTrace();

        String sql = "select * from " + CLASS_NAME + "." + TABLE1;
        ResultSet rs = statement.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, nrows);

        long statementId = 0;
        rs = statement.executeQuery("call SYSCS_UTIL.SYSCS_GET_XPLAIN_STATEMENTID()");
        if (rs.next()) {
            statementId = rs.getLong(1);
        }
        turnOffTrace();
        waitForStatement(statementId);

        XPlainTraceIT xPlainTrace = new XPlainTraceIT();
        xPlainTrace.setStatementId(statementId);
        xPlainTrace.setFormat("tree");
        xPlainTrace.setMode(0);
        xPlainTrace.setConnection(connection);
        XPlainTreeNode topOperation = xPlainTrace.getTopOperation();

        String operationType = topOperation.getOperationType();

        Assert.assertEquals(operationType.contains(TABLESCANOPERATION), true);
        Assert.assertEquals(topOperation.getLocalScanRows(), nrows);
    }

    @Test
    public void testCountStar() throws Exception {
        turnOnTrace();

        String sql = "select count(*) from " + CLASS_NAME + "." + TABLE1;
        ResultSet rs = statement.executeQuery(sql);
        int count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        Assert.assertEquals(count, 10);

        long statementId = 0;
        rs = statement.executeQuery("call SYSCS_UTIL.SYSCS_GET_XPLAIN_STATEMENTID()");
        if (rs.next()) {
            statementId = rs.getLong(1);
        }

        turnOffTrace();

        waitForStatement(statementId);

        XPlainTraceIT xPlainTrace = new XPlainTraceIT();
        xPlainTrace.setStatementId(statementId);
        xPlainTrace.setFormat("tree");
        xPlainTrace.setMode(0);
        xPlainTrace.setConnection(connection);
        XPlainTreeNode topOperation = xPlainTrace.getTopOperation();

        String operationType = topOperation.getOperationType();
        Assert.assertEquals(operationType.compareTo(PROJECTRESTRICTOPERATION), 0);
        Assert.assertEquals(topOperation.getInputRows(), 1);

        // Should be ScalarAggregate
        Deque<XPlainTreeNode> children = topOperation.getChildren();
        Assert.assertEquals(children.size(), 1);

        XPlainTreeNode child = children.getFirst();
        Assert.assertEquals(child.getOperationType().compareTo(SCALARAGGREGATEOPERATION), 0);
        Assert.assertEquals(child.getInputRows(), 10);
        Assert.assertEquals(child.getOutputRows(), 1);
        Assert.assertEquals(child.getWriteRows(), 1);
    }

    @Test
    public void testNestedLoopJoin() throws Exception {

        turnOnTrace();

        String sql = "select * from " +
                      CLASS_NAME + "." + TABLE1 + " t1, " + CLASS_NAME + "." + TABLE2 + " t2 " +
                      "where t1.i = t2.i*2";
        ResultSet rs = statement.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, 5);

        long statementId = 0;
        rs = statement.executeQuery("call SYSCS_UTIL.SYSCS_GET_XPLAIN_STATEMENTID()");
        if (rs.next()) {
            statementId = rs.getLong(1);
        }

        turnOffTrace();

        waitForStatement(statementId);

        XPlainTraceIT xPlainTrace = new XPlainTraceIT();
        xPlainTrace.setStatementId(statementId);
        xPlainTrace.setFormat("tree");
        xPlainTrace.setMode(0);
        xPlainTrace.setConnection(connection);

        XPlainTreeNode operation = xPlainTrace.getTopOperation();
        String operationType = operation.getOperationType();
        Assert.assertEquals(operationType.compareTo(PROJECTRESTRICTOPERATION), 0);
        Assert.assertEquals(operation.getInputRows(), count);
        Assert.assertEquals(operation.getOutputRows(), count);

        Assert.assertEquals(operation.getChildren().size(), 1);
        operation = operation.getChildren().getFirst();
        operationType = operation.getOperationType();
        Assert.assertEquals(operationType.compareTo(NESTEDLOOPJOINOPERATION), 0);
        Assert.assertEquals(operation.getInputRows(), nrows);
        Assert.assertEquals(operation.getRemoteScanRows(), count);
        Assert.assertEquals(operation.getOutputRows(), count);

        // Must have two children
        Assert.assertEquals(operation.getChildren().size(), 2);
        // First child should be a bulk table scan operation
        XPlainTreeNode child = operation.getChildren().getFirst();
        operationType = child.getOperationType();
        Assert.assertEquals(operationType.compareTo(BULKTABLESCANOPERATION), 0);
        Assert.assertEquals(child.getLocalScanRows(), nrows);
        Assert.assertEquals(child.getOutputRows(), nrows);

        // right child should be a bulk table scan operation
        child = operation.getChildren().getLast();
        operationType = child.getOperationType();
        Assert.assertEquals(operationType.compareTo(BULKTABLESCANOPERATION), 0);
        Assert.assertEquals(child.getLocalScanRows(), nrows*nrows);
        Assert.assertEquals(child.getOutputRows(), count);
        Assert.assertEquals(child.getFilteredRows(), nrows*nrows - count);
        Assert.assertEquals(child.getIterations(), nrows);

    }
}
