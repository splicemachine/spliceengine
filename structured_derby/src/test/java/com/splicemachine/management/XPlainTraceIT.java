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
    private static final String PROJECTRESTRICTOPERATION = "ProjectRestrict";
    private static final String SCALARAGGREGATEOPERATION = "ScalarAggregate";

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

        Assert.assertEquals(topOperation.getOperationType().contains(TABLESCANOPERATION), true);
        Assert.assertEquals(topOperation.getLocalScanRows(), nrows);
    }

    @Test
    public void testCountStar() throws Exception {
        turnOnTrace();

        String sql = "select count(*) from " + CLASS_NAME + "." + TABLE1;
        ResultSet rs = statement.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, 1);

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

        Assert.assertEquals(topOperation.getOperationType().compareTo(PROJECTRESTRICTOPERATION), 0);
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
        /*
        select t1.i from --SPLICE-PROPERTIES joinOrder=FIXED
        tab1 t1, tab2 t2 --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP
        where t1.i = t2.i*2;
         */
    }
}
