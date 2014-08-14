package com.splicemachine.management;

import com.splicemachine.derby.management.XPlainTrace;
import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.*;
import java.util.Deque;

/**
 * Created by jyuan on 7/7/14.
 */
public class XPlainTrace1IT extends XPlainTrace {

    public static final String CLASS_NAME = XPlainTrace1IT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE1 = "TAB1";
    public static final String TABLE2 = "TAB2";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2,CLASS_NAME, tableDef);
    private static final int nrows = 10;

    private SpliceXPlainTrace xPlainTrace = new SpliceXPlainTrace();

    public XPlainTrace1IT() {
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

    @Before
    public void setUp() throws Exception {
        Connection connection = SpliceNetConnection.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("call SYSCS_UTIL.SYSCS_PURGE_XPLAIN_TRACE()");

        ResultSet rs = statement.executeQuery("select count(*) from sys.sysstatementhistory");
        int count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        rs.close();
        Assert.assertTrue(count==0);

    }

    @After
    public void tearDown() throws Exception {
        Connection connection = SpliceNetConnection.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select count(*) from sys.sysstatementhistory");
        int count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        rs.close();
        Assert.assertTrue(count>0);

        statement.execute("call SYSCS_UTIL.SYSCS_PURGE_XPLAIN_TRACE()");

        rs = statement.executeQuery("select count(*) from sys.sysstatementhistory");
        count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        rs.close();
        Assert.assertTrue(count==0);
    }
    @Test
    public void testBroadcastJoin() throws Exception {
        xPlainTrace.turnOnTrace();

        String sql = "select * from " +
                CLASS_NAME + "." + TABLE1 + " t1, " + CLASS_NAME + "." + TABLE2 + " t2 " +
                "where t1.i = t2.i";
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, 10);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        String operationType = operation.getOperationType();

        Assert.assertTrue(operationType.compareToIgnoreCase(SpliceXPlainTrace.BROADCASTJOIN)==0);
        Assert.assertEquals(operation.getInputRows(), 10);
        Assert.assertEquals(operation.getOutputRows(), 10);
        Assert.assertEquals(operation.getWriteRows(), 0);
        Assert.assertEquals(operation.getRemoteScanRows(), 10);
        Assert.assertTrue(operation.getInfo().contains("Join Condition:(T1.I[4:1] = T2.I[4:2])"));
    }

    @Test
    public void testMergeSortJoin() throws Exception {
        xPlainTrace.turnOnTrace();

        String sql = "select * from \n" +
                CLASS_NAME + "." + TABLE1 + " t1, " + CLASS_NAME + "." + TABLE2 + " t2 --SPLICE-PROPERTIES joinStrategy=SORTMERGE\n" +
                "where t1.i = t2.i";
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, 10);
        xPlainTrace.turnOffTrace();
        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        String operationType = operation.getOperationType();

        Assert.assertTrue(operationType.compareToIgnoreCase(SpliceXPlainTrace.MERGESORTJOIN)==0);
        Assert.assertTrue(operation.getInfo().contains("Join Condition:(T1.I[4:1] = T2.I[4:2])"));
        Assert.assertEquals(operation.getInputRows(), nrows);
        Assert.assertEquals(operation.getOutputRows(), nrows);
        Assert.assertEquals(operation.getWriteRows(), 2*nrows);
        Assert.assertEquals(operation.getRemoteScanRows(), 2*nrows);
    }

    /*@Test
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
    }*/

    @Test
    public void testTableScan() throws Exception {
        xPlainTrace.turnOnTrace();

        String sql = "select * from " + CLASS_NAME + "." + TABLE1 + " where i > 0";
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, nrows-1);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode topOperation = xPlainTrace.getOperationTree();
        String operationType = topOperation.getOperationType();

        Assert.assertEquals(operationType.contains(SpliceXPlainTrace.TABLESCAN), true);
        Assert.assertEquals(topOperation.getLocalScanRows(), nrows);
        Assert.assertTrue(topOperation.getInfo().contains("Scan filter:(I[0:1] > 0), table:"));
    }

    @Test
    public void testCountStar() throws Exception {
        xPlainTrace.turnOnTrace();

        String sql = "select count(*) from " + CLASS_NAME + "." + TABLE1;
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        Assert.assertEquals(count, 10);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode topOperation = xPlainTrace.getOperationTree();

        String operationType = topOperation.getOperationType();
        Assert.assertEquals(operationType.compareTo(SpliceXPlainTrace.PROJECTRESTRICT), 0);
        Assert.assertEquals(topOperation.getInputRows(), 1);

        // Should be ScalarAggregate
        Deque<XPlainTreeNode> children = topOperation.getChildren();
        Assert.assertEquals(children.size(), 1);

        XPlainTreeNode child = children.getFirst();
        Assert.assertEquals(child.getOperationType().compareTo(SpliceXPlainTrace.SCALARAGGREGATE), 0);
        Assert.assertEquals(child.getInputRows(), 10);
        Assert.assertEquals(child.getOutputRows(), 1);
        Assert.assertEquals(child.getWriteRows(), 1);
    }

    @Test
    @Ignore
    public void testNestedLoopJoin() throws Exception {

        xPlainTrace.turnOnTrace();

        String sql = "select * from " +
                      CLASS_NAME + "." + TABLE1 + " t1, " + CLASS_NAME + "." + TABLE2 + " t2 " +
                      "where t1.i = t2.i*2";
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, 5);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        String operationType = operation.getOperationType();
        Assert.assertEquals(operationType.compareTo(SpliceXPlainTrace.PROJECTRESTRICT), 0);
        Assert.assertEquals(operation.getInputRows(), count);
        Assert.assertEquals(operation.getOutputRows(), count);

        Assert.assertEquals(operation.getChildren().size(), 1);
        operation = operation.getChildren().getFirst();
        operationType = operation.getOperationType();
        Assert.assertEquals(operationType.compareTo(SpliceXPlainTrace.BROADCASTJOIN), 0);
        Assert.assertEquals(operation.getInputRows(), nrows);
        Assert.assertEquals(operation.getRemoteScanRows(), count);
        Assert.assertEquals(operation.getOutputRows(), count);

        // Must have two children
        Assert.assertEquals(operation.getChildren().size(), 2);
        // First child should be a bulk table scan operation
        XPlainTreeNode child = operation.getChildren().getFirst();
        operationType = child.getOperationType();
        Assert.assertEquals(operationType.compareTo(SpliceXPlainTrace.BULKTABLESCAN), 0);
        Assert.assertEquals(child.getLocalScanRows(), nrows);
        Assert.assertEquals(child.getOutputRows(), nrows);

        // right child should be a bulk table scan operation
        child = operation.getChildren().getLast();
        operationType = child.getOperationType();
        Assert.assertEquals(operationType.compareTo(SpliceXPlainTrace.BULKTABLESCAN), 0);
        Assert.assertEquals(child.getLocalScanRows(), nrows*nrows);
        Assert.assertEquals(child.getOutputRows(), count);
        Assert.assertEquals(child.getFilteredRows(), nrows*nrows - count);
        Assert.assertEquals(child.getIterations(), nrows);
        Assert.assertTrue(child.getInfo().contains("Scan filter:(T1.I[2:1] = (T2.I[1:1] * 2)), table:"));
    }
}
