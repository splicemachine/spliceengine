package com.splicemachine.derby.test.framework;

import com.splicemachine.derby.management.XPlainTrace;
import com.splicemachine.derby.management.XPlainTreeNode;

import java.sql.*;

/**
 * Created by jyuan on 7/14/14.
 */
public class SpliceXPlainTrace extends XPlainTrace{

    private static final String STATEMENT_TABLE = "SYS.SYSSTATEMENTHISTORY";
    private static final String TASK_TABLE = "SYS.SYSTASKHISTORY";

    public static final String TABLESCAN = "TableScan";
    public static final String BULKTABLESCAN = "BulkTableScan";
    public static final String PROJECTRESTRICT = "ProjectRestrict";
    public static final String SCALARAGGREGATE = "ScalarAggregate";
    public static final String NESTEDLOOPJOIN = "NestedLoopJoin";
    public static final String MERGESORTJOIN = "MergeSortJoin";
    public static final String MERGEJOIN = "MergeJoin";
    public static final String BROADCASTJOIN = "BroadcastJoin";
    public static final String INDEXROWTOBASEROW = "IndexRowToBaseRow";
    public static final String SORT = "SORT";
    public static final String DISTINCTSCAN = "DISTINCTSCAN";
    public static final String ONCE = "ONCE";
    public static final String INSERT = "INSERT";
    public static final String UNION = "UNION";
    public static final String ROW = "ROW";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
    public static final String POPULATEINDEX = "POPULATEINDEX";
    public static final String WINDOW = "WINDOW";
    public static final String IMPORT = "IMPORT";

    private TestConnection testConn;
    private PreparedStatement countStatementPs;
    private PreparedStatement countOperationPs;
    private CallableStatement disableXPlainTracePs;
    private CallableStatement enableXPlainTracePs;

    public SpliceXPlainTrace () {
    }

    private void waitForStatement(long statementId) throws Exception{
        System.out.println(statementId);
        if(countStatementPs==null){
            countStatementPs = testConn.prepareStatement("select count(*) from " + STATEMENT_TABLE + " where statementid = ?");
        }
        countStatementPs.setLong(1,statementId);
        long count = testConn.getCount(countStatementPs);
        // Wait util statementHistory table is populated
        while (count == 0) {
            Thread.sleep(200);
            count = testConn.getCount(countStatementPs);
        }

        // Wait until taskHistory table is populated
        long count_before = -1;
        if(countOperationPs==null)
            countOperationPs = testConn.prepareStatement("select count(*) from " + TASK_TABLE + " where statementid = ?");

        countOperationPs.setLong(1,statementId);
        count = testConn.getCount(countOperationPs);
        while (count_before != count || count == -1) {
            count_before = count;
            Thread.sleep(200);
            count = testConn.getCount(countOperationPs);
        }
    }

    public XPlainTreeNode getOperationTree(long statementId) throws Exception{
        assert statementId!=-1l: "No statement id found";
        setStatementId(statementId);
        setFormat("tree");
        setMode(0);

        return getTopOperation();

    }

    // Turn on xplain trace
    public void turnOnTrace() throws Exception {
        if(enableXPlainTracePs==null)
            enableXPlainTracePs = testConn.prepareCall("call SYSCS_UTIL.SYSCS_SET_XPLAIN_TRACE(1)");
        enableXPlainTracePs.execute();
    }

    // Turn off xplain trace
    public void turnOffTrace() throws Exception {
        if(disableXPlainTracePs==null)
            disableXPlainTracePs = testConn.prepareCall("call SYSCS_UTIL.SYSCS_SET_XPLAIN_TRACE(0)");
        disableXPlainTracePs.execute();
    }

    public ResultSet executeQuery(String sql) throws Exception{
        return testConn.query(sql);
    }

    public boolean execute(String sql) throws Exception{
        return testConn.execute(sql);
    }

    @Override
    public void setConnection(Connection connection) {
        super.setConnection(connection);
        if(connection instanceof TestConnection)
            this.testConn = (TestConnection)connection;
        else{
            try {
                this.testConn = new TestConnection(connection);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
