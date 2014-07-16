package com.splicemachine.derby.test.framework;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.management.XPlainTrace;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by jyuan on 7/14/14.
 */
public class SpliceXPlainTrace extends XPlainTrace{

    private static final String STATEMENT_TABLE = "SYS.SYSSTATEMENTHISTORY";
    private static final String TASK_TABLE = "SYS.SYSTASKHISTORY";

    public static final String SCROLLINSENSITIVE = "ScrollInsensitive";
    public static final String TABLESCAN = "TableScan";
    public static final String BULKTABLESCAN = "BulkTableScan";
    public static final String PROJECTRESTRICT = "ProjectRestrict";
    public static final String SCALARAGGREGATE = "ScalarAggregate";
    public static final String NESTEDLOOPJOIN = "NestedLoopJoin";
    public static final String MERGESORTJOIN = "MergeSortJoin";
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
    public static final String POPULATEINDEX = "";
    private SpliceWatcher methodWatcher;
    private Connection connection = null;
    private Statement statement = null;

    public SpliceXPlainTrace () {
        methodWatcher = new SpliceWatcher();
    }

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
        while (count_before != count || count == -1) {
            count_before = count;
            Thread.sleep(100);
            ResultSet rs = methodWatcher.executeQuery(stmt);
            if (rs.next()) {
                count = rs.getInt(1);
            }
        }
    }

    public XPlainTreeNode getOperationTree() throws Exception{

        long statementId = 0;
        ResultSet rs = executeQuery("call SYSCS_UTIL.SYSCS_GET_XPLAIN_STATEMENTID()");
        if (rs.next()) {
            statementId = rs.getLong(1);
        }
        waitForStatement(statementId);

        SpliceXPlainTrace xPlainTrace = new SpliceXPlainTrace();
        xPlainTrace.setStatementId(statementId);
        xPlainTrace.setFormat("tree");
        xPlainTrace.setMode(0);
        xPlainTrace.setConnection(connection);

        XPlainTreeNode operation = xPlainTrace.getTopOperation();
        return operation;

    }

    // Turn on xplain trace
    public void turnOnTrace() throws Exception {
        connection = SpliceNetConnection.getConnection();
        statement = connection.createStatement();
        statement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        statement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
    }

    // Turn off xplain trace
    public void turnOffTrace() throws Exception {
        statement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        statement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");
    }

    public ResultSet executeQuery(String sql) throws Exception{
        ResultSet rs = statement.executeQuery(sql);
        return rs;
    }

    public boolean execute(String sql) throws Exception{
        boolean success = statement.execute(sql);
        return success;
    }
}
