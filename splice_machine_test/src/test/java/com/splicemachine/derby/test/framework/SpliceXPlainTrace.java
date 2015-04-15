package com.splicemachine.derby.test.framework;

import com.splicemachine.derby.management.XPlainTrace;
import com.splicemachine.derby.management.XPlainTreeNode;

import java.sql.*;

/**
 * Created by jyuan on 7/14/14.
 */
public class SpliceXPlainTrace extends XPlainTrace{

    public static final String TABLESCAN = "TableScan";
    public static final String BULKTABLESCAN = "BulkTableScan";
    public static final String PROJECTRESTRICT = "ProjectRestrict";
    public static final String SCALARAGGREGATE = "ScalarAggregate";
    public static final String NESTEDLOOPJOIN = "NestedLoopJoin";
    public static final String MERGESORTJOIN = "MergeSortJoin";
    public static final String MERGEJOIN = "MergeJoin";
    public static final String BROADCASTJOIN = "BroadcastJoin";
    public static final String INDEXROWTOBASEROW = "IndexLookup";
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
    private CallableStatement disableXPlainTracePs;
    private CallableStatement enableXPlainTracePs;

    public SpliceXPlainTrace () {
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
