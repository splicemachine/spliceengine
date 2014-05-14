package com.splicemachine.derby.management;

import com.splicemachine.derby.impl.sql.catalog.SpliceXplainUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.sql.DatabaseMetaData;

/**
 * Created by jyuan on 5/8/14.
 */

public class XPlainTrace {

    private String schemaName;
    private long statementId;
    private static final String operationTableName = "SYSXPLAIN_OPERATIONHISTORY";
    private static final String taskTableName = "SYSXPLAIN_TASKHISTORY";
    private XPlainTreeNode topOperation;
    private HashMap<Long, XPlainTreeNode> xPlainTreeNodeMap;
    private ArrayList<String> columnNames;
    private Connection connection;
    private int sequenceId;
    private XPlainTracePrinter printer;
    private int mode;

    public XPlainTrace(String sName, long sId, int mode) throws SQLException {
        this.schemaName = sName;
        this.statementId = sId;
        xPlainTreeNodeMap = new HashMap<Long, XPlainTreeNode>(10);
        sequenceId = 0;
        connection = SpliceXplainUtils.getDefaultConn();
        this.mode = mode;
    }

    public ResultSet populateTraceTable() throws Exception {

        try {
            populateTreeNodeMap();
            constructOperationTree();
            populateMetrics();
            populateSequenceId(topOperation);
            getTraceTableColumnNames();
            writeToTraceTable();
            printer = new XPlainTraceTreePrinter(mode, schemaName, connection, columnNames);
            return printer.print();

        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.close();
        }
    }

    private void populateSequenceId(XPlainTreeNode operation) {
        operation.setSequenceId(sequenceId);
        if(operation.isRightChildOp()) {
            XPlainTreeNode parent = xPlainTreeNodeMap.get(operation.getParentOperationId());
            parent.setRightChild(sequenceId);
        }
        sequenceId++;
        for (XPlainTreeNode child:operation.getChildren()) {
            populateSequenceId(child);
        }
    }

    private void getTraceTableColumnNames() throws SQLException{
        columnNames = new ArrayList<String>();
        DatabaseMetaData md = connection.getMetaData();
        ResultSet rs = md.getColumns(null, schemaName, "SYSXPLAIN_TRACE", "%");
        while (rs.next()) {
            String name = rs.getString(4);
            columnNames.add(name);
        }
    }

    private void writeToTraceTable() throws IllegalAccessException, SQLException {
        // clear SYSXPLAIN_TRACE table first
        Statement s = connection.createStatement();
        s.executeUpdate("delete from " + schemaName + ".SYSXPLAIN_TRACE");
        writeToTraceTable(topOperation, 0);

    }

    private void writeToTraceTable(XPlainTreeNode operation, int level)
            throws IllegalAccessException, SQLException{
        // write into SYSXPLAIN_TRACE table for the current node
        operation.writeToTraceTable(level, schemaName, columnNames, connection);
        for (XPlainTreeNode child:operation.getChildren()){
            writeToTraceTable(child, level+1);
        }
    }

    private void populateMetrics() throws SQLException, IllegalAccessException{
        ResultSet rs = getTaskHistory();
        while (rs.next()) {
            int index = rs.findColumn("OPERATIONID");
            Long operationId = rs.getLong(index);
            XPlainTreeNode node = xPlainTreeNodeMap.get(operationId);

            //TODO: enable this later
            /*if (node.isTableScanOperation()) {
                XPlainTreeNode child = new XPlainTreeNode(statementId);
                child.setOperationType("RegionScan");
                node.addLastChild(child);
                node = child;
            }*/
            node.setAttributes(rs);
        }

        rs.close();
    }

    private ResultSet getTaskHistory() throws SQLException{

        StringBuilder query = new StringBuilder("select * from ");
        query.append(schemaName)
                .append(".")
                .append(taskTableName)
                .append(" where statementId=")
                .append(statementId);

        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery(query.toString());

        return rs;
    }

    private void constructOperationTree() {
        Set<Map.Entry<Long, XPlainTreeNode>> nodeSet = xPlainTreeNodeMap.entrySet();
        for (Map.Entry<Long, XPlainTreeNode> entry : nodeSet) {
            XPlainTreeNode node = entry.getValue();
            Long parentOperationId = node.getParentOperationId();
            if (parentOperationId == 0) {
                topOperation = node;
            } else {
                XPlainTreeNode parent = xPlainTreeNodeMap.get(parentOperationId);
                if (node.isRightChildOp()) {
                    parent.addLastChild(node);
                } else {
                    parent.addFirstChild(node);
                }
            }
        }
    }

    private void populateTreeNodeMap() throws SQLException {
        Connection conn = SpliceXplainUtils.getDefaultConn();
        ResultSet rs = getOperationHistory();
        while (rs.next()) {
            XPlainTreeNode node = new XPlainTreeNode(statementId);
            Long operationId = rs.getLong(1);
            node.setParentOperationId(rs.getLong(2));
            node.setOperationType(rs.getString(3));
            node.setRightChildOp(rs.getBoolean(4));
            xPlainTreeNodeMap.put(operationId, node);
        }
        rs.close();
        conn.close();
    }

    private ResultSet getOperationHistory() throws SQLException{

        StringBuilder query =
                new StringBuilder("select operationId, parent_operation_id, operation_type, is_right_child_op from ");

        query.append(schemaName)
                .append(".")
                .append(operationTableName)
                .append(" where statementId=")
                .append(statementId);

        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery(query.toString());

        return rs;
    }

}
