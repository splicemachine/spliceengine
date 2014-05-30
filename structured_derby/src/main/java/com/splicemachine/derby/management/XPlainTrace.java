package com.splicemachine.derby.management;

import com.splicemachine.derby.impl.sql.catalog.SpliceXplainUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Deque;

/**
 * Created by jyuan on 5/8/14.
 */

public class XPlainTrace {

    private static final String operationTableName = "SYSXPLAIN_OPERATIONHISTORY";
    private static final String taskTableName = "SYSXPLAIN_TASKHISTORY";

    /* SYSCS_UTIL.XPLAIN_TRACE() parameters */
    private String schemaName;
    private long statementId;
    private int mode;
    private String format;

    /* root node of execution plan */
    private XPlainTreeNode topOperation;

    /* operations are hashed into the map to construct a tree */
    private HashMap<Long, XPlainTreeNode> xPlainTreeNodeMap;

    private Connection connection;
    private int sequenceId;
    private XPlainTracePrinter printer;


    public XPlainTrace(String sName, long sId, int mode, String format) throws SQLException {
        this.schemaName = sName;
        this.statementId = sId;
        xPlainTreeNodeMap = new HashMap<Long, XPlainTreeNode>(10);
        sequenceId = 0;
        connection = SpliceXplainUtils.getDefaultConn();
        this.mode = mode;
        this.format = format;
    }

    public ResultSet getXPlainTraceOutput() throws Exception {

        try {
            if (!populateTreeNodeMap()) return null;

            constructOperationTree();

            populateMetrics();

            aggregateTableScan();

            populateSequenceId(topOperation);

            if (format.toUpperCase().compareTo("TREE") == 0) {
                printer = new XPlainTraceTreePrinter(mode, connection, topOperation);
            }
            else if (format.toUpperCase().compareTo("JSON") == 0) {
                printer = new XPlainTraceJsonPrinter(mode, connection, topOperation);
            }
            else {
                throw new Exception("Wrong value \"" + format + "\" for parameter \"format\"");
            }
            return printer.print();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.close();
        }
    }

    private void aggregateTableScan () throws IllegalAccessException{

        Set<Long> keys = xPlainTreeNodeMap.keySet();
        for (Long key:keys) {
            XPlainTreeNode node = xPlainTreeNodeMap.get(key);

            if (node.isTableScanOperation()) {
                Deque<XPlainTreeNode> children = node.getChildren();
                if (children.size() == 0 ||
                    children.getFirst().getOperationType().compareToIgnoreCase("ScrollInsensitive") != 0) {
                    // We are only interested in table scan that has more than two ScrollInsensitive operations
                    continue;
                }
                HashMap<String, XPlainTreeNode> regionScanMap = new HashMap<String, XPlainTreeNode>();

                XPlainTreeNode first = children.removeFirst();
                for (XPlainTreeNode regionScan:first.getChildren().getFirst().getChildren()) {
                    regionScanMap.put(regionScan.getRegion(), regionScan);
                }
                while (children.size() > 0) {
                    XPlainTreeNode next = children.removeFirst();
                    // Aggregate "InsensitiveScroll"
                    first.aggregate(next);

                    // Aggregate at table scan level
                    first.getChildren().getFirst().aggregate(next.getChildren().getFirst());

                    // Aggregate region scan level
                    for (XPlainTreeNode regionScan:next.getChildren().getFirst().getChildren()) {
                        String region = regionScan.getRegion();
                        XPlainTreeNode n = regionScanMap.get(region);
                        if (n != null ) {
                            n.aggregate(regionScan);
                        }
                        else {
                            n = regionScan;
                        }
                        regionScanMap.put(region, n);
                    }
                }

                XPlainTreeNode parent = xPlainTreeNodeMap.get(node.getParentOperationId());
                parent.getChildren().removeLast();
                parent.getChildren().addLast(first.getChildren().getFirst());

                Set<String> regions = regionScanMap.keySet();
                first.getChildren().getFirst().getChildren().clear();
                for(String region:regions) {
                    first.getChildren().getFirst().addLastChild(regionScanMap.get(region));
                }
            }
        }

    }

    /*
     * Pre-order traverse the execution plan tree, assign a sequence id to each node
     */
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

    private void populateMetrics() throws SQLException, IllegalAccessException{
        ResultSet rs = getTaskHistory();
        while (rs.next()) {
            int index = rs.findColumn("OPERATIONID");
            Long operationId = rs.getLong(index);
            XPlainTreeNode node = xPlainTreeNodeMap.get(operationId);
            if (node != null) {
                // For detailed view of the execution plan, show metrics for each region
                if (mode == 1) {
                    if (node.isTableScanOperation()) {
                        XPlainTreeNode child = new XPlainTreeNode(statementId);
                        child.setOperationType("RegionScan");
                        node.addLastChild(child);
                        node = child;
                    }
                }
                node.setAttributes(rs);
            }
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

    /*
     * Construct an operation tree
     */
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

    /*
     * Read SYSXPLAIN_OPERATIONHISTORY table, create a tree node for each row,
     * hash tree node for operation tree construction
     */
    private boolean populateTreeNodeMap() throws SQLException {
        ResultSet rs = getOperationHistory();
        int count = 0;
        while (rs.next()) {
            count++;
            Long operationId = rs.getLong(1);
            XPlainTreeNode node = new XPlainTreeNode(statementId);
            node.setParentOperationId(rs.getLong(2));
            node.setOperationType(rs.getString(3));
            node.setRightChildOp(rs.getBoolean(4));
            node.setInfo(rs.getString(5));
            xPlainTreeNodeMap.put(operationId, node);
        }
        rs.close();
        return count != 0;
    }

    private ResultSet getOperationHistory() throws SQLException{

        StringBuilder query =
                new StringBuilder("select operationId, parent_operation_id, operation_type, is_right_child_op, info from ");

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
