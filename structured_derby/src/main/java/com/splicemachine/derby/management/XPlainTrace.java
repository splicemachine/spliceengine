package com.splicemachine.derby.management;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.impl.sql.catalog.SpliceXplainUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;

/**
 * Created by jyuan on 5/8/14.
 */

public class XPlainTrace {

    private static final String operationTableName = "SYSOPERATIONHISTORY";
    private static final String taskTableName = "SYSTASKHISTORY";
    private static final String PROJECTRESTRICT = "ProjectRestrict";
    public static final String SCROLLINSENSITIVE = "ScrollInsensitive";
    /* SYSCS_UTIL.XPLAIN_TRACE() parameters */
    private static final String SCHEMANAME="SYS";
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


    protected XPlainTrace() {
        xPlainTreeNodeMap = new HashMap<Long, XPlainTreeNode>(10);
        sequenceId = 0;
    }

    public XPlainTrace(long sId, int mode, String format) throws SQLException {
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

            aggregateSubqueries();

            aggregateLoops();

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

    private void aggregateSubqueries() throws IllegalAccessException{

        // Walk through the tree, find all ProjectRestrict node that has a subquery
        NavigableMap<Integer, List<XPlainTreeNode>> subqueryMap = Maps.newTreeMap();
        populateSubQueryMap(topOperation, subqueryMap, 0);

        // Aggregate subqueries
        for(Integer level:subqueryMap.descendingKeySet()) {
            List<XPlainTreeNode> l = subqueryMap.get(level);
            for(XPlainTreeNode node:l) {
                node.aggregateSubquery();
            }
        }
    }

    private void populateSubQueryMap(XPlainTreeNode node, NavigableMap<Integer,
            List<XPlainTreeNode>> subqueryMap, int level) {

        if (node.hasSubquery()) {
            List<XPlainTreeNode> operations = subqueryMap.get(level);
            if (operations == null) {
                operations = Lists.newArrayList();
                subqueryMap.put(level, operations);
            }
            operations.add(node);
        }

        Deque<XPlainTreeNode> children = node.getChildren();
        for(XPlainTreeNode c:children) {
            populateSubQueryMap(c, subqueryMap, level+1);
        }
    }

    private void aggregateLoops() throws IllegalAccessException {
        // Walk through the tree and aggregate broadcast and nested loop join operations
        NavigableMap<Integer, List<XPlainTreeNode>> loopsMap = Maps.newTreeMap();
        populateLoopsMap(topOperation, loopsMap, 0);

        // Aggregate loops
        for(Integer level:loopsMap.descendingKeySet()) {
            List<XPlainTreeNode> l = loopsMap.get(level);
            for(XPlainTreeNode node:l) {
                node.aggregateLoop();
                //remove scrollinsensitive node
                XPlainTreeNode siNode = node.getChildren().getFirst();
                XPlainTreeNode child = siNode.getChildren().getFirst();
                child.setIterations(siNode.getIterations());
                child.setInfo(node.getInfo());
                long parentOperationId = node.getParentOperationId();
                XPlainTreeNode parentNode = xPlainTreeNodeMap.get(parentOperationId);
                parentNode.getChildren().removeLast();
                parentNode.getChildren().add(child);
            }
        }
    }

    private void populateLoopsMap(XPlainTreeNode node, NavigableMap<Integer,
            List<XPlainTreeNode>> loopsMap, int level) {
        if (node.isIndexScanOperation() || node.isTableScanOperation() || node.isProjectRestrictOperation())
        {
            Deque<XPlainTreeNode> children = node.getChildren();
            if (children != null && children.size() > 0 &&
                children.getFirst().getOperationType().compareToIgnoreCase("ScrollInsensitive") == 0) {
                List<XPlainTreeNode> operations = loopsMap.get(level);
                if (operations == null) {
                    operations = Lists.newArrayList();
                    loopsMap.put(level, operations);
                }
                operations.add(node);
            }
        }

        Deque<XPlainTreeNode> children = node.getChildren();
        for(XPlainTreeNode c:children) {
            populateLoopsMap(c, loopsMap, level + 1);
        }
    }

    private void aggregateTableScan () throws IllegalAccessException{

        Set<Long> keys = xPlainTreeNodeMap.keySet();
        for (Long key:keys) {
            XPlainTreeNode node = xPlainTreeNodeMap.get(key);

            if (node.isTableScanOperation() || node.isIndexScanOperation()) {
                Deque<XPlainTreeNode> children = node.getChildren();
                if (children.size() == 0) {
                    continue;
                }
                HashMap<String, XPlainTreeNode> regionScanMap = new HashMap<String, XPlainTreeNode>();

                while (children.size() > 0) {
                    XPlainTreeNode first = children.removeFirst();
                    String region = first.getRegion();
                    XPlainTreeNode n = regionScanMap.get(region);
                    if (n != null ) {
                        n.aggregate(first);
                    }
                    else {
                        n = first;
                    }
                    regionScanMap.put(region, n);

                }

                Set<String> regions = regionScanMap.keySet();
                for(String region:regions) {
                    node.addLastChild(regionScanMap.get(region));
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
        HashMap<String, XPlainTreeNode> regionMap = new HashMap<String, XPlainTreeNode>();
        while (rs.next()) {
            int index = rs.findColumn("OPERATIONID");
            Long operationId = rs.getLong(index);
            XPlainTreeNode node = xPlainTreeNodeMap.get(operationId);
            if (node != null) {
                // For detailed view of the execution plan, show metrics for each region
                if (mode == 1) {
                    if (node.isTableScanOperation()) {
                        XPlainTreeNode child = new XPlainTreeNode(statementId, operationId);
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
        query.append(SCHEMANAME)
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
                if (parent != null) {
                    if (node.isRightChildOp()) {
                        parent.addLastChild(node);
                    } else {
                        parent.addFirstChild(node);
                    }
                }
            }
        }

        // Skip the top level ScrollInsensitive node
        if (topOperation.getOperationType().compareToIgnoreCase(SCROLLINSENSITIVE)==0) {
            Deque<XPlainTreeNode> children = topOperation.getChildren();
            topOperation = children.getFirst();
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
            XPlainTreeNode node = new XPlainTreeNode(statementId, operationId);
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

        query.append(SCHEMANAME)
                .append(".")
                .append(operationTableName)
                .append(" where statementId=")
                .append(statementId);

        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery(query.toString());

        return rs;
    }

    /*
     *  Convenience methods fpr testing purpose
     */
    protected XPlainTreeNode getTopOperation() throws Exception{
        if (topOperation == null) {
            if (!populateTreeNodeMap()) return null;

            constructOperationTree();

            populateMetrics();

            aggregateSubqueries();

            aggregateLoops();

            aggregateTableScan();
        }
        return topOperation;
    }

    protected void setStatementId(long sId) {
        this.statementId = sId;
    }

    protected void setMode (int mode) {
        this.mode = mode;
    }

    protected void setFormat (String format) {
        this.format = format;
    }

    protected void setConnection (Connection connection) {
        this.connection = connection;
    }
}
