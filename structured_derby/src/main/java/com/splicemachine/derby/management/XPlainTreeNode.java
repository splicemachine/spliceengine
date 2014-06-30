package com.splicemachine.derby.management;

/**
 * Created by jyuan on 5/9/14.
 */

import com.google.gson.annotations.Expose;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.HashMap;

public class XPlainTreeNode {

    private static final Logger LOG = Logger.getLogger(XPlainTreeNode.class);
    private static final String TABLESCAN = "TableScan";

    private long parentOperationId;
    @Expose private String operationType;
    private boolean isRightChildOp;
    private int sequenceId;
    private int rightChild;
    private long statementId;
    @Expose private String info;

    @Expose private int iterations;

    @Expose private String host;
    @Expose private String region;

    @Expose private long totalWallTime;
    @Expose private long totalUserTime;
    @Expose private long totalCPUTime;

    @Expose private long localScanRows;
    @Expose private long localScanBytes;
    @Expose private long localScanWallTime;
    @Expose private long localScanCPUTime;
    @Expose private long localScanUserTime;

    @Expose private long remoteScanRows;
    @Expose private long remoteScanBytes;
    @Expose private long remoteScanWallTime;
    @Expose private long remoteScanCPUTime;
    @Expose private long remoteScanUserTime;

    @Expose private long remoteGetRows;
    @Expose private long remoteGetBytes;
    @Expose private long remoteGetWallTime;
    @Expose private long remoteGetCPUTime;
    @Expose private long remoteGetUserTime;

    @Expose private long writeRows;
    @Expose private long writeBytes;

    @Expose private long processingWallTime;
    @Expose private long processingCPUTime;
    @Expose private long processingUserTime;

    @Expose private long filteredRows;
    @Expose private long inputRows;
    @Expose private long outputRows;

    @Expose private long writeSleepWallTime;
    @Expose private long writeSleepCPUTime;
    @Expose private long writeSleepUserTime;

    @Expose private long rejectedWriteAttempts;
    @Expose private long retriedWriteAttempts;
    @Expose private long failedWriteAttempts;
    @Expose private long partialWriteFailures;

    @Expose private long writeNetworkWallTime;
    @Expose private long writeNetworkCPUTime;
    @Expose private long writeNetworkUserTime;

    @Expose private long writeThreadWallTime;
    @Expose private long writeThreadCPUTime;
    @Expose private long writeThreadUserTime;

    @Expose private Deque<XPlainTreeNode> children;

    private Field[] fields;
    private HashMap<String, Field> fieldMap;

    public XPlainTreeNode(long statementId) {
        this.statementId = statementId;
        children = new LinkedList<XPlainTreeNode>();
        fields = this.getClass().getDeclaredFields();
        fieldMap = new HashMap<String, Field>(fields.length * 2);

        for (Field field:fields) {
            String name = field.getName().toUpperCase();
            fieldMap.put(name, field);
        }

        iterations = 1;
    }

    public void setParentOperationId(long parentOperationId) {
        this.parentOperationId = parentOperationId;
    }

    public long getParentOperationId() {
        return parentOperationId;
    }

    public void setOperationType (String operationType) {
        this.operationType = operationType;
    }

    public String getOperationType () {
        return operationType;
    }

    public void setRightChildOp(boolean isRightChildOp) {
        this.isRightChildOp = isRightChildOp;
    }

    public boolean isRightChildOp() {
        return isRightChildOp;
    }

    public void addFirstChild(XPlainTreeNode child) {
        children.addFirst(child);
    }

    public void addLastChild(XPlainTreeNode child) {
        children.addLast(child);
    }

    public Deque<XPlainTreeNode> getChildren() {
        return children;
    }

    public void setAttributes(ResultSet rs) throws SQLException, IllegalAccessException{

        for(Field f:fields) {
            String name = f.getName().toUpperCase();
            int index = 0;
            try {
                index = rs.findColumn(name);
            } catch (SQLException e) {
                // ignore SQLException if this is not a column name
            }

            if (index == 0) continue;
            if (isStringField(name)) {
                String s = rs.getString(index);
                f.set(this, s);
            } else {
                Long l = rs.getLong(index);
                f.set(this, l + f.getLong(this));
            }
        }
    }

    private boolean isStringField(String columnName) {
        return (columnName.equals("HOST") ||
                columnName.equals("REGION") ||
                columnName.equals("OPERATIONTYPE"));
    }

    public boolean isTableScanOperation() {
        return operationType.toUpperCase().contains("TABLESCAN");
    }

    public void setStatementId(long statementId) {
        this.statementId = statementId;
    }

    public void setRightChild(int r) {this.rightChild = r;}

    public void setSequenceId(int s) {this.sequenceId = s;}

    public int getSequenceId() {return sequenceId;}

    public void aggregate(XPlainTreeNode other) throws IllegalAccessException{

        HashMap<String, Field> otherFieldMap = other.getFieldMap();

        for (Field f:fields) {
            if (canBeAggregated(f)) {
                String name = f.getName().toUpperCase();
                Field otherField = otherFieldMap.get(name);

                long sum = f.getLong(this) + otherField.getLong(other);
                f.set(this, sum);
            }
        }
        iterations++;
    }

    private boolean canBeAggregated(Field f) {

        String name = f.getName();
        String type = f.getType().getCanonicalName();
        if (type.compareToIgnoreCase("long") != 0) return false;

        return (name.compareToIgnoreCase("parentOperationId") != 0 &&
                name.compareToIgnoreCase("statementId") != 0 &&
                name.compareToIgnoreCase("operationId") != 0);
    }

    public HashMap<String, Field> getFieldMap() {
        return fieldMap;
    }

    public String getRegion() {
        return region;
    }

    public void setInfo(String info) {this.info = info;}

    public String getInfo() {
        return info;
    }
    public boolean hasSubquery() {
        boolean result = false;

        // A ProjectRestrict operation may contain a subquery
        if (operationType.compareToIgnoreCase("ProjectRestrict") == 0) {
            for(XPlainTreeNode n:children) {
                if(n.getInfo().contains("Subquery:")) {
                    return true;
                }
            }
        }
        return result;
    }

    public void aggregateSubquery() throws IllegalAccessException{

        HashMap<String, XPlainTreeNode> nodeMap = new HashMap<String, XPlainTreeNode>();
        while(!children.isEmpty()) {
            XPlainTreeNode c = children.removeFirst();
            String key = c.getInfo();
            XPlainTreeNode node = nodeMap.get(key);
            if (node != null) {
                node.aggregateTree(c);
            }
            else {
                node = c;
            }
            nodeMap.put(key, node);
        }

        for(String key:nodeMap.keySet()) {
            children.add(nodeMap.get(key));
        }
    }

    // Assume that the two trees has the same structure
    public void aggregateTree(XPlainTreeNode other) throws IllegalAccessException{
        aggregate(other);
        if (operationType.compareToIgnoreCase(other.operationType) != 0) {
            SpliceLogUtils.trace(LOG, "operation name not equal");
        }
        for(XPlainTreeNode node:children) {
            XPlainTreeNode otherChild = other.children.removeFirst();
            node.aggregateTree(otherChild);
        }
    }
}
