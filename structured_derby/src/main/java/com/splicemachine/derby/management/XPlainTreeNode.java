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
    @Expose private long operationId;
    @Expose private String operationType;
    private boolean isRightChildOp;
    private int sequenceId;
    private int rightChild;
    @Expose private long statementId;
    @Expose private String info;

    @Expose private long iterations;

    @Expose private String host;
    @Expose private String region;

    @Expose private long totalWallTime;
            private long totalUserTime;
            private long totalCPUTime;

    @Expose private long localScanRows;
            private long localScanBytes;
    @Expose private long localScanWallTime;
            private long localScanCPUTime;
            private long localScanUserTime;

    @Expose private long remoteScanRows;
            private long remoteScanBytes;
    @Expose private long remoteScanWallTime;
            private long remoteScanCPUTime;
            private long remoteScanUserTime;

    @Expose private long remoteGetRows;
            private long remoteGetBytes;
    @Expose private long remoteGetWallTime;
            private long remoteGetCPUTime;
            private long remoteGetUserTime;

    @Expose private long writeRows;
            private long writeBytes;

            private long processingWallTime;
            private long processingCPUTime;
            private long processingUserTime;

            private long filteredRows;
    @Expose private long inputRows;
    @Expose private long outputRows;

            private long writeSleepWallTime;
            private long writeSleepCPUTime;
            private long writeSleepUserTime;

            private long rejectedWriteAttempts;
            private long retriedWriteAttempts;
            private long failedWriteAttempts;
            private long partialWriteFailures;

            private long writeNetworkWallTime;
            private long writeNetworkCPUTime;
            private long writeNetworkUserTime;

    @Expose private long writeTotalWallTime;
            private long writeTotalCPUTime;
            private long writeTotalUserTime;

    @Expose private Deque<XPlainTreeNode> children;

    private Field[] fields;
    private HashMap<String, Field> fieldMap;

    public XPlainTreeNode(long statementId, long operationId) {
        this.statementId = statementId;
        this.operationId = operationId;
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
                if (!isIdField(name)) {
                    Long l = rs.getLong(index);
                    f.set(this, l + f.getLong(this));
                }
            }
        }

        // For operation other than region scan, do not display region information
        if (!operationType.toUpperCase().contains("REGIONSCAN")) {
            region = null;
        }
    }

    private boolean isStringField(String columnName) {
        return (columnName.equals("HOST") ||
                columnName.equals("REGION") ||
                columnName.equals("OPERATIONTYPE"));
    }

    private boolean isIdField(String name) {
        return (name.toUpperCase().equals("OPERATIONID") ||
                name.toUpperCase().equals("STATEMENTID") ||
                name.toUpperCase().equals("PARENTOPERATIONID"));
    }

    public boolean isTableScanOperation() {
        return operationType.toUpperCase().contains("TABLESCAN");
    }

    public boolean isIndexScanOperation() {
        return operationType.toUpperCase().contains("INDEXROWTOBASEROW");
    }

    public boolean isProjectRestrictOperation() {return operationType.toUpperCase().contains("PROJECTRESTRICT");};

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
        //iterations++;
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
            XPlainTreeNode child = nodeMap.get(key);
            if (child.getOperationType().compareToIgnoreCase(XPlainTrace.SCROLLINSENSITIVE) == 0) {
                String info = child.getInfo();
                child = child.getChildren().getFirst();
                child.setInfo(info);
            }
            children.add(child);
        }
    }

    // Assume that the two trees has the same structure
    public void aggregateTree(XPlainTreeNode other) throws IllegalAccessException{
        aggregate(other);
        if (operationType.compareToIgnoreCase(other.operationType) != 0) {
            SpliceLogUtils.trace(LOG, "operation name not equal");
            return;
        }
        for(XPlainTreeNode node:children) {
            XPlainTreeNode otherChild = other.children.removeFirst();
            node.aggregateTree(otherChild);
        }
    }

    public void aggregateLoop() throws IllegalAccessException {
        XPlainTreeNode first = children.removeFirst();
        while(children.size() > 0) {
            XPlainTreeNode next = children.removeFirst();
            first.aggregateTree(next);
        }
        children.add(first);
    }

    public long getLocalScanRows() {
        return localScanRows;
    }

    public long getRemoteScanRows() {
        return remoteScanRows;
    }

    public long getInputRows() {
        return inputRows;
    }

    public long getOutputRows() {
        return outputRows;
    }

    public long getWriteRows() {
        return writeRows;
    }

    public long getIterations() {
        return iterations;
    }

    public void setIterations(long i) {
        this.iterations = i;
    }

    public long getFilteredRows() {
        return filteredRows;
    }

    public long getRemoteGetRows() {
        return remoteGetRows;
    }
}
