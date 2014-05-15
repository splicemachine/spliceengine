package com.splicemachine.derby.management;

/**
 * Created by jyuan on 5/9/14.
 */

import com.google.gson.annotations.Expose;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.HashMap;

public class XPlainTreeNode {

    private static final String TABLESCAN = "TableScan";

    private long parentOperationId;
    private String operationType;
    private boolean isRightChildOp;
    private int sequenceId;
    private int rightChild;
    private long statementId;

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

    public void setAttribute(String name, Object value) {

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

}
