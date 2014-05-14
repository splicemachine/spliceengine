package com.splicemachine.derby.management;

/**
 * Created by jyuan on 5/9/14.
 */

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class XPlainTreeNode {

    private static final String TABLESCAN = "TableScan";

    private long parentOperationId;
    private String operationType;
    private boolean isRightChildOp;
    private int sequenceId;
    private int rightChild;
    private String host;
    private String region;
    private long statementId;
    private long totalWallTime;
    private long totalUserTime;
    private long totalCPUTime;

    private long localScanRows;
    private long localScanBytes;
    private long localScanWallTime;
    private long localScanCPUTime;
    private long localScanUserTime;

    private long remoteScanRows;
    private long remoteScanBytes;
    private long remoteScanWallTime;
    private long remoteScanCPUTime;
    private long remoteScanUserTime;

    private long remoteGetRows;
    private long remoteGetBytes;
    private long remoteGetWallTime;
    private long remoteGetCPUTime;
    private long remoteGetUserTime;

    private long writeRows;
    private long writeBytes;

    private long processingWallTime;
    private long processingCPUTime;
    private long processingUserTime;

    private long filteredRows;
    private long inputRows;
    private long outputRows;

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

    private long writeThreadWallTime;
    private long writeThreadCPUTime;
    private long writeThreadUserTime;

    private Deque<XPlainTreeNode> children;

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

    public void writeToTraceTable(int level, String schemaName, ArrayList<String> columnNames, Connection connection)
            throws IllegalAccessException, SQLException {
        StringBuilder insert = new StringBuilder("insert into ").append(schemaName).append(".SYSXPLAIN_TRACE (level");
        StringBuilder values = new StringBuilder(" values (").append(level);

        for (String columnName:columnNames) {

            Field field = fieldMap.get(columnName.toUpperCase());
            if (field != null) {
                insert.append(",");
                values.append(",");
                insert.append(columnName);
                if (isStringField(columnName.toUpperCase())) {
                    values.append("'").append((String)field.get(this)).append("'");
                } else {
                    values.append(field.getLong(this));
                }
            }
        }
        insert.append(")");
        values.append(")");

        String execSQL = insert.toString() + " " + values.toString();
        PreparedStatement stmt = connection.prepareStatement(execSQL);
        stmt.execute();
    }

    public void setRightChild(int r) {this.rightChild = r;}

    public void setSequenceId(int s) {this.sequenceId = s;}
}
