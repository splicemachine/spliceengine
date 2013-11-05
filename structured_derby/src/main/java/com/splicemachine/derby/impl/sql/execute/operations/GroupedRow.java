package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
class GroupedRow {
    private ExecRow row;
    private byte[] groupingKey;
    private boolean isDistinct;

    GroupedRow() { }

    public GroupedRow(ExecRow row, byte[] groupingKey){
        this.row = row;
        this.groupingKey = groupingKey;
    }

    public GroupedRow copy(){
        return new GroupedRow(row,groupingKey);
    }

    public GroupedRow deepCopy(){
        ExecRow rowCopy = row.getClone();
        byte[] gKeyCopy = new byte[groupingKey.length];
        System.arraycopy(groupingKey,0,gKeyCopy,0,groupingKey.length);

        return new GroupedRow(rowCopy,gKeyCopy);
    }

    public ExecRow getRow() {
        return row;
    }

    public void setRow(ExecRow row) {
        this.row = row;
    }

    public byte[] getGroupingKey() {
        return groupingKey;
    }

    public void setGroupingKey(byte[] groupingKey) {
        this.groupingKey = groupingKey;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }
}
