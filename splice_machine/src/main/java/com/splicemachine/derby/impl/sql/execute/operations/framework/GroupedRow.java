package com.splicemachine.derby.impl.sql.execute.operations.framework;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class wraps a grouping key and an ExecRow
 * 
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class GroupedRow {
    private ExecRow row;
    private byte[] groupingKey;
    private boolean isDistinct;

    public GroupedRow() { }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public GroupedRow(ExecRow row, byte[] groupingKey){
        this.row = row;
        this.groupingKey = groupingKey;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public GroupedRow(ExecRow row, byte[] groupingKey, boolean isDistinct){
        this.row = row;
        this.groupingKey = groupingKey;
        this.isDistinct = isDistinct;
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

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public byte[] getGroupingKey() {
        return groupingKey;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public void setGroupingKey(byte[] groupingKey) {
        this.groupingKey = groupingKey;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }

	@Override
	public String toString() {
		return "grouping key={" + groupingKey + "}, row={" + row + "}, isDistinct="+isDistinct;
	}
    
}
