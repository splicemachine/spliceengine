package com.splicemachine.derby.utils;

import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Bytes;

public class JoinSideExecRow {
	protected ExecRow row;
	protected JoinSide joinSide;
	protected byte[] hash;
    private byte[] rowKey;

    public JoinSideExecRow (ExecRow row,JoinSide joinSide) {
        this.row = row;
        this.joinSide = joinSide;
    }

	public JoinSideExecRow (ExecRow row,JoinSide joinSide, byte[] hash) {
		this.row = row;
		this.joinSide = joinSide;
		this.hash = hash;
	}

    public JoinSideExecRow (ExecRow row,JoinSide joinSide, byte[] hash,byte[] rowKey) {
        this.row = row;
        this.joinSide = joinSide;
        this.hash = hash;
        this.rowKey = rowKey;
    }

    public byte[] getRowKey(){
        return rowKey;
    }

	public ExecRow getRow() {
		return row;
	}
	public JoinSide getJoinSide() {
		return joinSide;
	}
	public byte[] getHash() {
		return hash;
	}

    public void setHash(byte[] hash) {
        this.hash = hash;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    @Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("joinSide = ");
		sb.append(joinSide.toString());
		sb.append(" row = ");
		sb.append(row);
		sb.append(" hash = ");
		sb.append(hash);
		return sb.toString();
	}
	
	public boolean sameHash(byte[] hash) {
		if (hash == null)
			return false;
		return Bytes.compareTo(this.hash, hash) == 0;
	}

}
