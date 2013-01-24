package com.splicemachine.derby.utils;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;

public class JoinSideExecRow {
	protected ExecRow row;
	protected JoinSide joinSide;
	protected byte[] hash;
	public JoinSideExecRow (ExecRow row,JoinSide joinSide, byte[] hash) {
		this.row = row;
		this.joinSide = joinSide;
		this.hash = hash;
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
