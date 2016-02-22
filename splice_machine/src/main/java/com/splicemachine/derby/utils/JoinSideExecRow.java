package com.splicemachine.derby.utils;

import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.primitives.Bytes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JoinSideExecRow {
	protected ExecRow row;
	protected JoinSide joinSide;
	protected byte[] hash;
    private byte[] rowKey;

    public JoinSideExecRow (ExecRow row,JoinSide joinSide) {
        this.row = row;
        this.joinSide = joinSide;
    }

	@SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
	public JoinSideExecRow (ExecRow row,JoinSide joinSide, byte[] hash) {
		this.row = row;
		this.joinSide = joinSide;
		this.hash = hash;
	}

	@SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
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

    public boolean isRightSide(){
        return joinSide.ordinal() == JoinSide.RIGHT.ordinal();
    }

	@SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
	public byte[] getHash() {
		return hash;
	}

	@SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
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
		return Bytes.basicByteComparator().compare(this.hash,hash) == 0;
	}

}
