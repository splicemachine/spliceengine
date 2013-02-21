package com.splicemachine.si.coprocessor;

public class SIRegion {
	protected byte[] beginKey;
	protected byte[] endKey;
	public SIRegion(byte[] beginKey, byte[] endKey) {
		this.beginKey = beginKey;
		this.endKey = endKey;
	}
	public byte[] getBeginKey() {
		return beginKey;
	}
	public void setBeginKey(byte[] beginKey) {
		this.beginKey = beginKey;
	}
	public byte[] getEndKey() {
		return endKey;
	}
	public void setEndKey(byte[] endKey) {
		this.endKey = endKey;
	}
}
