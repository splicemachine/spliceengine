package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.constants.bytes.BytesUtil;

public class ByteArraySlice {
	private byte[] buffer;
	private int offset;
	private int length;

	public ByteArraySlice() {
		
	}
	
	public ByteArraySlice(byte[] data) {
		this(data,0,data==null?0:data.length);
	}

	
	public ByteArraySlice(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;		
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}
	
	public byte[] getBytes() {
		return BytesUtil.slice(buffer, offset, length);
	}	
	
	public void updateSlice(byte[] buffer, int offset, int length) {
		this.buffer = buffer;
		this.offset = offset;
		this.length = length;		
	}
	
}
