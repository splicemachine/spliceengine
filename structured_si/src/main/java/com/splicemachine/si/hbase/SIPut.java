package com.splicemachine.si.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.log4j.Logger;
import com.splicemachine.si.utils.SIConstants;

public class SIPut extends Put {
	private static Logger LOG = Logger.getLogger(SIGet.class);
	protected long startTimestamp;
	@SuppressWarnings("unused")
	private SIPut() {		
		super();
	}
	@SuppressWarnings("unused")
	private SIPut(byte[] row) {
		super(row);
	}

	@SuppressWarnings("unused")
	private SIPut(Put put) {
		super(put);
	}

	public SIPut(byte[] row, long timestamp) {
		super(row,timestamp);
		this.startTimestamp = timestamp;
		init(timestamp);
	}

	@SuppressWarnings("unused")
	private SIPut(byte[] row, RowLock rowLock) {
		super(row,rowLock);
	}

	public SIPut(byte[] row,  long timestamp, RowLock rowLock) {
		super(row, timestamp, rowLock);
		init(timestamp);
	}
	private void init(long timestamp) {
		this.startTimestamp = timestamp;		
		add(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN, startTimestamp, SIConstants.ZERO_BYTE_ARRAY);
	}
	@Override
	public Put add(byte[] family, byte[] qualifier, byte[] value) {
		return super.add(family, qualifier, startTimestamp, value);
	}
	
}
