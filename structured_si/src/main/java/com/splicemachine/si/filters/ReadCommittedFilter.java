package com.splicemachine.si.filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

public class ReadCommittedFilter extends FilterBase {
	private static Logger LOG = Logger.getLogger(ReadCommittedFilter.class);
	protected long timestamp;
	protected byte[] currentRow;
	public ReadCommittedFilter(long timestamp) {
		this.timestamp = timestamp;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timestamp);		
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue keyValue) {
		currentRow = keyValue.getRow();

		return super.filterKeyValue(keyValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		timestamp = in.readLong();
	}

}
