package com.splicemachine.si.filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SIFilter extends FilterBase {
	private static Logger LOG = Logger.getLogger(SIFilter.class);
	protected long startTimestamp;
	protected byte[] currentRow;
	
	public SIFilter() {
		
	}
	
	public SIFilter(long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(startTimestamp);		
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue keyValue) {
		SpliceLogUtils.trace(LOG, "filterKeyValue %s",keyValue);
		currentRow = keyValue.getRow();
		return super.filterKeyValue(keyValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		startTimestamp = in.readLong();
	}

}
