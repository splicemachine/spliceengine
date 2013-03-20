package com.splicemachine.si.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;

import com.splicemachine.si.utils.SIConstants;

public class SIScan extends Scan {
	public SIScan() {
		super();
		setAttribute(SIConstants.SI, SIConstants.EMPTY_BYTE_ARRAY);
	}

	public SIScan(long timestamp) throws IOException {
		super();
		setTimeRange(0l, timestamp);
		setAttribute(SIConstants.SI, SIConstants.EMPTY_BYTE_ARRAY);
	}

	public SIScan(Scan scan, long timestamp) throws IOException {
		super(scan);
		setTimeRange(0l, timestamp);
		setAttribute(SIConstants.SI, SIConstants.EMPTY_BYTE_ARRAY);
	}
	

}
