package com.splicemachine.si.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;

public class SIScan extends Scan {
	public SIScan() {
		super();
	}
	
	public SIScan(Scan scan) throws IOException {
		super(scan);
	}
	

}
