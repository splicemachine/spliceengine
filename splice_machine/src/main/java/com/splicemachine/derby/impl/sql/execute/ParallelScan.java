package com.splicemachine.derby.impl.sql.execute;

import org.apache.hadoop.hbase.client.Scan;

public interface ParallelScan {
	public Scan getScan();
}
