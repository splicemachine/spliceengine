package com.splicemachine.si2.relations.hbase;

import com.splicemachine.si2.relations.api.TupleGet;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseScanTupleGet implements TupleGet {
	final Scan scan;

	public HBaseScanTupleGet(Scan scan) {
		this.scan = scan;
	}
}
