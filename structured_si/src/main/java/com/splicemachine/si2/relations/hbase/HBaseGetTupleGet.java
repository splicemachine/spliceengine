package com.splicemachine.si2.relations.hbase;

import com.splicemachine.si2.relations.api.TupleGet;
import org.apache.hadoop.hbase.client.Get;

public class HBaseGetTupleGet implements TupleGet {
	final Get get;

	public HBaseGetTupleGet(Get get) {
		this.get = get;
	}
}
