package com.splicemachine.si2.relations.hbase;

import com.splicemachine.si2.relations.api.TuplePut;
import org.apache.hadoop.hbase.client.Put;

public class HBaseTuplePut implements TuplePut {
	final Put put;

	public HBaseTuplePut(Put put) {
		this.put = put;
	}
}
