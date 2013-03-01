package com.splicemachine.si2.relations.hbase;

import com.splicemachine.si2.relations.api.Relation;
import org.apache.hadoop.hbase.client.HTable;

public class HBaseRelation implements Relation {
	final HTable table;

	public HBaseRelation(HTable table) {
		this.table = table;
	}
}
