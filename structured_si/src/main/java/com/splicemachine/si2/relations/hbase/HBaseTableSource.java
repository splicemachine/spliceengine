package com.splicemachine.si2.relations.hbase;

import org.apache.hadoop.hbase.client.HTable;

public interface HBaseTableSource {
	HTable getTable(String tableName);
}
