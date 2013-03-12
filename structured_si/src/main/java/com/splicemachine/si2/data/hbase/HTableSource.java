package com.splicemachine.si2.data.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;

public interface HTableSource {
    HTableInterface getTable(String tableName);
}
