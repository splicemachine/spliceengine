package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

public interface HTableSource {
    HTableInterface getTable(String tableName) throws IOException;
}
