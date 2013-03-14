package com.splicemachine.si2.data.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

public interface HTableSource {
    HTableInterface getTable(String tableName) throws IOException;
}
