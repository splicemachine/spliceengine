package com.splicemachine.si2.data.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.Iterator;

public interface IHTableReader {
    HTableInterface open(String tableName) throws IOException;
    void close(HTableInterface table);

    Result get(HTableInterface table, Get get);
    Result get(HRegion table, Get get);
    Iterator scan(HTableInterface table, Scan scan);
}
