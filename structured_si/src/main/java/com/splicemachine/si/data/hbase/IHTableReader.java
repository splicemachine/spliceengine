package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.Iterator;

public interface IHTableReader {
    HTableInterface open(String tableName) throws IOException;
    void close(HTableInterface table) throws IOException;

    Result get(HTableInterface table, Get get) throws IOException;
    Result get(HRegion table, Get get) throws IOException;
    Iterator scan(HTableInterface table, Scan scan) throws IOException;
}
