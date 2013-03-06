package com.splicemachine.si2.data.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.Iterator;

public interface HTableReaderI {
    HTableInterface open(String relationIdentifier);
    void close(HTableInterface table);

    Result get(HTableInterface table, Get get);
    Iterator scan(HTableInterface table, Scan scan);
}
