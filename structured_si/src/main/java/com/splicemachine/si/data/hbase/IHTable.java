package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Abstraction that makes HBase tables and regions have a uniform interface.
 */
public interface IHTable {
    void close() throws IOException;
    Result get(Get get) throws IOException;
    Iterator<Result> scan(Scan scan) throws IOException;

    void put(Put put) throws IOException;
    void put(Put put, HRowLock rowLock) throws IOException;
    void put(Put put, boolean durable) throws IOException;
    void put(List<Put> puts) throws IOException;

    boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;

    void delete(Delete delete, HRowLock rowLock) throws IOException;

    HRowLock lockRow(byte[] rowKey) throws IOException;
    void unLockRow(HRowLock lock) throws IOException;
}
