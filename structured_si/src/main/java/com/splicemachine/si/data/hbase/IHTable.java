package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.STable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public interface IHTable extends STable {
    void close() throws IOException;
    Result get(HGet get) throws IOException;
    Iterator<Result> scan(HScan scan) throws IOException;

    void put(Put put) throws IOException;
    void put(Put put, HRowLock rowLock) throws IOException;
    void put(Put put, boolean durable) throws IOException;
    void put(List<Put> puts) throws IOException;

    boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;

    void delete(Delete delete, HRowLock rowLock) throws IOException;

    HRowLock lockRow(byte[] rowKey) throws IOException;
    void unLockRow(HRowLock lock) throws IOException;
}
