package com.splicemachine.si.data.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import com.splicemachine.utils.CloseableIterator;

/**
 * Abstraction that makes HBase tables and regions have a uniform interface.
 */
public interface IHTable {
    String getName();
    void close() throws IOException;
    Result get(Get get) throws IOException;
    CloseableIterator<Result> scan(Scan scan) throws IOException;
    void put(Put put) throws IOException;
    void put(List<Put> puts) throws IOException;
    OperationStatus[] batchPut(Mutation[] puts) throws IOException;
    boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;
    void delete(Delete delete) throws IOException;
    HRegion.RowLock lockRow(byte[] rowKey) throws IOException;
    void unLockRow(HRegion.RowLock lock) throws IOException;
    void startOperation() throws IOException;
    void closeOperation() throws IOException;
	HRegion.RowLock tryLock(byte[] rowKey);
}
