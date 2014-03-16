package com.splicemachine.si.data.hbase;

import com.splicemachine.utils.CloseableIterator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;
import java.io.IOException;
import java.util.List;

/**
 * Abstraction that makes HBase tables and regions have a uniform interface.
 */
public interface IHTable {
    String getName();
    void close() throws IOException;
    Result get(Get get) throws IOException;
    CloseableIterator<Result> scan(Scan scan) throws IOException;
    void put(Put put) throws IOException;
    void put(Put put, Integer rowLock) throws IOException;
    void put(Put put, boolean durable) throws IOException;
    void put(List<Put> puts) throws IOException;
    OperationStatus[] batchPut(Pair<Mutation, Integer>[] puts) throws IOException;
    boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;
    void delete(Delete delete, Integer rowLock) throws IOException;
    Integer lockRow(byte[] rowKey) throws IOException;
    void unLockRow(Integer lock) throws IOException;
    void startOperation() throws IOException;
    void closeOperation() throws IOException;

		Integer tryLock(byte[] rowKey);
}
