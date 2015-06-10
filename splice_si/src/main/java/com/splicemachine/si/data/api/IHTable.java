package com.splicemachine.si.data.api;

import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;

import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collection;
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
    void put(Put put, SRowLock rowLock) throws IOException;
    void put(Put put, boolean durable) throws IOException;
    void put(List<Put> puts) throws IOException;
    OperationStatus[] batchPut(Pair<Mutation, SRowLock>[] puts) throws IOException;
    boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;
    void delete(Delete delete, SRowLock rowLock) throws IOException;
    SRowLock lockRow(byte[] rowKey) throws IOException;
    void unLockRow(SRowLock lock) throws IOException;
    void startOperation() throws IOException;
    void closeOperation() throws IOException;
    OperationStatus[] batchMutate(Collection<KVPair> data,TxnView txn) throws IOException;
    SRowLock getLock(byte[] rowKey, boolean waitForLock) throws IOException;

    SRowLock tryLock(ByteSlice rowKey) throws IOException;

    void increment(byte[] rowKey, byte[] family, byte[] qualifier, long amount, SRowLock rowLock) throws IOException;

}
