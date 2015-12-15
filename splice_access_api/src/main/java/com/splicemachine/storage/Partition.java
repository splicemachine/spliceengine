package com.splicemachine.storage;

import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.metrics.MetricFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;


/**
 * Abstraction that makes HBase tables and regions have a uniform interface.
 */
public interface Partition<OperationWithAttributes,Delete extends OperationWithAttributes,
        Get extends OperationWithAttributes,
        Put extends OperationWithAttributes,Result,Scan extends OperationWithAttributes> extends AutoCloseable{

    String getName();

    @Override
    void close() throws IOException;

    Result get(Get get) throws IOException;

    /**
     * Get a single row. Generally you'll want to re-use the DataGet object that you create in order
     * to do many reads.
     * @param get the get representing the row to fetch
     * @param previous an object holder for previous fetches. Helps to save objects.
     * @return the row result
     * @throws IOException
     */
    DataResult get(DataGet get,DataResult previous) throws IOException;

    CloseableIterator<Result> scan(Scan scan) throws IOException;

    DataScanner openScanner(DataScan scan) throws IOException;

    DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException;

    void put(Put put) throws IOException;

    void put(Put put,Lock rowLock) throws IOException;

    void put(Put put,boolean durable) throws IOException;

    void put(List<Put> puts) throws IOException;

    void put(DataPut put) throws IOException;

    boolean checkAndPut(byte[] family,byte[] qualifier,byte[] expectedValue,Put put) throws IOException;

    void delete(Delete delete,Lock rowLock) throws IOException;

    void startOperation() throws IOException;

    void closeOperation() throws IOException;

    Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException;

    Lock getLock(byte[] rowKey,boolean waitForLock) throws IOException;

    byte[] getStartKey();

    byte[] getEndKey();
    void increment(byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws IOException;
    boolean isClosed();
    boolean isClosing();

    /**
     * Get the latest value of the Foreign Key Counter specifically.
     *
     * @param key the key for the counter to fetch
     * @param previous a holder object to save storage. If {@code null} is passed in, then a new RowResult
     *                 is created. Otherwise, the previous value is used. This allows us to save on object creation
     *                 when we wish to fetch a lot of these at the same time
     * @return a DataResult containing the latest value of the Foreign Key Counter
     * @throws IOException if something goes wrong
     */
    DataResult getFkCounter(byte[] key,DataResult previous) throws IOException;

    /**
     * Get the latest single value for all data types.
     * <p>
     *     This will fetch one and <em>only</em> one version of each possible cell, but it need not fetch
     *     the <em>same</em> version for all cells--that is, it is possible to get a commit timestamp with version 2 and
     *      a tombstone with version 10 in the same returned result.
     * </p>
     * @param key the row key to use during fetch
     * @param previous a holder object to save storage. If {@code null} is passed in, then a new RowResult
     *                 is created. Otherwise, the previous value is used. This allows us to save on object creation
     *                 when we wish to fetch a lot of these at the same time
     * @return a DataResult containing the latest value of all present cells for the specified key.
     * @throws IOException if something goes wrong
     */
    DataResult getLatest(byte[] key,DataResult previous) throws IOException;

    Lock getRowLock(byte[] key,int keyOff,int keyLen) throws IOException;

    DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory) throws IOException;

    DataResultScanner openResultScanner(DataScan scan) throws IOException;

    DataResult getLatest(byte[] rowKey,byte[] family,DataResult previous) throws IOException;

    void delete(DataDelete delete) throws IOException;

    void mutate(DataMutation put) throws IOException;
}
