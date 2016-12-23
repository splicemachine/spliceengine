/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;


/**
 * Representation of a unique logical data owner.
 *
 * A Partition represents the main storage abstraction in use. It encapsulates all logic for interacting
 * with the underlying storage interface.
 *
 */
public interface Partition<K,Txn,IsolationLevel> extends AutoCloseable{

    String getTableName();

    String getName();

    @Override
    void close() throws IOException;

    /**
     *
     * Attempts to get a single record.
     *
     * @param key
     * @param txn
     * @param isolationLevel
     * @return
     * @throws IOException
     */
    Record get(K key,Txn txn, IsolationLevel isolationLevel) throws IOException;

    Iterator<Record> batchGet(List<K>rowKeys, Txn txn, IsolationLevel isolationLevel) throws IOException;

    RecordScanner openScanner(RecordScan scan, Txn txn, IsolationLevel isolationLevel) throws IOException;

    void write(Record record, Txn txn, IsolationLevel isolationLevel) throws IOException;

    boolean checkAndPut(K key, Record expectedValue, Record put) throws IOException;

    void startOperation() throws IOException;

    void closeOperation() throws IOException;

    Iterator<MutationStatus> writeBatch(Record[] toWrite) throws IOException;

    byte[] getStartKey();

    byte[] getEndKey();

    // JL TODO
    long increment(byte[] rowKey, long amount) throws IOException;

    boolean isClosed();

    boolean isClosing();

    Lock getRowLock(Record record) throws IOException;

    Record getLatest(K key) throws IOException;

    void delete(K key, Txn txn, IsolationLevel isolationLevel) throws IOException;

    void mutate(Record record, Txn txn, IsolationLevel isolationLevel) throws IOException;

    boolean containsKey(K key);

    boolean overlapsKeyRange(K start,K stop);

    void writesRequested(long writeRequests);

    void readsRequested(long readRequests);

    List<Partition> subPartitions();

    List<Partition> subPartitions(boolean refresh);

    PartitionServer owningServer();

    List<Partition> subPartitions(K startRow,K stopRow, boolean refresh);

    PartitionLoad getLoad() throws IOException;

    /**
     * Optional Method: compact the data in storage.
     *
     * If the underlying architecture does not support compaction, then this method should do nothing, rather
     * than throw an error
     */
    void compact() throws IOException;

    /**
     * Optional Method: flush the data in storage.
     *
     * If the underlying architecture does not support flush, then this method should do nothing, rather
     * than throw an error
     */
    void flush() throws IOException;

    /**
     *
     * Return BitSet representing items possibly found in Bloom Filter/In Memory Structure
     *
     * @return
     * @throws IOException
     */
    BitSet getBloomInMemoryCheck(boolean hasConstraintChecker, Record[] records, Lock[] locks) throws IOException;

}
