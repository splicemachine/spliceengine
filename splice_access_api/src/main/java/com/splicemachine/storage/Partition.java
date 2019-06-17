/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.utils.Pair;
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
 */
public interface Partition extends AutoCloseable{

    String getTableName();

    String getName();

    String getEncodedName();

    @Override
    void close() throws IOException;

    /**
     * Get a single row. Generally you'll want to re-use the DataGet object that you create in order
     * to do many reads.
     * @param get the get representing the row to fetch
     * @param previous an object holder for previous fetches. Helps to save objects.
     * @return the row result
     * @throws IOException
     */
    DataResult get(DataGet get,DataResult previous) throws IOException;

    Iterator<DataResult> batchGet(Attributable attributes,List<byte[]>rowKeys) throws IOException;

    DataScanner openScanner(DataScan scan) throws IOException;

    DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException;

    void put(DataPut put) throws IOException;

    boolean checkAndPut(byte[] key,byte[] family,byte[] qualifier,byte[] expectedValue,DataPut put) throws IOException;

    void startOperation() throws IOException;

    void closeOperation() throws IOException;

    Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException;

    byte[] getStartKey();

    byte[] getEndKey();

    long increment(byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws IOException;

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

    void delete(List<DataDelete> delete) throws IOException;

    void mutate(DataMutation put) throws IOException;

    void batchMutate(List<DataMutation> mutations) throws IOException;

    boolean containsRow(byte[] row);

    boolean containsRow(byte[] row, int offset, int length);

    boolean overlapsRange(byte[] start,byte[] stop);

    boolean overlapsRange(byte[] start,int startOff,int startLen,
                          byte[] stop,int stopOff,int stopLen);

    void writesRequested(long writeRequests);

    void readsRequested(long readRequests);

    List<Partition> subPartitions();

    List<Partition> subPartitions(boolean refresh);

    PartitionServer owningServer();

    List<Partition> subPartitions(byte[] startRow,byte[] stopRow);

    List<Partition> subPartitions(byte[] startRow,byte[] stopRow, boolean refresh);

    PartitionLoad getLoad() throws IOException;

    /**
     * Optional Method: compact the data in storage.
     *
     * If the underlying architecture does not support compaction, then this method should do nothing, rather
     * than throw an error
     */
    void compact(boolean isMajor) throws IOException;

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
    BitSet getBloomInMemoryCheck(boolean hasConstraintChecker, Pair<KVPair, Lock>[] dataAndLocks) throws IOException;

    PartitionDescriptor getDescriptor() throws IOException;

    default boolean grantCreatePrivilege() throws IOException{return false;}

    default boolean revokeCreatePrivilege() throws IOException {
        return false;
    };
}
