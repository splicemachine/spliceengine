/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si.api.server;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a "Transactional Region", that is, a region in Hbase which is transactionally aware.
 *
 * @author Scott Fines
 *         Date: 7/1/14
 */
public interface TransactionalRegion<InternalScanner> extends AutoCloseable{

    /**
     * Create a new Transactional Filter for the region.
     *
     * This filter is "Unpacked", in the sense that it will not attempt to deal with packed
     * data.
     *
     * @param txn the transaction to create a filter for
     * @return a new transactional filter for the region
     * @throws IOException if something goes wrong.
     */
    TxnFilter unpackedFilter(TxnView txn, boolean ignoreRecentTransactions) throws IOException;

    TxnFilter packedFilter(TxnView txn, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException;

    //    SICompactionState compactionFilter() throws IOException;

    /**
     * @return true if the underlying region is either closed or is closing
     */
    boolean isClosed();

    boolean rowInRange(byte[] row);

    boolean rowInRange(ByteSlice slice);

    String getTableName();

    void updateWriteRequests(long writeRequests);

    Iterable<MutationStatus> bulkWrite(TxnView txn,
                                       byte[] family, byte[] qualifier,
                                       ConstraintChecker constraintChecker,
                                       Collection<KVPair> data, boolean skipConflictDetection,
                                       boolean skipWAL, boolean rollforward) throws IOException;

    String getRegionName();

    TxnSupplier getTxnSupplier();

    ReadResolver getReadResolver();

    void close();

    InternalScanner compactionScanner(InternalScanner scanner);

    Partition unwrap();
}
