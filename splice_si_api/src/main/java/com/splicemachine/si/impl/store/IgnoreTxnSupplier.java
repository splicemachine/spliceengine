
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

package com.splicemachine.si.impl.store;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.storage.*;
import com.splicemachine.utils.Pair;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by jyuan on 10/30/17.
 */
public class IgnoreTxnSupplier {
    private Set<Pair<Long, Long>> cache;
    private EntryDecoder entryDecoder;
    final private PartitionFactory partitionFactory;
    final private TxnOperationFactory txnOperationFactory;
    private boolean ignoreTxnTableExists;
    private volatile boolean initialized = false;

    public IgnoreTxnSupplier(PartitionFactory partitionFactory, TxnOperationFactory txnOperationFactory) {
        this.partitionFactory = partitionFactory;
        this.txnOperationFactory = txnOperationFactory;
        cache = new HashSet<>();
    }

    public boolean shouldIgnore(Long txnId) throws IOException{

        boolean ignore = false;

        init();
        
        if (!cache.isEmpty()) {
            for (Pair<Long, Long> range : cache) {
                if (txnId > range.getFirst() && txnId < range.getSecond()) {
                    ignore = true;
                    break;
                }
            }
        }

        return ignore;
    }


    private void populateIgnoreTxnCache() throws IOException {

        PartitionAdmin admin = partitionFactory.getAdmin();
        ignoreTxnTableExists = admin.tableExists(HBaseConfiguration.IGNORE_TXN_TABLE_NAME);
        if (ignoreTxnTableExists) {
            if (entryDecoder == null)
                entryDecoder = new EntryDecoder();
            try (Partition table = partitionFactory.getTable(HBaseConfiguration.IGNORE_TXN_TABLE_NAME)){
                try (DataResultScanner scanner = openScanner(table)){
                    DataResult r = null;
                    while ((r = scanner.next()) != null) {
                        DataCell cell = r.userData();
                        byte[] buffer = cell.valueArray();
                        int offset = cell.valueOffset();
                        int length = cell.valueLength();
                        entryDecoder.set(buffer, offset, length);
                        MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                        long startTxnId = decoder.decodeNextLong();
                        long endTxnId = decoder.decodeNextLong();
                        cache.add(new Pair<Long, Long>(startTxnId, endTxnId));
                    }
                }
            }
        }
    }

    private DataResultScanner openScanner(Partition table) throws IOException {
        DataScan scan = txnOperationFactory.newDataScan(null);
        return table.openResultScanner(scan);
    }

    private void init() throws IOException {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    populateIgnoreTxnCache();
                    initialized = true;
                }
            }
        }
    }
}
