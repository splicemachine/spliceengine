package com.splicemachine.si.impl.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.STableReader;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jyuan on 4/17/15.
 */
public class IgnoreTxnCacheSupplier<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,Get extends OperationWithAttributes,
        Put extends OperationWithAttributes,RegionScanner,Result,Scan extends OperationWithAttributes,Table> {
    private ConcurrentLinkedHashMap<String,List<Pair<Long, Long>>> cache;
    private CloseableIterator<Result> iterator;
    private SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan>  dataLib = SIDriver.siFactory.getDataLib();
    private STableReader<Table, Get, Scan,Result> tableReader = SIDriver.siFactory.getTableReader();

    private EntryDecoder entryDecoder;

    public IgnoreTxnCacheSupplier () {
        cache = new ConcurrentLinkedHashMap.Builder<String, List<Pair<Long, Long>>>()
                .maximumWeightedCapacity(1024)
                .concurrencyLevel(64)
                .build();
    }

    public List<Pair<Long, Long>> getIgnoreTxnList(String table) throws IOException{
        List<Pair<Long, Long>> ignoreTxnList = cache.get(table);

        if (ignoreTxnList == null) {
            synchronized (this) {
                ignoreTxnList = cache.get(table);
                if (ignoreTxnList == null) {
                    // It's not in cache yet, load from SPLICE_RESTORE table
                    ignoreTxnList = getIgnoreTxnListFromStore(table);
                }
            }
            cache.put(table, ignoreTxnList);
        }

        return ignoreTxnList;
    }

    private List<Pair<Long, Long>> getIgnoreTxnListFromStore(String tableName) throws IOException {
        List<Pair<Long, Long>> ignoreTxnList = new ArrayList<>();

        if (entryDecoder == null)
            entryDecoder = new EntryDecoder();
        try {
            openScanner(tableName);
            Result r = null;

            while ((r = iterator.next()) != null) {
                byte[] buffer = dataLib.getDataValueBuffer(dataLib.matchDataColumn(r));
                int offset = dataLib.getDataValueOffset(dataLib.matchDataColumn(r));
                int length = dataLib.getDataValuelength(dataLib.matchDataColumn(r));
                entryDecoder.set(buffer, offset, length);
                MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                String item = decoder.decodeNextString();
                long startTxnId = decoder.decodeNextLong();
                long endTxnId = decoder.decodeNextLong();
                ignoreTxnList.add(new Pair<Long, Long>(startTxnId, endTxnId));
            }
        } finally {
            iterator.close();
        }
        return ignoreTxnList;
    }

    private void openScanner(String tableName) throws IOException {
        Table table = tableReader.open(tableName);
        byte[] startRow = MultiFieldEncoder.create(1).encodeNext(tableName).build();
        byte[] stopRow = Bytes.unsignedCopyAndIncrement(startRow);
        Scan scan = dataLib.newScan(startRow,stopRow);
        iterator = tableReader.scan(table, scan);
    }
}
