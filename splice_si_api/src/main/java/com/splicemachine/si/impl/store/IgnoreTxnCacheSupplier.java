package com.splicemachine.si.impl.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.access.api.STableFactory;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.Partition;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.storage.*;
import com.splicemachine.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Created by jyuan on 4/17/15.
 */
public class IgnoreTxnCacheSupplier<OperationWithAttributes,Data,Delete extends OperationWithAttributes,
        Get extends OperationWithAttributes,
        Put extends OperationWithAttributes,
        RegionScanner,
        Result,
        Scan extends OperationWithAttributes,
        TableInfo> {
    private final ConcurrentLinkedHashMap<String,List<Pair<Long, Long>>> cache;
    private final SDataLib<OperationWithAttributes,Data,Delete, Get, Put,RegionScanner,Result,Scan>  dataLib;
    private final STableFactory<TableInfo> tableFactory;

    private EntryDecoder entryDecoder;

    public IgnoreTxnCacheSupplier(SDataLib<OperationWithAttributes, Data, Delete, Get, Put, RegionScanner, Result, Scan> dataLib,STableFactory<TableInfo> tableFactory) {
        this.dataLib = dataLib;
        this.tableFactory=tableFactory;
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
        try(DataResultScanner ds = openScanner(tableName)){
            DataResult r;
            while ((r = ds.next()) != null) {
                DataCell ud = r.userData();
                byte[] buffer = ud.valueArray();
                int offset = ud.valueOffset();
                int length = ud.valueLength();
                entryDecoder.set(buffer, offset, length);
                MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                decoder.skip(); //skip the string
                long startTxnId = decoder.decodeNextLong();
                long endTxnId = decoder.decodeNextLong();
                ignoreTxnList.add(new Pair<>(startTxnId,endTxnId));
            }
        }
        return ignoreTxnList;
    }

    private DataResultScanner openScanner(String tableName) throws IOException {
        Partition p =tableFactory.getTable(tableName);
        byte[] startRow = MultiFieldEncoder.create(1).encodeNext(tableName).build();
        byte[] stopRow = Bytes.unsignedCopyAndIncrement(startRow);
        DataScan scan = dataLib.newDataScan().startKey(startRow).stopKey(stopRow);
        return p.openResultScanner(scan,Metrics.noOpMetricFactory());
    }
}
