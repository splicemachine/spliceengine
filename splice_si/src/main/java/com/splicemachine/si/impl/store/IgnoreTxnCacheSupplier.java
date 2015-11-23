package com.splicemachine.si.impl.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.storage.EntryDecoder;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.client.ResultScanner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jyuan on 4/17/15.
 */
public class IgnoreTxnCacheSupplier {
    private ConcurrentLinkedHashMap<String,List<Pair<Long, Long>>> cache;
    private ResultScanner resultScanner;
    private SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
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
        openScanner(tableName);
        try {
            Result r = null;

            while ((r = resultScanner.next()) != null) {
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
            resultScanner.close();
        }
        return ignoreTxnList;
    }

    private void openScanner(String tableName) throws IOException {
        HTable hTable = new HTable(SpliceConstants.config, SpliceConstants.RESTORE_TABLE_NAME);
        Scan scan = new Scan();
        byte[] startRow = MultiFieldEncoder.create(1).encodeNext(tableName).build();
        byte[] stopRow = Bytes.unsignedCopyAndIncrement(startRow);
        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);
        resultScanner = hTable.getScanner(scan);
    }
}
