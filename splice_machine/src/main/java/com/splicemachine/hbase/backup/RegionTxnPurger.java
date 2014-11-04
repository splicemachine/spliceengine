package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.RegionScanIterator;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.V1TxnDecoder;
import com.splicemachine.si.impl.region.V2TxnDecoder;
import com.splicemachine.utils.Source;

/**
 * Purges unneeded transactions from a Transaction table region.
 */
public class RegionTxnPurger {
    /*
     * The region in which to access data
     */
    private final HRegion region;

    private final V1TxnDecoder oldTransactionDecoder;
    private final V2TxnDecoder newTransactionDecoder;

    public RegionTxnPurger(HRegion region) {
        this.region = region;
        this.oldTransactionDecoder = V1TxnDecoder.INSTANCE;
        this.newTransactionDecoder = V2TxnDecoder.INSTANCE;
    }

    public Source<DenseTxn> getPostBackupTxns(final long afterTs) throws IOException {
        Scan scan = setupScan();

        RegionScanner baseScanner = region.getScanner(scan);

        final RegionScanner scanner = new BufferedRegionScanner(region, baseScanner, scan, 1024, Metrics.noOpMetricFactory());
        return new RegionScanIterator<DenseTxn>(scanner, new RegionScanIterator.IOFunction<DenseTxn>() {
            @Override
            public DenseTxn apply(@Nullable List<KeyValue> keyValues) throws IOException {
                DenseTxn txn = decode(keyValues);
                switch (txn.getState()) {
                    case ROLLEDBACK:
                        return null;
                    case ACTIVE:
                        return txn;
                }
                if (txn.getBeginTimestamp() > afterTs ||
                        txn.getCommitTimestamp() > afterTs ||
                        txn.getGlobalCommitTimestamp() > afterTs) {
                    return txn;
                }
                return null;
            }
        });

    }

    public void rollbackTransactionsAfter(final long afterTs) throws IOException {
        Source<DenseTxn> source = getPostBackupTxns(afterTs);
        RegionTxnStore store = new RegionTxnStore(region, null, null);
        while (source.hasNext()) {
            DenseTxn txn = source.next();
            store.recordRollback(txn.getTxnId());
        }
        source.close();
    }


    /**
     * **************************************************************************************************************
     */
		/*private helper methods*/

    private Scan setupScan() {
        byte[] startKey = region.getStartKey();
        byte[] stopKey = region.getEndKey();
        Scan scan = new Scan(startKey, stopKey);
        scan.setMaxVersions(1);
        return scan;
    }

    private DenseTxn decode(List<KeyValue> keyValues) throws IOException {
        DenseTxn txn = newTransactionDecoder.decode(keyValues);
        if (txn == null) {
            txn = oldTransactionDecoder.decode(keyValues);
        }

        return txn;
    }
}
