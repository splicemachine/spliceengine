package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.RegionScanIterator;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.STransactionLib;
import com.splicemachine.si.impl.region.TxnDecoder;
import com.splicemachine.utils.Source;

/**
 * Purges unneeded transactions from a Transaction table region.
 */
public class RegionTxnPurger<TxnInfo,Transaction,Data> {
    /*
     * The region in which to access data
     */
    private final HRegion region;

    private final TxnDecoder<TxnInfo,Transaction, Data, Put, Delete, Get, Scan> oldTransactionDecoder;
    private final TxnDecoder<TxnInfo,Transaction, Data, Put, Delete, Get, Scan> newTransactionDecoder;
    private final SDataLib dataLib;
    private final STransactionLib transactionLib;

    public RegionTxnPurger(HRegion region) {
        this.region = region;
        this.transactionLib = SIFactoryDriver.siFactory.getTransactionLib();
        this.oldTransactionDecoder = transactionLib.getV1TxnDecoder();
        this.newTransactionDecoder = transactionLib.getV2TxnDecoder();
        this.dataLib = SIFactoryDriver.siFactory.getDataLib();
    }

    public Source<Transaction> getPostBackupTxns(final long afterTs) throws IOException {
        Scan scan = setupScan();

        RegionScanner baseScanner = region.getScanner(scan);

        final RegionScanner scanner = new BufferedRegionScanner(region, baseScanner, scan, 1024, Metrics.noOpMetricFactory(),SIFactoryDriver.siFactory.getDataLib() );
        return new RegionScanIterator<>(scanner, new RegionScanIterator.IOFunction<Transaction,Data>() {
            @Override
            public Transaction apply(@Nullable List<Data> keyValues) throws IOException {
            	Transaction txn = decode(keyValues);
                switch (transactionLib.getTransactionState(txn)) {
                    case ROLLEDBACK:
                        return null;
                    case ACTIVE:
                        return txn;
                }
                if (transactionLib.getBeginTimestamp(txn) > afterTs ||
                		transactionLib.getCommitTimestamp(txn) > afterTs ||
                		transactionLib.getGlobalCommitTimestamp(txn) > afterTs) {
                    return txn;
                }
                return null;
            }
        }, dataLib);

    }

    public void rollbackTransactionsAfter(final long afterTs) throws IOException {
        Source<Transaction> source = getPostBackupTxns(afterTs);
        RegionTxnStore store = new RegionTxnStore(region, null, null,dataLib,transactionLib);
        while (source.hasNext()) {
            Transaction txn = source.next();
            store.recordRollback(transactionLib.getTxnId(txn));
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

    private Transaction decode(List<Data> keyValues) throws IOException {
    	Object txn = newTransactionDecoder.decode(dataLib,keyValues);
        if (txn == null) {
            txn = oldTransactionDecoder.decode(dataLib,keyValues);
        }

        return (Transaction)txn;
    }
}
