package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.SpliceQuery;
import com.splicemachine.si.impl.functions.Version3DataScanner;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.HCell;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.RegionDataScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Wrapped Scanner
 *
 */
public class SIObserverScanner implements RegionScanner {
    private RegionScanner regionScanner;
    private RegionDataScanner regionDataScanner;
    private SpliceQuery spliceQuery;
    private Version3DataScanner dataScanner;
    private ScannerContext scannerContext;
    public SIObserverScanner(RegionScanner regionScanner, SpliceQuery spliceQuery,
                             Partition source, TxnView txn, TxnSupplier txnSupplier,
                             OperationFactory operationFactory, TxnOperationFactory txnOperationFactory) {
        this(regionScanner,spliceQuery,source,txn,txnSupplier,operationFactory,txnOperationFactory,16);
    }

    public SIObserverScanner(RegionScanner regionScanner, SpliceQuery spliceQuery,
                             Partition source, TxnView txn, TxnSupplier txnSupplier,
                             OperationFactory operationFactory, TxnOperationFactory txnOperationFactory, int batchSize) {
        this.regionScanner = regionScanner;
        this.regionDataScanner = new RegionDataScanner(source,regionScanner, Metrics.noOpMetricFactory());
        dataScanner = new Version3DataScanner(
                regionDataScanner, batchSize,// This seems problematic based on cache size...
                txn,txnSupplier,source,spliceQuery,operationFactory,txnOperationFactory);
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return regionScanner.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() throws IOException {
        return regionScanner.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return dataScanner.reseek(row);
    }

    @Override
    public long getMaxResultSize() {
        return regionScanner.getMaxResultSize();
    }

    @Override
    public long getMvccReadPoint() {
        return regionScanner.getMvccReadPoint();
    }

    @Override
    public int getBatch() {
        return regionScanner.getBatch();
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return getNextResults(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
        //return getNextResults(result);
        this.scannerContext = scannerContext;
        return getNextResults(result);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return getNextResults(results);
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        this.scannerContext = scannerContext;
        return getNextResults(result);
    }

    @Override
    public void close() throws IOException {
        dataScanner.close();
    }

    boolean getNextResults(List<Cell> result) throws IOException {
        List<DataCell> results = dataScanner.next(-1);
        if (results.isEmpty()) {
            return false;
        }
        result.add(((HCell) results.get(0)).unwrapDelegate());
        return true;

    }

}
