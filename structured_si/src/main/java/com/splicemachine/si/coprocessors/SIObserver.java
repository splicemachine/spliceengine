package com.splicemachine.si.coprocessors;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.HHasher;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.*;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserver extends BaseRegionObserver {
    private static Logger LOG = Logger.getLogger(SIObserver.class);
    protected HRegion region;
    private boolean tableEnvMatch = false;
    private String tableName;
    private static final int S = 1000;
    private RollForwardQueue<byte[], ByteBuffer> rollForwardQueue;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "starting %s", SIObserver.class);
        region = ((RegionCoprocessorEnvironment) e).getRegion();
        tableName = ((RegionCoprocessorEnvironment) e).getRegion().getTableDesc().getNameAsString();
        Tracer.traceRegion(tableName, region);
        tableEnvMatch = doesTableNeedSI(region);
        RollForwardAction<byte[]> action = new RollForwardAction<byte[]>() {
            @Override
            public Boolean rollForward(long transactionId, List<byte[]> rowList) throws IOException {
                Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
                return transactor.rollForward(new HbRegion(region), transactionId, rowList);
            }
        };
        rollForwardQueue = new SynchronousRollForwardQueue<byte[], ByteBuffer>(new HHasher(), action, 10000, 10 * S, 5 * 60 * S, tableName);
//        rollForwardQueue = RollForwardQueueMap.registerRegion(tableName,new HHasher(),action);
        RollForwardQueueMap.registerRollForwardQueue(tableName, rollForwardQueue);
        super.start(e);
    }

    public static boolean doesTableNeedSI(HRegion region) {
        final String tableName= region.getTableDesc().getNameAsString();
        return (EnvUtils.getTableEnv(tableName).equals(SpliceConstants.TableEnv.USER_TABLE)
                || EnvUtils.getTableEnv(tableName).equals(SpliceConstants.TableEnv.USER_INDEX_TABLE)
                || EnvUtils.getTableEnv(tableName).equals(SpliceConstants.TableEnv.DERBY_SYS_TABLE))
                && !tableName.equals(SpliceConstants.TEMP_TABLE)
                && !tableName.equals(SpliceConstants.TEST_TABLE);
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        RollForwardQueueMap.deregisterRegion(tableName);
        SpliceLogUtils.trace(LOG, "stopping %s", SIObserver.class);
        super.stop(e);
    }

    @Override
    public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
        SpliceLogUtils.trace(LOG, "preGet %s", get);
        if (tableEnvMatch && shouldUseSI(get)) {
            final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
            transactor.preProcessGet(get);
            assert (get.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToGet(e, get);
        }
        super.preGet(e, get, results);
    }

    private void logEvent(String event) {
        LOG.warn("SIObserver " + event + " " + tableEnvMatch + " " + "_" + " " + tableName + " " + Thread.currentThread().getName() + " " + Thread.currentThread().getId());
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        SpliceLogUtils.trace(LOG, "preScannerOpen %s", scan);
        if (tableEnvMatch && shouldUseSI(scan)) {
            final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
            transactor.preProcessScan(scan);
            assert (scan.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToScan(e, scan);
        }
        return super.preScannerOpen(e, scan, s);
    }

    private boolean shouldUseSI(Get get) {
        final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
        return transactor.isFilterNeededGet(get);
    }

    private boolean shouldUseSI(Scan scan) {
        final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
        return transactor.isFilterNeededScan(scan);
    }

    private void addSIFilterToGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get) throws IOException {
        final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();

        final Filter newFilter = makeSIFilter(e, transactor.transactionIdFromGet(get), get.getFilter(),
                getPredicateFilter(get),
                transactor.isGetIncludeSIColumn(get), false);
        get.setFilter(newFilter);
    }

    private void addSIFilterToScan(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan) throws IOException {
        final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();

        final Filter newFilter = makeSIFilter(e, transactor.transactionIdFromScan(scan), scan.getFilter(),
                getPredicateFilter(scan),
                transactor.isScanIncludeSIColumn(scan), transactor.isScanIncludeUncommittedAsOfStart(scan));
        scan.setFilter(newFilter);
    }

    private EntryPredicateFilter getPredicateFilter(OperationWithAttributes operation) throws IOException {
        final byte[] serializedPredicateFilter = operation.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL);
        return EntryPredicateFilter.fromBytes(serializedPredicateFilter);
//        try {
//            return (EntryPredicateFilter) new ByteDataInput(serializedPredicateFilter).readObject();
//        } catch (ClassNotFoundException ex) {
//            throw new IOException(ex);
//        }

    }

    private Filter makeSIFilter(ObserverContext<RegionCoprocessorEnvironment> e, TransactionId transactionId,
                                Filter currentFilter, EntryPredicateFilter predicateFilter, boolean includeSIColumn, boolean includeUncommittedAsOfStart) throws IOException {
        final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
        final SIFilterPacked siFilter = new SIFilterPacked(tableName, transactor, transactionId, rollForwardQueue, predicateFilter,
                includeSIColumn, includeUncommittedAsOfStart);
        Filter newFilter;
        if (currentFilter != null) {
            newFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, siFilter, currentFilter); // Wrap Existing Filters
        } else {
            newFilter = siFilter;
        }
        return newFilter;
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
        if (tableEnvMatch) {
            final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
            final boolean processed = transactor.processPut(new HbRegion(e.getEnvironment().getRegion()), rollForwardQueue, put);
            if (processed) {
                e.bypass();
                e.complete();
            }
        }
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                          boolean writeToWAL) throws IOException {
        if (tableEnvMatch) {
            if (delete.getAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME) == null) {
                throw new RuntimeException("Direct deletes are not supported under snapshot isolation. Instead a Put is expected that will set a record level tombstone.");
            }
        }
        super.preDelete(e, delete, edit, writeToWAL);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                      InternalScanner scanner) throws IOException {
        if (tableEnvMatch && Bytes.compareTo(store.getFamily().getName(), SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES) == 0) {
            final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
            return new SICompactionScanner(transactor, scanner);
        } else {
            return super.preCompact(e, store, scanner);
        }
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) {
        if (tableEnvMatch) {
            Tracer.compact();
        }
    }
}
