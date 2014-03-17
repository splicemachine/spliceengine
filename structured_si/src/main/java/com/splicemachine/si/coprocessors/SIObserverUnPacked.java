package com.splicemachine.si.coprocessors;

import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.SynchronousRollForwardQueue;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserverUnPacked extends BaseRegionObserver {
    private static Logger LOG = Logger.getLogger(SIObserverUnPacked.class);
    protected HRegion region;
    private boolean tableEnvMatch = false;
    private String tableName;
    private static final int S = 1000;
    private RollForwardQueue rollForwardQueue;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "starting %s", SIObserverUnPacked.class);
        region = ((RegionCoprocessorEnvironment) e).getRegion();
        tableName = ((RegionCoprocessorEnvironment) e).getRegion().getTableDesc().getNameAsString();
        Tracer.traceRegion(tableName, region);
        tableEnvMatch = doesTableNeedSI(region);

				RollForwardAction action = HTransactorFactory.getRollForwardFactory().newAction(new HbRegion(region));
//        RollForwardAction<byte[]> action = new RollForwardAction<byte[]>() {
//            @Override
//            public Boolean rollForward(long transactionId, List<byte[]> rowList) throws IOException {
//                Transactor<IHTable, Mutation,Put, OperationStatus, byte[], ByteBuffer> transactor = HTransactorFactory.getTransactor();
//                return transactor.rollForward(new HbRegion(region), transactionId, rowList);
//            }
//        };
        rollForwardQueue = new SynchronousRollForwardQueue(action, 10000, 10 * S, 5 * 60 * S, tableName);
        RollForwardQueueMap.registerRollForwardQueue(tableName, rollForwardQueue);
        super.start(e);
    }

    public static boolean doesTableNeedSI(HRegion region) {
        final String tableName = region.getTableDesc().getNameAsString();
        return (EnvUtils.getTableEnv(tableName).equals(SpliceConstants.TableEnv.USER_TABLE)
                || EnvUtils.getTableEnv(tableName).equals(SpliceConstants.TableEnv.USER_INDEX_TABLE)
                || EnvUtils.getTableEnv(tableName).equals(SpliceConstants.TableEnv.DERBY_SYS_TABLE))
                && !tableName.equals(SpliceConstants.TEMP_TABLE);
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "stopping %s", SIObserverUnPacked.class);
        super.stop(e);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        SpliceLogUtils.trace(LOG, "preGet %s", get);
        if (tableEnvMatch && shouldUseSI(get)) {
            final Transactor<IHTable,Mutation, Put> transactor = HTransactorFactory.getTransactor();
            HTransactorFactory.getTransactionReadController().preProcessGet(get);
            assert (get.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToGet(e, get);
        }
        super.preGetOp(e, get, results);
    }

    private void logEvent(String event) {
        LOG.warn("SIObserverUnPacked " + event + " " + tableEnvMatch + " " + "_" + " " + tableName + " " + Thread.currentThread().getName() + " " + Thread.currentThread().getId());
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        SpliceLogUtils.trace(LOG, "preScannerOpen %s", scan);
        if (tableEnvMatch && shouldUseSI(scan)) {
            final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
            HTransactorFactory.getTransactionReadController().preProcessScan(scan);
            assert (scan.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToScan(e, scan);
        }
        return super.preScannerOpen(e, scan, s);
    }

    private boolean shouldUseSI(Get get) {
        final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
        return HTransactorFactory.getTransactionReadController().isFilterNeededGet(get);
    }

    private boolean shouldUseSI(Scan scan) {
        final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
        return HTransactorFactory.getTransactionReadController().isFilterNeededScan(scan);
    }

    private void addSIFilterToGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get) throws IOException {
        final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();

        final Filter newFilter = makeSIFilter(e, HTransactorFactory.getClientTransactor().transactionIdFromGet(get), get.getFilter());
        get.setFilter(newFilter);
    }

    private void addSIFilterToScan(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan) throws IOException {
        final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
        final Filter newFilter = makeSIFilter(e, HTransactorFactory.getClientTransactor().transactionIdFromScan(scan), scan.getFilter());
        scan.setFilter(newFilter);
    }

    private Filter makeSIFilter(ObserverContext<RegionCoprocessorEnvironment> e, TransactionId transactionId,
                                Filter currentFilter) throws IOException {
        final SIFilter siFilter = new SIFilter(HTransactorFactory.getTransactionReadController(),transactionId, HTransactorFactory.getTransactionManager(), rollForwardQueue);
        Filter newFilter;
        if (currentFilter != null) {
            newFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, siFilter, currentFilter); // Wrap Existing Filters
        } else {
            newFilter = siFilter;
        }
        return newFilter;
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability writeToWAL) throws IOException {
				/*
				 * This is relatively expensive--it's better to use the write pipeline when you need to load a lot of rows.
				 */
				if (tableEnvMatch) {
						final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
						//TODO -sf- make HbRegion() a constant field
						final boolean processed = transactor.processPut(new HbRegion(e.getEnvironment().getRegion()), rollForwardQueue, put);
						if (processed) {
								e.bypass();
								e.complete();
						}
				}
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                          Durability writeToWAL) throws IOException {
        if (tableEnvMatch) {
            if (delete.getAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME) == null) {
                throw new RuntimeException("Direct deletes are not supported under snapshot isolation. Instead a Put is expected that will set a record level tombstone.");
            }
        }
        super.preDelete(e, delete, edit, writeToWAL);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                      InternalScanner scanner, ScanType scanType, CompactionRequest compactionRequest) throws IOException {
        if (tableEnvMatch) {
            final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
            return new SICompactionScanner(transactor, scanner);
        } else {
            return super.preCompact(e, store, scanner, scanType, compactionRequest);
        }
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) {
        if (tableEnvMatch) {
            Tracer.compact();
        }
    }
}
