package com.splicemachine.si.coprocessors;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.si.api.*;
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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
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
    private RollForwardQueue rollForwardQueue;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "starting %s", SIObserver.class);
        region = ((RegionCoprocessorEnvironment) e).getRegion();
        tableName = ((RegionCoprocessorEnvironment) e).getRegion().getTableDesc().getNameAsString();
        Tracer.traceRegion(tableName, region);
        tableEnvMatch = doesTableNeedSI(region);
		RollForwardAction action = HTransactorFactory.getRollForwardFactory().newAction(new HbRegion(region));
//		rollForwardQueue = new ConcurrentRollForwardQueue(action,10000,10000,5*60*S,timedRoller,rollerPool);
		rollForwardQueue = new SynchronousRollForwardQueue(action,10000,10*S,5*60*S,tableName);
        RollForwardQueueMap.registerRollForwardQueue(tableName, rollForwardQueue);
        super.start(e);
    }

    public static boolean doesTableNeedSI(HRegion region) {
        final String tableName = region.getTableDesc().getNameAsString();
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
            HTransactorFactory.getTransactionReadController().preProcessGet(get);
            assert (get.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToGet(get);
        }
        SpliceLogUtils.trace(LOG, "preGet after %s", get);        
        super.preGet(e, get, results);
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        SpliceLogUtils.trace(LOG, "preScannerOpen %s", scan);
        if (tableEnvMatch && shouldUseSI(scan)) {
            HTransactorFactory.getTransactionReadController().preProcessScan(scan);
            assert (scan.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToScan(scan);
        }
        return super.preScannerOpen(e, scan, s);
    }

    private boolean shouldUseSI(Get get) {
        return HTransactorFactory.getTransactionReadController().isFilterNeededGet(get);
    }

    private boolean shouldUseSI(Scan scan) {
        return HTransactorFactory.getTransactionReadController().isFilterNeededScan(scan);
    }

    private void addSIFilterToGet(Get get) throws IOException {

        final Filter newFilter = makeSIFilter(HTransactorFactory.getClientTransactor().transactionIdFromGet(get), get.getFilter(),
								getPredicateFilter(get),false);
        get.setFilter(newFilter);
    }

    private void addSIFilterToScan(Scan scan) throws IOException {
        final Filter newFilter = makeSIFilter(HTransactorFactory.getClientTransactor().transactionIdFromScan(scan), scan.getFilter(),
								getPredicateFilter(scan),scan.getAttribute(SIConstants.SI_COUNT_STAR) != null);
        scan.setFilter(newFilter);
    }

    private EntryPredicateFilter getPredicateFilter(OperationWithAttributes operation) throws IOException {
        final byte[] serializedPredicateFilter = operation.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL);
        return EntryPredicateFilter.fromBytes(serializedPredicateFilter);
    }

    private Filter makeSIFilter(TransactionId transactionId, Filter currentFilter, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException {
        final SIFilterPacked siFilter = new SIFilterPacked(tableName,
								transactionId, HTransactorFactory.getTransactionManager(),
								rollForwardQueue, predicateFilter,HTransactorFactory.getTransactionReadController(), countStar);
        if (needsCompositeFilter(currentFilter)) {
            return composeFilters(orderFilters(currentFilter, siFilter));
        } else {
            return siFilter;
        }
    }

    private boolean needsCompositeFilter(Filter currentFilter) {
        return currentFilter != null;
    }

    private Filter[] orderFilters(Filter currentFilter, Filter siFilter) {
        if (currentFilter instanceof TransactionalFilter && ((TransactionalFilter) currentFilter).isBeforeSI()) {
            return new Filter[] {currentFilter, siFilter};
        } else {
            return new Filter[] {siFilter, currentFilter};
        }
    }

    private FilterList composeFilters(Filter[] filters) {
        return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters[0], filters[1]);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
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
        if (tableEnvMatch) {
            final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
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
