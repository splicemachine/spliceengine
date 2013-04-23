package com.splicemachine.si.coprocessors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.TransactionConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.si.api.PutLog;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.hbase.HGet;
import com.splicemachine.si.data.hbase.HScan;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.filters.SIFilter;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactorFactoryImpl;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class SIObserver extends BaseRegionObserver {
    private static Logger LOG = Logger.getLogger(SIObserver.class);
    protected HRegion region = null;
    private boolean tableEnvMatch = false;
    private String tableName;
    private final Timer rollforwardTimer = new Timer("rollforward");
    private final  Cache<Long, Set> putLogCache = CacheBuilder.newBuilder().maximumSize(50000).expireAfterWrite(30, TimeUnit.SECONDS).build();
    private PutLog putLog;
    private PutLog filterPutLog;

    @Override
    public void start(final CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "starting %s", SIObserver.class);
        region = ((RegionCoprocessorEnvironment) e).getRegion();
        tableName = ((RegionCoprocessorEnvironment) e).getRegion().getTableDesc().getNameAsString();
        tableEnvMatch = (EnvUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TransactionConstants.TableEnv.USER_TABLE)
                || EnvUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TransactionConstants.TableEnv.USER_INDEX_TABLE)
                || EnvUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TransactionConstants.TableEnv.DERBY_SYS_TABLE))
                && !tableName.equals(HBaseConstants.TEMP_TABLE);
        putLog = makePutLog(10000);
        filterPutLog = makePutLog(0);
        super.start(e);
    }

    private PutLog makePutLog(final int delay) {
        return new PutLog() {
            @Override
            public Set getRows(long transactionId) {
                return putLogCache.getIfPresent(transactionId);
            }

            @Override
            public void removeRows(long transactionId) {
                putLogCache.invalidate(transactionId);
            }

            @Override
            public void setRows(final long transactionId, Set rows) {
                putLogCache.put(transactionId, rows);
                final PutLog putLogRef = this;
                rollforwardTimer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        Transactor transactor = TransactorFactoryImpl.getTransactor();
                        STable sTable = new HbRegion(region);
                        try {
                            transactor.rollForward(sTable, putLogRef, transactionId);
                        } catch (Throwable ex) {
                            // The timer is a separate thread, to keep from killing it we need to handle all exceptions here
                            LOG.warn("Problem while rolling forward", ex);
                        }
                    }
                }, delay);
            }
        };
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "stopping %s", SIObserver.class);
        super.stop(e);
    }

    @Override
    public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
        SpliceLogUtils.trace(LOG, "preGet %s", get);
        if (tableEnvMatch && shouldUseSI(new HGet(get))) {
            Transactor transactor = TransactorFactoryImpl.getTransactor();
            transactor.preProcessRead(new HGet(get));
            assert (get.getMaxVersions() == Integer.MAX_VALUE);
            addSiFilterToGet(e, get);
        }
        super.preGet(e, get, results);
    }

    private void logEvent(String event) {
        LOG.warn("SIObserver " + event + " " + tableEnvMatch + " " + "_" + " " + tableName + " " + Thread.currentThread().getName() + " " + Thread.currentThread().getId());
    }


    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        SpliceLogUtils.trace(LOG, "preScannerOpen %s", scan);
        if (tableEnvMatch && shouldUseSI(new HScan(scan))) {
            Transactor transactor = TransactorFactoryImpl.getTransactor();
            transactor.preProcessRead(new HScan(scan));
            assert (scan.getMaxVersions() == Integer.MAX_VALUE);
            addSiFilterToScan(e, scan);
        }
        return super.preScannerOpen(e, scan, s);
    }

    private boolean shouldUseSI(Object operation) {
        Transactor transactor = TransactorFactoryImpl.getTransactor();
        return transactor.isFilterNeeded(operation);
    }

    private void addSiFilterToGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get) throws IOException {
        Transactor transactor = TransactorFactoryImpl.getTransactor();
        Filter newFilter = makeSiFilter(e, transactor.transactionIdFromOperation(new HGet(get)), get.getFilter());
        get.setFilter(newFilter);
    }

    private void addSiFilterToScan(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan) throws IOException {
        Transactor transactor = TransactorFactoryImpl.getTransactor();
        Filter newFilter = makeSiFilter(e, transactor.transactionIdFromOperation(new HScan(scan)), scan.getFilter());
        scan.setFilter(newFilter);
    }

    private Filter makeSiFilter(ObserverContext<RegionCoprocessorEnvironment> e, TransactionId transactionId, Filter currentFilter) throws IOException {
        Transactor transactor = TransactorFactoryImpl.getTransactor();
        SIFilter siFilter = new SIFilter(transactor, filterPutLog, transactionId);
        Filter newFilter;
        if (currentFilter != null) {
            newFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, currentFilter, siFilter); // Wrap Existing Filters
        } else {
            newFilter = siFilter;
        }
        return newFilter;
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
        if (tableEnvMatch) {
            Transactor transactor = TransactorFactoryImpl.getTransactor();
            STable region = new HbRegion(e.getEnvironment().getRegion());
            boolean processed = transactor.processPut(region, put, putLog);
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
            throw new RuntimeException("Direct deletes are not supported under snapshot isolation. Instead a Put is expected that will set a record level tombstone.");
        } else {
            super.preDelete(e, delete, edit, writeToWAL);
        }
    }
}
