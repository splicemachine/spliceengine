package com.splicemachine.si.coprocessors;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.TransactionConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.si.api.TransactorFactory;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.hbase.HGet;
import com.splicemachine.si.data.hbase.HScan;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.TransactorFactoryImpl;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

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
    private Configuration envConfiguration;
    private Transactor transactor;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "starting %s", SIObserver.class);
        this.envConfiguration = e.getConfiguration();
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)e;
        region = rce.getRegion();
        tableName = rce.getRegion().getTableDesc().getNameAsString();
        tableEnvMatch = (EnvUtils.getTableEnv(rce).equals(TransactionConstants.TableEnv.USER_TABLE)
                || EnvUtils.getTableEnv(rce).equals(TransactionConstants.TableEnv.USER_INDEX_TABLE)
                || EnvUtils.getTableEnv(rce).equals(TransactionConstants.TableEnv.DERBY_SYS_TABLE))
                && !tableName.equals(HBaseConstants.TEMP_TABLE);
        transactor= TransactorFactoryImpl.getTransactor(envConfiguration);
        RollForwardAction action = new RollForwardAction() {
            @Override
            public void rollForward(long transactionId, List rowList) throws IOException {
                transactor.rollForward(new HbRegion(region), transactionId, rowList);
            }
        };
        rollForwardQueue = new RollForwardQueue(action, 10000, 10 * S, 5 * 60 * S);
        super.start(e);
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
        Filter newFilter = makeSiFilter(e, transactor.transactionIdFromOperation(new HGet(get)), get.getFilter());
        get.setFilter(newFilter);
    }

    private void addSiFilterToScan(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan) throws IOException {
        Filter newFilter = makeSiFilter(e, transactor.transactionIdFromOperation(new HScan(scan)), scan.getFilter());
        scan.setFilter(newFilter);
    }

    private Filter makeSiFilter(ObserverContext<RegionCoprocessorEnvironment> e, TransactionId transactionId, Filter currentFilter) throws IOException {
        SIFilter siFilter = new SIFilter(transactor, transactionId, rollForwardQueue);
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
            STable region = new HbRegion(e.getEnvironment().getRegion());
            boolean processed = transactor.processPut(region, rollForwardQueue, put);
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

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                      InternalScanner scanner) {
        if (tableEnvMatch) {
            Transactor transactor = TransactorFactoryImpl.getTransactor();
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
