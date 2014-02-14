package com.splicemachine.si.coprocessors;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.hbase.KVPair;
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
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;

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
    public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
        SpliceLogUtils.trace(LOG, "preGet %s", get);
        if (tableEnvMatch && shouldUseSI(get)) {
            final Transactor<IHTable,Mutation, Put> transactor = HTransactorFactory.getTransactor();
            HTransactorFactory.getTransactionReadController().preProcessGet(get);
            assert (get.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToGet(e, get);
        }
        super.preGet(e, get, results);
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

        final Filter newFilter = makeSIFilter(e, HTransactorFactory.getClientTransactor().transactionIdFromGet(get), get.getFilter(),
								HTransactorFactory.getTransactionReadController().isGetIncludeSIColumn(get));
        get.setFilter(newFilter);
    }

    private void addSIFilterToScan(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan) throws IOException {
        final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();

        final Filter newFilter = makeSIFilter(e, HTransactorFactory.getClientTransactor().transactionIdFromScan(scan), scan.getFilter(),
								HTransactorFactory.getTransactionReadController().isScanIncludeSIColumn(scan));
        scan.setFilter(newFilter);
    }

    private Filter makeSIFilter(ObserverContext<RegionCoprocessorEnvironment> e, TransactionId transactionId,
                                Filter currentFilter, boolean includeSIColumn) throws IOException {
        final SIFilter siFilter = new SIFilter(HTransactorFactory.getTransactionReadController(),transactionId, HTransactorFactory.getTransactionManager(), rollForwardQueue, includeSIColumn);
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
						final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
						if(!HTransactorFactory.getClientTransactor().requiresSI(put)){
								return; //not managed by SI, ignore it
						}
						TransactionId transactionId = HTransactorFactory.getClientTransactor().transactionIdFromPut(put);
						/*
						 * We are given a single Put. This means we are modifying a single row at a time. This means
						 * that there is one KVPair for each column to be added (no more). Thus, we construct a map
						 * from column family to a map of column qualifier to KVPair for the actual data, then
						 * insert them one at a time. This is very expensive because of the extra WAL edits, but we won't deal
						 * with that situation.
						 */
						//convert Put into many KVPairs, then initiate them one at a time
						HbRegion region = new HbRegion(e.getEnvironment().getRegion());
						Map<byte[],List<KeyValue>> familyMap = put.getFamilyMap();
						for(Map.Entry<byte[],List<KeyValue>> familyPair:familyMap.entrySet()){
								byte[] family = familyPair.getKey();
								List<KeyValue> kvs = familyPair.getValue();
								for(KeyValue kv:kvs){
										byte[] qualifier = kv.getQualifier();
										byte[] value = kv.getValue();
										KVPair newKVPair = new KVPair(put.getRow(),value);

										OperationStatus[] statuses = transactor.processKvBatch(region,rollForwardQueue, transactionId, family, qualifier, Collections.singletonList(newKVPair));
										switch(statuses[0].getOperationStatusCode()){
												case BAD_FAMILY:
														throw new NoSuchColumnFamilyException(Bytes.toString(family));
												case SANITY_CHECK_FAILURE:
												case NOT_RUN:
												case FAILURE:
														throw new IOException("Operation did not succeed");
										}
								}
						}
						e.bypass();
						e.complete();
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
