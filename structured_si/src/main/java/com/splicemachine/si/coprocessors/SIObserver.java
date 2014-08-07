package com.splicemachine.si.coprocessors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserver extends BaseRegionObserver {
		private static Logger LOG = Logger.getLogger(SIObserver.class);
//		protected HRegion region;
		private boolean tableEnvMatch = false;
//		private String tableName;
		private static final int S = 1000;

		private TxnOperationFactory txnOperationFactory;
		private TransactionalRegion region;

		@Override
		public void start(CoprocessorEnvironment e) throws IOException {
				SpliceLogUtils.trace(LOG, "starting %s", SIObserver.class);
				tableEnvMatch = doesTableNeedSI(((RegionCoprocessorEnvironment)e).getRegion().getTableDesc().getNameAsString());
        if(tableEnvMatch){
            txnOperationFactory = new SimpleOperationFactory(TransactionStorage.getTxnSupplier());
            region = TransactionalRegions.get(((RegionCoprocessorEnvironment) e).getRegion(), SegmentedRollForward.NOOP_ACTION);
            Tracer.traceRegion(region.getTableName(), ((RegionCoprocessorEnvironment)e).getRegion());
        }
//				readResolver = HTransactorFactory.getReadResolver(region);
//				this.rollForward = NoopRollForward.INSTANCE;
//				RollForwardAction action = HTransactorFactory.getRollForwardQueueFactory().newAction(new HbRegion(region));
//		rollForwardQueue = new ConcurrentRollForwardQueue(action,10000,10000,5*60*S,timedRoller,rollerPool);
//				rollForwardQueue = new SynchronousRollForwardQueue(action,10000,10*S,5*60*S,tableName);
//				RollForwardQueueMap.registerRollForwardQueue(tableName, rollForwardQueue);
				super.start(e);
    }

    public static boolean doesTableNeedSI(String tableName) {
        SpliceConstants.TableEnv tableEnv = EnvUtils.getTableEnv(tableName);
        SpliceLogUtils.trace(LOG,"table %s has Env %s",tableName,tableEnv);
        if(SpliceConstants.TableEnv.ROOT_TABLE.equals(tableEnv)||
                SpliceConstants.TableEnv.META_TABLE.equals(tableEnv)||
                SpliceConstants.TableEnv.TRANSACTION_TABLE.equals(tableEnv)) return false;
        if(SpliceConstants.TEMP_TABLE.equals(tableName)||
                SpliceConstants.TEST_TABLE.equals(tableName)) return false;

        return true;
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
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
//				Txn txn = HTransactorFactory.getClientTransactor().txnFromOp(get,true);
				Txn txn = txnOperationFactory.fromReads(get);
        final Filter newFilter = makeSIFilter(txn, get.getFilter(),
								getPredicateFilter(get),false);
        get.setFilter(newFilter);
    }

    private void addSIFilterToScan(Scan scan) throws IOException {
//				Txn txn = HTransactorFactory.getClientTransactor().txnFromOp(scan,true);
				Txn txn = txnOperationFactory.fromReads(scan);
        final Filter newFilter = makeSIFilter(txn, scan.getFilter(),
								getPredicateFilter(scan),scan.getAttribute(SIConstants.SI_COUNT_STAR) != null);
        scan.setFilter(newFilter);
    }

    private EntryPredicateFilter getPredicateFilter(OperationWithAttributes operation) throws IOException {
        final byte[] serializedPredicateFilter = operation.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL);
        return EntryPredicateFilter.fromBytes(serializedPredicateFilter);
    }

    private Filter makeSIFilter(Txn txn, Filter currentFilter, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException {
				TxnFilter txnFilter = region.packedFilter(txn, predicateFilter, countStar);
				SIFilterPacked siFilter = new SIFilterPacked(txnFilter);

//				final SIFilterPacked siFilter = new SIFilterPacked(txn,
//								readResolver,
//								predicateFilter,
//								HTransactorFactory.getTransactionReadController(),countStar);
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
				if(!tableEnvMatch || put.getAttribute(SIConstants.SI_NEEDED)==null) {
						super.prePut(e,put,edit,writeToWAL);
						return;
				}
				Txn txn = txnOperationFactory.fromWrites(put);
				boolean isDelete = put.getAttribute(SIConstants.SI_DELETE_PUT)!=null;
				byte[] row = put.getRow();
				boolean isSIDataOnly = true;
				//convert the put into a collection of KVPairs
				Map<byte[],Map<byte[],KVPair>> familyMap = Maps.newHashMap();
				Iterable<KeyValue> keyValues = Iterables.concat(put.getFamilyMap().values());
				for(KeyValue kv:keyValues){
						byte[] family = kv.getFamily();
						byte[] column = kv.getQualifier();
						if(!Bytes.equals(column, SIConstants.PACKED_COLUMN_BYTES)) continue; //skip SI columns

						isSIDataOnly = false;
						byte[] value = kv.getValue();
						Map<byte[],KVPair> columnMap = familyMap.get(family);
						if(columnMap==null){
								columnMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
								familyMap.put(family,columnMap);
						}
						columnMap.put(column,new KVPair(row,value,isDelete? KVPair.Type.DELETE: KVPair.Type.INSERT));
				}
				if(isSIDataOnly){
						byte[] family = SpliceConstants.DEFAULT_FAMILY_BYTES;
						byte[] column = SpliceConstants.PACKED_COLUMN_BYTES;
						byte[] value = HConstants.EMPTY_BYTE_ARRAY;
						Map<byte[],KVPair> columnMap = familyMap.get(family);
						if(columnMap==null){
								columnMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
								familyMap.put(family,columnMap);
						}
						columnMap.put(column,new KVPair(row,value,isDelete? KVPair.Type.DELETE: KVPair.Type.EMPTY_COLUMN));
				}
				boolean processed = false;
				for(Map.Entry<byte[],Map<byte[],KVPair>> family:familyMap.entrySet()){
						byte[] fam = family.getKey();
						Map<byte[],KVPair> cols = family.getValue();
						for(Map.Entry<byte[],KVPair> column:cols.entrySet()){
								OperationStatus[] status = region.bulkWrite(txn,fam,column.getKey(),ConstraintChecker.NO_CONSTRAINT, Collections.singleton(column.getValue()));
								switch(status[0].getOperationStatusCode()){
										case NOT_RUN:
												break;
										case BAD_FAMILY:
												throw new NoSuchColumnFamilyException(status[0].getExceptionMsg());
										case SANITY_CHECK_FAILURE:
												throw new IOException("Sanity Check failure:" + status[0].getExceptionMsg());
										case FAILURE:
												throw new IOException(status[0].getExceptionMsg());
										default:
												processed=true;
								}
						}
				}

//						final boolean processed = transactor.processPut(new HbRegion(e.getEnvironment().getRegion()), rollForward, put);
				if (processed) {
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
