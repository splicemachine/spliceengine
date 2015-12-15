package com.splicemachine.si.coprocessors;

import com.splicemachine.constants.EnvUtils;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionalFilter;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.BaseOperationFactory;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;
import static com.splicemachine.si.constants.SIConstants.ENTRY_PREDICATE_LABEL;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public abstract class SIBaseObserver extends BaseRegionObserver {
		private static Logger LOG = Logger.getLogger(SIBaseObserver.class);
		protected boolean tableEnvMatch = false;
		protected static final int S = 1000;
		protected TxnOperationFactory txnOperationFactory;
		protected TransactionalRegion region;
		@Override
		public void start(CoprocessorEnvironment e) throws IOException {
				SpliceLogUtils.trace(LOG, "starting %s", SIBaseObserver.class);
				tableEnvMatch = doesTableNeedSI(((RegionCoprocessorEnvironment)e).getRegion().getTableDesc().getTableName());
        if(tableEnvMatch){
            txnOperationFactory = new BaseOperationFactory();
            region = SIDriver.getTransactionalRegion((HRegion) ((RegionCoprocessorEnvironment) e).getRegion());
            Tracer.traceRegion(region.getTableName(), (HRegion) ((RegionCoprocessorEnvironment) e).getRegion());
        }
	    super.start(e);
    }


    public static boolean doesTableNeedSI(TableName tableName) {
        SpliceConstants.TableEnv tableEnv = EnvUtils.getTableEnv(tableName);
        SpliceLogUtils.trace(LOG,"table %s has Env %s",tableName,tableEnv);
        if(SpliceConstants.TableEnv.ROOT_TABLE.equals(tableEnv)||
                SpliceConstants.TableEnv.META_TABLE.equals(tableEnv)||
                SpliceConstants.TableEnv.TRANSACTION_TABLE.equals(tableEnv) ||
                SpliceConstants.TableEnv.HBASE_TABLE.equals(tableEnv)) return false;
        if(SpliceConstants.TEST_TABLE.equals(tableName)) return false;
        return true;
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "stopping %s", SIBaseObserver.class);
        if(region!=null)
            region.close();
        super.stop(e);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,Get get,List<Cell> results) throws IOException{
        SpliceLogUtils.trace(LOG, "preGet %s", get);
        if (tableEnvMatch && shouldUseSI(get)) {
            SIDriver.getTransactionReadController().preProcessGet(get);
            assert (get.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToGet(get);
        }
        SpliceLogUtils.trace(LOG, "preGet after %s", get);
        super.preGetOp(e,get,results);
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        SpliceLogUtils.trace(LOG, "preScannerOpen %s with tableEnvMatch=%s, shouldUseSI=%s", scan, tableEnvMatch, shouldUseSI(scan));
        if (tableEnvMatch && shouldUseSI(scan)) {
            SIDriver.getTransactionReadController().preProcessScan(scan);
            assert (scan.getMaxVersions() == Integer.MAX_VALUE);
            addSIFilterToScan(scan);
        }
        return super.preScannerOpen(e, scan, s);
    }

    private boolean shouldUseSI(Get get) {
        return SIDriver.getTransactionReadController().isFilterNeededGet(get);
    }

    private boolean shouldUseSI(Scan scan) {
        return SIDriver.getTransactionReadController().isFilterNeededScan(scan);
    }

    private void addSIFilterToGet(Get get) throws IOException {
				TxnView txn = txnOperationFactory.fromReads(get);
        final Filter newFilter = makeSIFilter(txn, get.getFilter(),
								getPredicateFilter(get),false);
        get.setFilter(newFilter);
    }

    private void addSIFilterToScan(Scan scan) throws IOException {
				TxnView txn = txnOperationFactory.fromReads(scan);
        final Filter newFilter = makeSIFilter(txn, scan.getFilter(),
								getPredicateFilter(scan),scan.getAttribute(SIConstants.SI_COUNT_STAR) != null);
        scan.setFilter(newFilter);
    }

    private EntryPredicateFilter getPredicateFilter(OperationWithAttributes operation) throws IOException {
        final byte[] serializedPredicateFilter = operation.getAttribute(ENTRY_PREDICATE_LABEL);
        return EntryPredicateFilter.fromBytes(serializedPredicateFilter);
    }

    
    protected abstract Filter makeSIFilter(TxnView txn, Filter currentFilter, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException;

    protected boolean needsCompositeFilter(Filter currentFilter) {
        return currentFilter != null;
    }

    protected Filter[] orderFilters(Filter currentFilter, Filter siFilter) {
        if (currentFilter instanceof TransactionalFilter && ((TransactionalFilter) currentFilter).isBeforeSI()) {
            return new Filter[] {currentFilter, siFilter};
        } else {
            return new Filter[] {siFilter, currentFilter};
        }
    }

		protected FilterList composeFilters(Filter[] filters) {
				return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters[0], filters[1]);
		}

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) {
        if (tableEnvMatch) {
            Tracer.compact();
        }
    }
}
