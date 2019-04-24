package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.AclCheckerService;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionalFilter;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.ExtendedOperationStatus;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.SICompactionState;
import com.splicemachine.si.impl.server.SimpleCompactionContext;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.ipc.RpcUtils;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TableAuthManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Maps;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.splicemachine.si.constants.SIConstants.ENTRY_PREDICATE_LABEL;

/**
 * Created by jyuan on 4/11/19.
 */
public abstract class BaseSIObserver extends BaseRegionObserver {
    private static Logger LOG=Logger.getLogger(BaseSIObserver.class);

    protected boolean tableEnvMatch=false;
    protected boolean spliceTable=false;
    protected long conglomId;
    protected TxnOperationFactory txnOperationFactory;
    protected OperationStatusFactory operationStatusFactory;
    protected TransactionalRegion region;
    protected TableAuthManager authManager = null;
    protected boolean authTokenEnabled;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        try {
            SpliceLogUtils.trace(LOG, "starting %s", SIObserver.class);
            RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) e;
            TableName tableName = HBasePlatformUtils.getTableName(rce);
            doesTableNeedSI(tableName);
            HBaseSIEnvironment env = HBaseSIEnvironment.loadEnvironment(new SystemClock(), ZkUtils.getRecoverableZooKeeper());
            SIDriver driver = env.getSIDriver();
            if (tableEnvMatch) {
                try{
                    conglomId=Long.parseLong(tableName.getQualifierAsString());
                }catch(NumberFormatException nfe){
                    SpliceLogUtils.warn(LOG,"Unable to parse conglomerate id for table %s",tableName);
                    conglomId=-1;
                }
                operationStatusFactory = driver.getOperationStatusLib();
                //noinspection unchecked
                txnOperationFactory = new SimpleTxnOperationFactory(driver.getExceptionFactory(), HOperationFactory.INSTANCE);
                //noinspection unchecked
                Partition regionPartition = new RegionPartition((HRegion) rce.getRegion());
                region = new TxnRegion(regionPartition,
                        driver.getRollForward(),
                        driver.getReadResolver(regionPartition),
                        driver.getTxnSupplier(),
                        driver.getTransactor(),
                        driver.getOperationFactory()
                );
                Tracer.traceRegion(region.getTableName(), rce.getRegion());
            }

            this.authManager = HBasePlatformUtils.getTableAuthManager((RegionCoprocessorEnvironment) e);
            this.authTokenEnabled = driver.getConfiguration().getAuthenticationTokenEnabled();

        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException{
        try {
            SpliceLogUtils.trace(LOG,"stopping %s",SIObserver.class);
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }


    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException{
        checkAccess();

        try {
            SpliceLogUtils.trace(LOG,"preGet %s",get);
            if(tableEnvMatch && shouldUseSI(get)){
                get.setMaxVersions();
                get.setTimeRange(0L,Long.MAX_VALUE);
                assert (get.getMaxVersions()==Integer.MAX_VALUE);
                addSIFilterToGet(get);
            }
            SpliceLogUtils.trace(LOG,"preGet after %s",get);

        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    protected void preScannerOpenAction(Scan scan) throws IOException{
        try {
            SpliceLogUtils.trace(LOG,"preScannerOpen %s with tableEnvMatch=%s, shouldUseSI=%s",scan,tableEnvMatch,shouldUseSI(scan));
            if(tableEnvMatch && shouldUseSI(scan)){
                scan.setMaxVersions();
                scan.setTimeRange(0L,Long.MAX_VALUE);
                assert (scan.getMaxVersions()==Integer.MAX_VALUE);
                addSIFilterToScan(scan);
            }
            if (tableEnvMatch && hasToken(scan)) {
                aclCheck(scan);
            } else {
                checkAccess();
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    protected void postCompactAction() throws IOException {
        try {
            if(tableEnvMatch){
                Tracer.compact();
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    protected void preDeleteAction(ObserverContext<RegionCoprocessorEnvironment> e,Delete delete) throws IOException {
        checkAccess();

        try {
            if(tableEnvMatch){
                if(delete.getAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null){
                    TableName tableName=HBasePlatformUtils.getTableName(e.getEnvironment());
                    String message="Direct deletes are not supported under snapshot isolation. "+
                            "Instead a Put is expected that will set a record level tombstone. tableName="+tableName;
                    throw new RuntimeException(message);
                }
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    protected InternalScanner preFlushAction(InternalScanner scanner) {
        SIDriver driver=SIDriver.driver();
        // We must make sure the engine is started, otherwise we might try to resolve transactions against SPLICE_TXN which
        // hasn't been loaded yet, causing a deadlock
        if(tableEnvMatch && scanner != null && driver != null && driver.isEngineStarted() && driver.getConfiguration().getResolutionOnFlushes()){
            SimpleCompactionContext context = new SimpleCompactionContext();
            SICompactionState state = new SICompactionState(driver.getTxnSupplier(),
                    driver.getConfiguration().getActiveTransactionCacheSize(), context, driver.getRejectingExecutorService());
            SConfiguration conf = driver.getConfiguration();
            SICompactionScanner siScanner = new SICompactionScanner(state,scanner, false, conf.getFlushResolutionShare(), conf.getOlapCompactionResolutionBufferSize(), context);
            siScanner.start();
            return siScanner;
        }else {
            return scanner;
        }
    }

    protected InternalScanner preCompactAction (InternalScanner scanner) throws IOException {
        try {
            if(tableEnvMatch && scanner != null){
                SIDriver driver=SIDriver.driver();
                SimpleCompactionContext context = new SimpleCompactionContext();
                boolean blocking = HConfiguration.getConfiguration().getOlapCompactionBlocking();
                SICompactionState state = new SICompactionState(driver.getTxnSupplier(),
                        driver.getConfiguration().getActiveTransactionCacheSize(), context, blocking ? driver.getExecutorService() : driver.getRejectingExecutorService());
                SConfiguration conf = driver.getConfiguration();
                SICompactionScanner siScanner = new SICompactionScanner(state,scanner, false, conf.getOlapCompactionResolutionShare(), conf.getOlapCompactionResolutionBufferSize(), context);
                siScanner.start();
                return siScanner;
            }else{
                return scanner;
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    protected void prePutAction(ObserverContext<RegionCoprocessorEnvironment> c, Put put) throws IOException {
        checkAccess();

        try {
        /*
		 * This is relatively expensive--it's better to use the write pipeline when you need to load a lot of rows.
		 */
            if (!tableEnvMatch || put.getAttribute(SIConstants.SI_NEEDED) == null) {
                return;
            }

            byte[] attribute = put.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
            assert attribute != null : "Transaction not specified!";


            TxnView txn = txnOperationFactory.fromWrites(attribute, 0, attribute.length);
            boolean isDelete = put.getAttribute(SIConstants.SI_DELETE_PUT) != null;
            byte[] row = put.getRow();
            boolean isSIDataOnly = true;
            //convert the put into a collection of KVPairs
            Map<byte[], Map<byte[], KVPair>> familyMap = Maps.newHashMap();
            Iterable<Cell> keyValues = Iterables.concat(put.getFamilyCellMap().values());
            for (Cell kv : keyValues) {
                @SuppressWarnings("deprecation") byte[] family = CellUtil.cloneFamily(kv);
                @SuppressWarnings("deprecation") byte[] column = CellUtil.cloneQualifier(kv);
                if (!Bytes.equals(column, SIConstants.PACKED_COLUMN_BYTES)) continue; //skip SI columns

                isSIDataOnly = false;
                @SuppressWarnings("deprecation") byte[] value = CellUtil.cloneValue(kv);
                Map<byte[], KVPair> columnMap = familyMap.get(family);
                if (columnMap == null) {
                    columnMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                    familyMap.put(family, columnMap);
                }
                columnMap.put(column, new KVPair(row, value, isDelete ? KVPair.Type.DELETE : KVPair.Type.INSERT));
            }
            if (isSIDataOnly) {
                byte[] family = SIConstants.DEFAULT_FAMILY_BYTES;
                byte[] column = SIConstants.PACKED_COLUMN_BYTES;
                byte[] value = HConstants.EMPTY_BYTE_ARRAY;
                Map<byte[], KVPair> columnMap = familyMap.get(family);
                if (columnMap == null) {
                    columnMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                    familyMap.put(family, columnMap);
                }
                columnMap.put(column, new KVPair(row, value, isDelete ? KVPair.Type.DELETE : KVPair.Type.EMPTY_COLUMN));
            }
            for (Map.Entry<byte[], Map<byte[], KVPair>> family : familyMap.entrySet()) {
                byte[] fam = family.getKey();
                Map<byte[], KVPair> cols = family.getValue();
                for (Map.Entry<byte[], KVPair> column : cols.entrySet()) {
                    boolean processed = false;
                    while (!processed) {
                        @SuppressWarnings("unchecked") Iterable<MutationStatus> status =
                                region.bulkWrite(txn, fam, column.getKey(), operationStatusFactory.getNoOpConstraintChecker(), Collections.singleton(column.getValue()), false, false, true);
                        Iterator<MutationStatus> itr = status.iterator();
                        if (!itr.hasNext())
                            throw new IllegalStateException();
                        OperationStatus ms = ((HMutationStatus) itr.next()).unwrapDelegate();
                        switch (ms.getOperationStatusCode()) {
                            case NOT_RUN:
                                break;
                            case BAD_FAMILY:
                                throw new NoSuchColumnFamilyException(ms.getExceptionMsg());
                            case SANITY_CHECK_FAILURE:
                                throw new IOException("Sanity Check failure:" + ms.getExceptionMsg());
                            case FAILURE:
                                if (ms instanceof ExtendedOperationStatus) {
                                    throw ((ExtendedOperationStatus) ms).getException();
                                }
                                throw new IOException(ms.getExceptionMsg());
                            default:
                                processed = true;
                        }
                    }
                }
            }

            c.bypass();
            // TODO c.complete();
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    protected boolean shouldUseSI(OperationWithAttributes op){
        if(op.getAttribute(SIConstants.SI_NEEDED)==null) return false;
        else return op.getAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null;
    }

    @SuppressWarnings("RedundantIfStatement") //we keep it this way for clarity
    protected void doesTableNeedSI(TableName tableName){
        SConfiguration config = HConfiguration.getConfiguration();
        TableType tableType= EnvUtils.getTableType(config,tableName);
        spliceTable = tableName.getNamespaceAsString().equals(config.getNamespace());

        SpliceLogUtils.trace(LOG,"table %s has Env %s",tableName,tableType);
        switch(tableType){
            case TRANSACTION_TABLE:
            case ROOT_TABLE:
            case META_TABLE:
            case HBASE_TABLE:
                tableEnvMatch = false;
                return;
            default:
                tableEnvMatch = true;
                return;

        }
    }

    protected void checkAccess() throws AccessDeniedException {
        if (!spliceTable)
            return;

        if (!UserGroupInformation.isSecurityEnabled())
            return;

        User user = HBasePlatformUtils.getUser();
        if (user == null || user.getShortName().equalsIgnoreCase("hbase"))
            return;

        if (RpcUtils.isAccessAllowed())
            return;

        if (!authTokenEnabled && authManager.authorize(user, Permission.Action.ADMIN))
            return;

        throw new AccessDeniedException("Insufficient permissions for user " +
                user.getShortName());
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/
    protected void addSIFilterToGet(Get get) throws IOException{
        byte[] attribute=get.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
        assert attribute!=null: "Transaction information is missing";

        TxnView txn=txnOperationFactory.fromReads(attribute,0,attribute.length);
        final Filter newFilter=makeSIFilter(txn,get.getFilter(), getPredicateFilter(get),false);
        get.setFilter(newFilter);
    }

    protected void addSIFilterToScan(Scan scan) throws IOException{
        byte[] attribute=scan.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
        assert attribute!=null: "Transaction information is missing";

        TxnView txn=txnOperationFactory.fromReads(attribute,0,attribute.length);
        final Filter newFilter=makeSIFilter(txn,scan.getFilter(),
                getPredicateFilter(scan),scan.getAttribute(SIConstants.SI_COUNT_STAR)!=null);
        scan.setFilter(newFilter);
    }

    protected Filter makeSIFilter(TxnView txn, Filter currentFilter, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException{
        TxnFilter txnFilter=region.packedFilter(txn,predicateFilter,countStar);
        @SuppressWarnings("unchecked") SIFilterPacked siFilter=new SIFilterPacked(txnFilter);
        if(currentFilter!=null){
            return composeFilters(orderFilters(currentFilter,siFilter));
        }else{
            return siFilter;
        }
    }
    protected EntryPredicateFilter getPredicateFilter(OperationWithAttributes operation) throws IOException{
        final byte[] serializedPredicateFilter=operation.getAttribute(ENTRY_PREDICATE_LABEL);
        return EntryPredicateFilter.fromBytes(serializedPredicateFilter);
    }

    protected Filter[] orderFilters(Filter currentFilter,Filter siFilter){
        if(currentFilter instanceof TransactionalFilter && ((TransactionalFilter)currentFilter).isBeforeSI()){
            return new Filter[]{currentFilter,siFilter};
        }else{
            return new Filter[]{siFilter,currentFilter};
        }
    }

    protected FilterList composeFilters(Filter[] filters){
        return new FilterList(FilterList.Operator.MUST_PASS_ALL,filters[0],filters[1]);
    }

    protected boolean hasToken(Scan scan) {
        if (!SIDriver.driver().getConfiguration().getAuthenticationTokenEnabled())
            return false;
        byte[] token=scan.getAttribute(SIConstants.TOKEN_ACL_NAME);
        return token != null && token.length > 0;
    }

    protected boolean aclCheck(Scan scan) throws IOException, StandardException {
        byte[] token=scan.getAttribute(SIConstants.TOKEN_ACL_NAME);

        AclCheckerService.getService().checkPermission(token, conglomId, Authorizer.SELECT_PRIV);
        return true;
    }
}
