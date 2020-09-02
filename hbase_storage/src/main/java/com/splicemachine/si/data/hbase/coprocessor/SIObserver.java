/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.data.hbase.coprocessor;

import static com.splicemachine.si.constants.SIConstants.ENTRY_PREDICATE_LABEL;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.compactions.SpliceCompactionRequest;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.hbase.TransactionsWatcher;
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
import com.splicemachine.si.impl.server.PurgeConfigBuilder;
import com.splicemachine.si.impl.server.SICompactionState;
import com.splicemachine.si.impl.server.PurgeConfig;
import com.splicemachine.si.impl.server.SimpleCompactionContext;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcUtils;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TableAuthManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Iterables;
import splice.com.google.common.collect.Maps;

import static com.splicemachine.si.constants.SIConstants.ENTRY_PREDICATE_LABEL;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserver implements RegionObserver, Coprocessor, RegionCoprocessor{
    private static Logger LOG = Logger.getLogger(SIObserver.class);
    protected boolean tableEnvMatch=false;
    protected boolean spliceTable=false;
    protected long conglomId;
    protected TxnOperationFactory txnOperationFactory;
    protected OperationStatusFactory operationStatusFactory;
    protected TransactionalRegion region;
    protected TableAuthManager authManager = null;
    protected boolean authTokenEnabled;
    protected Optional<RegionObserver> optionalRegionObserver = Optional.empty();

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        try {
            SpliceLogUtils.trace(LOG, "starting %s", SIObserver.class);
            optionalRegionObserver = Optional.of(this);
            RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) e;
            TableName tableName = rce.getRegion().getTableDescriptor().getTableName();
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

            ZKWatcher zk = ((RegionServerServices)((RegionCoprocessorEnvironment)e).getOnlineRegions()).getZooKeeper();
            this.authManager = TableAuthManager.getOrCreate(zk, e.getConfiguration());
            this.authTokenEnabled = driver.getConfiguration().getAuthenticationTokenEnabled();

        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException{
        try {
            SpliceLogUtils.trace(LOG,"stopping %s",SIObserver.class);
            optionalRegionObserver = Optional.empty();
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan) throws IOException {
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

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        try {
            if(tableEnvMatch){
                Tracer.compact();
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
        SIDriver driver=SIDriver.driver();
        // We must make sure the engine is started, otherwise we might try to resolve transactions against SPLICE_TXN which
        // hasn't been loaded yet, causing a deadlock
        if(tableEnvMatch && scanner != null && driver != null && driver.isEngineStarted() && driver.getConfiguration().getResolutionOnFlushes()){
            SimpleCompactionContext context = new SimpleCompactionContext();
            SICompactionState state = new SICompactionState(driver.getTxnSupplier(),
                    driver.getConfiguration().getActiveTransactionMaxCacheSize(), context, driver.getRejectingExecutorService());
            SConfiguration conf = driver.getConfiguration();
            PurgeConfigBuilder purgeConfig = new PurgeConfigBuilder();
            if (conf.getOlapCompactionAutomaticallyPurgeDeletedRows()) {
                purgeConfig.purgeDeletesDuringFlush();
            } else {
                purgeConfig.noPurgeDeletes();
            }
            purgeConfig.transactionLowWatermark(TransactionsWatcher.getLowWatermarkTransaction());
            purgeConfig.purgeUpdates(conf.getOlapCompactionAutomaticallyPurgeOldUpdates());
            // We use getOlapCompactionResolutionBufferSize() here instead of getLocalCompactionResolutionBufferSize() because we are dealing with data
            // coming from the MemStore, it's already in memory and the rows shouldn't be very big or have many KVs
            SICompactionScanner siScanner = new SICompactionScanner(
                    state, scanner, purgeConfig.build(), conf.getFlushResolutionShare(),
                    conf.getOlapCompactionResolutionBufferSize(), context);
            siScanner.start();
            return siScanner;
        }else {
            return scanner;
        }
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        assert request instanceof SpliceCompactionRequest;
        try {
            // We can't return null, there's a check in org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost.preCompact
            // return a dummy implementation instead
            if (scanner == null || scanner == DummyScanner.INSTANCE)
                return DummyScanner.INSTANCE;

            if(tableEnvMatch){
                SIDriver driver=SIDriver.driver();
                SimpleCompactionContext context = new SimpleCompactionContext();
                SICompactionState state = new SICompactionState(driver.getTxnSupplier(),
                        driver.getConfiguration().getActiveTransactionMaxCacheSize(), context, driver.getRejectingExecutorService());
                SConfiguration conf = driver.getConfiguration();
                SICompactionScanner siScanner = new SICompactionScanner(
                        state, scanner, ((SpliceCompactionRequest) request).getPurgeConfig(),
                        conf.getOlapCompactionResolutionShare(), conf.getLocalCompactionResolutionBufferSize(), context);
                siScanner.start();
                return siScanner;
            }
            return scanner;
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }


    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
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

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
        checkAccess();

        try {
            if(tableEnvMatch){
                if(delete.getAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null){
                    TableName tableName=c.getEnvironment().getRegion().getTableDescriptor().getTableName();
                    String message="Direct deletes are not supported under snapshot isolation. "+
                            "Instead a Put is expected that will set a record level tombstone. tableName="+tableName;
                    throw new RuntimeException(message);
                }
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }


    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return optionalRegionObserver;
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

        User user = RpcServer.getRequestUser().get();
        if (user == null || user.getShortName().equalsIgnoreCase("hbase"))
            return;

        if (RpcUtils.isAccessAllowed())
            return;

        if (!authTokenEnabled && authManager.authorize(user, Permission.Action.ADMIN))
            return;

        throw new AccessDeniedException("Insufficient permissions for user " +
                user.getShortName());
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

    private byte[] getToken(String path) throws IOException{
        byte[] token = null;
        FSDataInputStream in = null;
        try {
            FileSystem fs = FileSystem.get(new URI(path), HConfiguration.unwrapDelegate());
            Path p = new Path(path, "_token");
            if (fs.exists(p)) {
                in = fs.open(p);
                int len = in.readInt();
                token = new byte[len];
                in.readFully(token);
            }
            return token;
        } catch (Exception e) {
            throw new IOException(e);
        }
        finally {
            if (in != null) {
                in.close();
            }
        }
    }

    @Override
    public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx, RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
        // DB-9895: HBase may replay a WAL edits that has been persisted in HFile. It could happen that a row with
        // FIRST_WRITE_TOKE has already been persisted in an HFile. If the row is replayed to memstore and deleted,
        // the row will be purged during flush. However, the row that already persisted in HFile is still visible.
        // Remove FIRST_WRITE_TOKE during wal replay to prevent this from happening.
        SConfiguration config = HConfiguration.getConfiguration();
        String namespace = logKey.getTableName().getNamespaceAsString();
        if (namespace.equals(config.getNamespace())) {
            ArrayList<Cell> cells = logEdit.getCells();
            Iterator<Cell> it = cells.iterator();
            while (it.hasNext()) {
                Cell cell = it.next();
                CellType cellType = CellUtils.getKeyValueType(cell);
                if (cellType == CellType.FIRST_WRITE_TOKEN) {
                    it.remove();
                }
            }
        }
    }
}
