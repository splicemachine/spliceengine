/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.splicemachine.si.data.hbase.ExtendedOperationStatus;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Maps;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionalFilter;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.HOperationFactory;
import com.splicemachine.si.impl.SIFilterPacked;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.SICompactionState;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HMutationStatus;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.RegionPartition;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserver extends BaseRegionObserver{
    private static Logger LOG=Logger.getLogger(SIObserver.class);
    private boolean tableEnvMatch=false;
    private TxnOperationFactory txnOperationFactory;
    private OperationStatusFactory operationStatusFactory;
    private TransactionalRegion region;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException{
        SpliceLogUtils.trace(LOG,"starting %s",SIObserver.class);
        RegionCoprocessorEnvironment rce=(RegionCoprocessorEnvironment)e;
        tableEnvMatch=doesTableNeedSI(rce.getRegion().getTableDesc().getTableName());
        if(tableEnvMatch){
            HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),ZkUtils.getRecoverableZooKeeper());
            SIDriver driver = env.getSIDriver();
            operationStatusFactory = driver.getOperationStatusLib();
            //noinspection unchecked
            txnOperationFactory=new SimpleTxnOperationFactory(driver.getExceptionFactory(),HOperationFactory.INSTANCE);
            //noinspection unchecked
            Partition regionPartition = new RegionPartition((HRegion)rce.getRegion());
            region=new TxnRegion(regionPartition,
                    driver.getRollForward(),
                    driver.getReadResolver(regionPartition),
                    driver.getTxnSupplier(),
                    driver.getTransactor(),
                    driver.getOperationFactory()
                    );
            Tracer.traceRegion(region.getTableName(),rce.getRegion());
        }
        super.start(e);
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException{
        SpliceLogUtils.trace(LOG,"stopping %s",SIObserver.class);
        super.stop(e);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,Get get,List<Cell> results) throws IOException{
        SpliceLogUtils.trace(LOG,"preGet %s",get);
        if(tableEnvMatch && shouldUseSI(get)){
            get.setMaxVersions();
            get.setTimeRange(0L,Long.MAX_VALUE);
            assert (get.getMaxVersions()==Integer.MAX_VALUE);
            addSIFilterToGet(get);
        }
        SpliceLogUtils.trace(LOG,"preGet after %s",get);
        super.preGetOp(e,get,results);
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,Scan scan,RegionScanner s) throws IOException{
        SpliceLogUtils.trace(LOG,"preScannerOpen %s with tableEnvMatch=%s, shouldUseSI=%s",scan,tableEnvMatch,shouldUseSI(scan));
        if(tableEnvMatch && shouldUseSI(scan)){
            scan.setMaxVersions();
            scan.setTimeRange(0L,Long.MAX_VALUE);
//            txnReadController.preProcessScan(scan);
            assert (scan.getMaxVersions()==Integer.MAX_VALUE);
            addSIFilterToScan(scan);
        }
        return super.preScannerOpen(e,scan,s);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile){
        if(tableEnvMatch){
            Tracer.compact();
        }
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,Delete delete,WALEdit edit,
                          Durability writeToWAL) throws IOException{
        if(tableEnvMatch){
            if(delete.getAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null){
                TableName tableName=e.getEnvironment().getRegion().getTableDesc().getTableName();
                String message="Direct deletes are not supported under snapshot isolation. "+
                        "Instead a Put is expected that will set a record level tombstone. tableName="+tableName;
                throw new RuntimeException(message);
            }
        }
        super.preDelete(e,delete,edit,writeToWAL);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,
                                      InternalScanner scanner,ScanType scanType,CompactionRequest compactionRequest) throws IOException{
        if(tableEnvMatch){
            SIDriver driver=SIDriver.driver();
            SICompactionState state = new SICompactionState(driver.getTxnSupplier(),
                    driver.getRollForward(),
                    driver.getConfiguration().getActiveTransactionCacheSize());
            return new SICompactionScanner(state,scanner);
        }else{
            return super.preCompact(e,store,scanner,scanType,compactionRequest);
        }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,Put put,WALEdit edit,Durability writeToWAL) throws IOException{
        /*
		 * This is relatively expensive--it's better to use the write pipeline when you need to load a lot of rows.
		 */
        if(!tableEnvMatch || put.getAttribute(SIConstants.SI_NEEDED)==null){
            super.prePut(e,put,edit,writeToWAL);
            return;
        }
        byte[] attribute=put.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
        assert attribute!=null: "Transaction not specified!";


        TxnView txn=txnOperationFactory.fromWrites(attribute,0,attribute.length);
        boolean isDelete=put.getAttribute(SIConstants.SI_DELETE_PUT)!=null;
        byte[] row=put.getRow();
        boolean isSIDataOnly=true;
        //convert the put into a collection of KVPairs
        Map<byte[], Map<byte[], KVPair>> familyMap=Maps.newHashMap();
        Iterable<Cell> keyValues=Iterables.concat(put.getFamilyCellMap().values());
        for(Cell kv : keyValues){
            @SuppressWarnings("deprecation") byte[] family=kv.getFamily();
            @SuppressWarnings("deprecation") byte[] column=kv.getQualifier();
            if(!Bytes.equals(column,SIConstants.PACKED_COLUMN_BYTES)) continue; //skip SI columns

            isSIDataOnly=false;
            @SuppressWarnings("deprecation") byte[] value=kv.getValue();
            Map<byte[], KVPair> columnMap=familyMap.get(family);
            if(columnMap==null){
                columnMap=Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                familyMap.put(family,columnMap);
            }
            columnMap.put(column,new KVPair(row,value,isDelete?KVPair.Type.DELETE:KVPair.Type.INSERT));
        }
        if(isSIDataOnly){
            byte[] family=SIConstants.DEFAULT_FAMILY_BYTES;
            byte[] column=SIConstants.PACKED_COLUMN_BYTES;
            byte[] value=HConstants.EMPTY_BYTE_ARRAY;
            Map<byte[], KVPair> columnMap=familyMap.get(family);
            if(columnMap==null){
                columnMap=Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                familyMap.put(family,columnMap);
            }
            columnMap.put(column,new KVPair(row,value,isDelete?KVPair.Type.DELETE:KVPair.Type.EMPTY_COLUMN));
        }
        for(Map.Entry<byte[], Map<byte[], KVPair>> family : familyMap.entrySet()){
            byte[] fam=family.getKey();
            Map<byte[], KVPair> cols=family.getValue();
            for(Map.Entry<byte[], KVPair> column : cols.entrySet()){
                boolean processed=false;
                while (!processed) {
                    @SuppressWarnings("unchecked") Iterable<MutationStatus> status =
                            region.bulkWrite(txn, fam, column.getKey(), operationStatusFactory.getNoOpConstraintChecker(), Collections.singleton(column.getValue()));
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
                                throw ((ExtendedOperationStatus)ms).getException();
                            }
                            throw new IOException(ms.getExceptionMsg());
                        default:
                            processed = true;
                    }
                }
            }
        }

        e.bypass();
        e.complete();
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void addSIFilterToGet(Get get) throws IOException{
        byte[] attribute=get.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
        assert attribute!=null: "Transaction information is missing";

        TxnView txn=txnOperationFactory.fromReads(attribute,0,attribute.length);
        final Filter newFilter=makeSIFilter(txn,get.getFilter(), getPredicateFilter(get),false);
        get.setFilter(newFilter);
    }

    private void addSIFilterToScan(Scan scan) throws IOException{
        byte[] attribute=scan.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
        assert attribute!=null: "Transaction information is missing";

        TxnView txn=txnOperationFactory.fromReads(attribute,0,attribute.length);
        final Filter newFilter=makeSIFilter(txn,scan.getFilter(),
                getPredicateFilter(scan),scan.getAttribute(SIConstants.SI_COUNT_STAR)!=null);
        scan.setFilter(newFilter);
    }

    private EntryPredicateFilter getPredicateFilter(OperationWithAttributes operation) throws IOException{
        final byte[] serializedPredicateFilter=operation.getAttribute(ENTRY_PREDICATE_LABEL);
        return EntryPredicateFilter.fromBytes(serializedPredicateFilter);
    }

    private boolean shouldUseSI(OperationWithAttributes op){
        if(op.getAttribute(SIConstants.SI_NEEDED)==null) return false;
        else return op.getAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null;
    }

    @SuppressWarnings("RedundantIfStatement") //we keep it this way for clarity
    private static boolean doesTableNeedSI(TableName tableName){
        TableType tableType=EnvUtils.getTableType(HConfiguration.getConfiguration(),tableName);
        SpliceLogUtils.trace(LOG,"table %s has Env %s",tableName,tableType);
        switch(tableType){
            case TRANSACTION_TABLE:
            case ROOT_TABLE:
            case META_TABLE:
            case HBASE_TABLE:
                return false;
        }
        return true;
    }

    private Filter makeSIFilter(TxnView txn,Filter currentFilter,EntryPredicateFilter predicateFilter,boolean countStar) throws IOException{
        TxnFilter txnFilter=region.packedFilter(txn,predicateFilter,countStar);
        @SuppressWarnings("unchecked") SIFilterPacked siFilter=new SIFilterPacked(txnFilter);
        if(currentFilter!=null){
            return composeFilters(orderFilters(currentFilter,siFilter));
        }else{
            return siFilter;
        }
    }

    private Filter[] orderFilters(Filter currentFilter,Filter siFilter){
        if(currentFilter instanceof TransactionalFilter && ((TransactionalFilter)currentFilter).isBeforeSI()){
            return new Filter[]{currentFilter,siFilter};
        }else{
            return new Filter[]{siFilter,currentFilter};
        }
    }

    private FilterList composeFilters(Filter[] filters){
        return new FilterList(FilterList.Operator.MUST_PASS_ALL,filters[0],filters[1]);
    }
}
