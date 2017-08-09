/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.data.hbase.coprocessor;


import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
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
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.driver.SIDriver;
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
            txnOperationFactory=new SimpleTxnOperationFactory();
            //noinspection unchecked
            Partition regionPartition = new RegionPartition((HRegion)rce.getRegion());
            region=new TxnRegion(regionPartition,
                    driver.getTxnStore(),
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
            return new SICompactionScanner(scanner);
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
        assert attribute!=null: "Txn not specified!";
        //convert the put into a collection of KVPairs
        e.bypass();
        e.complete();
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

}
