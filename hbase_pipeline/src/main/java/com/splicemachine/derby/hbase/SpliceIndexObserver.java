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

package com.splicemachine.derby.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.db.iapi.error.ExceptionUtil;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.PipelineLoadService;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.CoprocessorUtils;
import com.splicemachine.si.data.hbase.coprocessor.TableType;
import com.splicemachine.si.impl.HWriteConflict;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.RegionServerControl;
import com.splicemachine.storage.RegionPartition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Function;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceIndexObserver implements RegionObserver, RegionCoprocessor {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);

    private long conglomId=-1L;
    private TransactionalRegion region;
    private TxnOperationFactory operationFactory;
    private PipelineExceptionFactory exceptionFactory;
    private volatile ContextFactoryLoader factoryLoader;
    private volatile PipelineLoadService<TableName> service;
    private volatile WriteContextFactory<TransactionalRegion> ctxFactory;
    protected Optional<RegionObserver> optionalRegionObserver = Optional.empty();

    @Override
    public void start(final CoprocessorEnvironment e) throws IOException{
        try {
            RegionCoprocessorEnvironment rce = ((RegionCoprocessorEnvironment) e);
            optionalRegionObserver = Optional.of(this);
            String tableName = rce.getRegion().getTableDescriptor().getTableName().getQualifierAsString();
            TableType table = EnvUtils.getTableType(HConfiguration.getConfiguration(), rce);
            switch (table) {
                case DERBY_SYS_TABLE:
                    conglomId = -1; //bypass index management on derby system tables
                    break;
                case USER_TABLE:
                    conglomId = Long.parseLong(tableName);
                    break;
                default:
                    return; //disregard table environments which are not user or system tables
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
        RegionCoprocessorEnvironment rce = c.getEnvironment();
        try {
            final long cId = conglomId;
            final RegionPartition baseRegion=new RegionPartition((HRegion)rce.getRegion());
            ServerControl sc = new RegionServerControl((HRegion)rce.getRegion(),(RegionServerServices)rce.getOnlineRegions());
            if(service==null){
                service=new PipelineLoadService<TableName>(sc,baseRegion,cId){
                    @Override
                    public void start() throws Exception{
                        super.start();
                        PipelineDriver pipelineDriver=PipelineDriver.driver();
                        factoryLoader=pipelineDriver.getContextFactoryLoader(cId);
                        ctxFactory = getWritePipeline().getContextFactory();

                        SIDriver siDriver=SIDriver.driver();

                        region=siDriver.transactionalPartition(cId,baseRegion);
                        operationFactory=siDriver.getOperationFactory();
                        exceptionFactory=pipelineDriver.exceptionFactory();
                    }

                    @Override
                    public void shutdown() throws Exception{
                        if(factoryLoader!=null)
                            factoryLoader.close();
                        super.shutdown();
                    }

                    @Override
                    protected Function<TableName, String> getStringParsingFunction(){
                        return new Function<TableName, String>(){
                            @Nullable
                            @Override
                            public String apply(TableName tableName){
                                return tableName.getNameAsString();
                            }
                        };
                    }

                    @Override
                    protected PipelineEnvironment loadPipelineEnvironment(ContextFactoryDriver cfDriver) throws IOException{
                        return HBasePipelineEnvironment.loadEnvironment(new SystemClock(),cfDriver);
                    }
                };
            }
            DatabaseLifecycleManager.manager().registerGeneralService(service);
        } catch (Throwable t) {
            ExceptionUtil.throwAsRuntime(CoprocessorUtils.getIOException(t));
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        try {
            optionalRegionObserver = Optional.empty();
            if (region != null)
                region.close();
            if(service!=null) {
                service.shutdown();
                DatabaseLifecycleManager.manager().deregisterGeneralService(service);
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        try {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "prePut %s",put);
            if(conglomId>0){
                if(put.getAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null) return;
                if(factoryLoader==null){
                    /** the below awaitStartup() call is commented out since we don't think there will be
                     * rows inserted into a user index during startup, this code will cause a deadlock
                     * when we add a new system index during system upgrade *
                    try{
                        DatabaseLifecycleManager.manager().awaitStartup();
                    }catch(InterruptedException e1){
                        throw new InterruptedIOException();
                    }
                    */
                    /* In the case that a new system index is added during a system upgrade, its conglomId will be
                       bigger than 0, but it should still go through the path for system index instead of the regular
                       user table index
                     */
                    return;
                }
                //we can't update an index if the conglomerate id isn't positive--it's probably a temp table or something
                byte[] row = put.getRow();
                List<Cell> data = put.get(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES);
                KVPair kv;
                if(data!=null&&!data.isEmpty()){
                    byte[] value = CellUtil.cloneValue(data.get(0));
                    if(put.getAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null){
                        kv = new KVPair(row,value, KVPair.Type.UPDATE);
                    }else
                        kv = new KVPair(row,value);
                }else{
                    kv = new KVPair(row, HConstants.EMPTY_BYTE_ARRAY);
                }
                byte[] txnData = put.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
                TxnView txn = operationFactory.fromWrites(txnData,0,txnData.length);
                mutate(kv,txn);
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

   
    public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException{
        try {
            RegionCoprocessorEnvironment rce=ctx.getEnvironment();
            start(rce);
            RegionCoprocessorHost coprocessorHost=((HRegion)rce.getRegion()).getCoprocessorHost();
            Coprocessor coprocessor=coprocessorHost.findCoprocessor(SpliceIndexEndpoint.class.getName());
            coprocessor.start(rce);
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    /**
     * ***************************************************************************************************************
     */
    /*private helper methods*/
    private void mutate(KVPair mutation,TxnView txn) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "mutate %s", mutation);
        //we've already done our write path, so just pass it through
//        WriteContextFactory<TransactionalRegion> ctxFactory = WriteContextFactoryManager.getWriteContext(conglomId,
//                config,
//                tableFactory,
//                exceptionFactory,
//                TABLE_INFO_PARSER,
//                factoryLoader);

        try {
            WriteContext context = ctxFactory.createPassThrough(null, txn, region, 1, null);
            context.sendUpstream(mutation);
            context.flush();
            WriteResult mutationResult = context.close().get(mutation);
            if (mutationResult == null) {
                return; //we didn't actually do anything, so no worries
            }
            switch (mutationResult.getCode()) {
                case FAILED:
                    throw new IOException(mutationResult.getErrorMessage());
                case PRIMARY_KEY_VIOLATION:
                    throw exceptionFactory.primaryKeyViolation(mutationResult.getConstraintContext());
                case UNIQUE_VIOLATION:
                    throw exceptionFactory.uniqueViolation(mutationResult.getConstraintContext());
                case FOREIGN_KEY_VIOLATION:
                    throw exceptionFactory.foreignKeyViolation(mutationResult.getConstraintContext());
                case CHECK_VIOLATION:
                    throw exceptionFactory.doNotRetry(mutationResult.toString());//TODO -sf- implement properly!
                case WRITE_CONFLICT:
                    throw HWriteConflict.fromString(mutationResult.getErrorMessage());
                case NOT_RUN:
                case SUCCESS:
                default:
                    break;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            ctxFactory.close();
        }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return optionalRegionObserver;
    }
}
