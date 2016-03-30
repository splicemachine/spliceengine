package com.splicemachine.derby.hbase;

import com.google.common.base.Function;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.PipelineLoadService;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.contextfactory.WriteContextFactoryManager;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.TableType;
import com.splicemachine.si.impl.HWriteConflict;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.RegionServerControl;
import com.splicemachine.storage.RegionPartition;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.PipelineEnvironment;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceIndexObserver extends BaseRegionObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);
    private static final Function<TableName,String> TABLE_INFO_PARSER= new Function<TableName, String>(){
        @Override public String apply(TableName input){
            assert input!=null: "input cannot be null!";
            return input.getNameAsString(); }
    };

    private long conglomId=-1l;
    private TransactionalRegion region;
    private TxnOperationFactory operationFactory;
    private PipelineExceptionFactory exceptionFactory;
    private SConfiguration config;
    private PartitionFactory tableFactory;
    private volatile ContextFactoryLoader factoryLoader;
    private volatile PipelineLoadService<TableName> service;

    @Override
    public void start(final CoprocessorEnvironment e) throws IOException{
        RegionCoprocessorEnvironment rce=((RegionCoprocessorEnvironment)e);

        String tableName=rce.getRegion().getTableDesc().getTableName().getQualifierAsString();
        TableType table=EnvUtils.getTableType(HConfiguration.getConfiguration(),rce);
        switch(table){
            case TRANSACTION_TABLE:
            case ROOT_TABLE:
            case META_TABLE:
            case HBASE_TABLE:
                return; //disregard table environments which are not user or system tables
        }
        long conglomId;
        try{
            conglomId=Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.warn(LOG,"Unable to parse conglomerate id for table %s, "+
                    "index management for batch operations will be disabled",tableName);
            conglomId=-1;
        }

        final long cId = conglomId;
        final RegionPartition baseRegion=new RegionPartition(rce.getRegion());
        ServerControl sc = new RegionServerControl(rce.getRegion());
        try{
            if(service==null){
                service=new PipelineLoadService<TableName>(sc,baseRegion,cId){
                    @Override
                    public void start() throws Exception{
                        super.start();
                        PipelineDriver pipelineDriver=PipelineDriver.driver();
                        factoryLoader=pipelineDriver.getContextFactoryLoader(cId);

                        SIDriver siDriver=SIDriver.driver();

                        region=siDriver.transactionalPartition(cId,baseRegion);
                        operationFactory=siDriver.getOperationFactory();
                        exceptionFactory=pipelineDriver.exceptionFactory();
                        config=pipelineEnv.configuration();
                        tableFactory=siDriver.getTableFactory();
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
        }catch(Exception ex){
            throw new IOException(ex);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
        if (region != null)
            region.close();
        if(service!=null)
            try{
                service.shutdown();
            }catch(Exception e1){
                throw new IOException(e1);
            }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "prePut %s",put);
        if(conglomId>0){
            if(factoryLoader==null){
                try{
                    DatabaseLifecycleManager.manager().awaitStartup();
                }catch(InterruptedException e1){
                    throw new InterruptedIOException();
                }
            }
            if(put.getAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null) return;

            //we can't update an index if the conglomerate id isn't positive--it's probably a temp table or something
            byte[] row = put.getRow();
            List<Cell> data = put.get(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES);
            KVPair kv;
            if(data!=null&&data.size()>0){
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
        super.prePut(e, put, edit, durability);
    }

    @Override
    public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException{
        RegionCoprocessorEnvironment rce=ctx.getEnvironment();
        start(rce);
        RegionCoprocessorHost coprocessorHost=rce.getRegion().getCoprocessorHost();
        Coprocessor coprocessor=coprocessorHost.findCoprocessor(SpliceIndexEndpoint.class.getName());
        coprocessor.start(rce);
        super.postRollBackSplit(ctx);
    }

    /**
     * ***************************************************************************************************************
     */
    /*private helper methods*/

    protected void mutate(KVPair mutation,TxnView txn) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "mutate %s", mutation);
        //we've already done our write path, so just pass it through
        WriteContextFactory<TransactionalRegion> ctxFactory = WriteContextFactoryManager.getWriteContext(conglomId,
                config,
                tableFactory,
                exceptionFactory,
                TABLE_INFO_PARSER,
                factoryLoader);

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

}
