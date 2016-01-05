package com.splicemachine.derby.hbase;

import com.google.common.base.Function;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.DatabaseLifecycleService;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.contextfactory.WriteContextFactoryManager;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.HWriteConflict;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.RegionPartition;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.PipelineEnvironment;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceIndexObserver extends BaseRegionObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);
    private static final Function<TableName,String> TABLE_INFO_PARSER= new Function<TableName, String>(){
        @Override public String apply(TableName input){ return input.getNameAsString(); }
    };

    private long conglomId=-1l;
    private TransactionalRegion region;
    private TxnOperationFactory operationFactory;
    private PipelineExceptionFactory exceptionFactory;
    private SConfiguration config;
    private PartitionFactory tableFactory;
    private volatile ContextFactoryLoader factoryLoader;

    //TODO -sf- add relevant observers in
    private Set<CompactionObserver> compactionObservers = new CopyOnWriteArraySet<>();
    private Set<SplitObserver> splitObservers = new CopyOnWriteArraySet<>();
    private Set<FlushObserver> flushObservers = new CopyOnWriteArraySet<>();
    private Set<StoreScannerObserver> storeScannerObservers = new CopyOnWriteArraySet<>();

    @Override
    public void start(final CoprocessorEnvironment e) throws IOException{
        RegionCoprocessorEnvironment rce=((RegionCoprocessorEnvironment)e);

        String tableName=rce.getRegion().getTableDesc().getTableName().getQualifierAsString();
        SpliceConstants.TableEnv table=EnvUtils.getTableEnv(rce);
        switch(table){
            case TRANSACTION_TABLE:
            case ROOT_TABLE:
            case META_TABLE:
            case HBASE_TABLE:
                return; //disregard table environments which are not user or system tables
        }
        final long conglomId;
        try{
            conglomId=Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.warn(LOG,"Unable to parse conglomerate id for table %s, "+
                    "index management for batch operations will be disabled",tableName);
            return;
        }
        PipelineEnvironment pipelineEnv=HBasePipelineEnvironment.loadEnvironment(new SystemClock(),null); //TODO -sf- register a factory loader
        RegionPartition baseRegion=new RegionPartition(rce.getRegion());
        final PipelineDriver pipelineDriver=pipelineEnv.getPipelineDriver();
        SIDriver siDriver=pipelineEnv.getSIDriver();

        region=siDriver.transactionalPartition(conglomId,baseRegion);
        operationFactory = siDriver.getOperationFactory();
        exceptionFactory = pipelineDriver.exceptionFactory();
        config = pipelineEnv.configuration();
        tableFactory = siDriver.getTableFactory();
        try{
            DatabaseLifecycleManager.manager().registerService(new DatabaseLifecycleService(){
                @Override
                public void start() throws Exception{
                    factoryLoader = pipelineDriver.getContextFactoryLoader(conglomId);
                }

                @Override
                public void registerJMX(MBeanServer mbs) throws Exception{
                    pipelineDriver.registerJMX(mbs); //may already be registered, but why not?
                }

                @Override
                public void shutdown() throws Exception{
                    stop(e);
                }
            });
        }catch(Exception e1){
            throw new IOException(e1);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
        if (region != null)
            region.close();
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
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,InternalScanner scanner,ScanType scanType,CompactionRequest request) throws IOException{
        InternalScanner toReturn = scanner;
        for(CompactionObserver observer:compactionObservers){
            toReturn =observer.preCompact(e,store,toReturn,scanType,request);
        }
        return super.preCompact(e,store,toReturn,scanType,request);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                            Store store, StoreFile resultFile, CompactionRequest request) throws IOException {

        for(CompactionObserver observer:compactionObservers){
            observer.postCompact(e,store,resultFile,request);
        }
//        if (LOG.isTraceEnabled())
//            SpliceLogUtils.trace(LOG, "postCompact store=%s, storeFile=%s, request=%s", store, resultFile, request);
//
//
//        super.postCompact(e, store, resultFile, request);
    }


    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,byte[] splitRow) throws IOException{
        for(SplitObserver observer:splitObservers){
            observer.preSplit(c,splitRow);
        }
        super.preSplit(c,splitRow);
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException {
        for(SplitObserver observer:splitObservers){
            observer.postSplit(e,l,r);
        }
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
    

    @Override
	public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
			Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s)
			throws IOException {
        KeyValueScanner kvs = s;
        for(StoreScannerObserver obsever:storeScannerObservers){
            kvs = obsever.preStoreScannerOpen(c,store,scan,targetCols,s);
        }
		return super.preStoreScannerOpen(c, store, scan, targetCols, kvs);
	}	


	@Override
	public InternalScanner preFlush(
			ObserverContext<RegionCoprocessorEnvironment> e, Store store,
			InternalScanner scanner) throws IOException {
		SpliceLogUtils.trace(LOG, "preFlush called on store %s",store);
        InternalScanner toReturn = scanner;
        for(FlushObserver observer:flushObservers){
            scanner = observer.preFlush(e,store,scanner);
        }
		return super.preFlush(e, store, toReturn);
	}
	
	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        for(FlushObserver observer:flushObservers){
            observer.postFlush(e,null,null);
        }
    }

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile) throws IOException {
		SpliceLogUtils.trace(LOG, "postFlush called on store %s with file=%s",store, resultFile);
        for(FlushObserver observer:flushObservers){
            observer.postFlush(e,store,resultFile);
        }
	}
	
    @Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		SpliceLogUtils.trace(LOG, "preSplit");
        for(SplitObserver observer:splitObservers){
            observer.preSplit(e,null);
        }
    	super.preSplit(e);
	}

	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "postCompact store=%s, storeFile=%s", store, resultFile);
        super.postCompact(e, store, resultFile);
    }
}
