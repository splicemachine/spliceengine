package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class SpliceIndexEndpoint extends BaseEndpointCoprocessor implements BatchProtocol{
    private static final Logger LOG = Logger.getLogger(SpliceIndexEndpoint.class);

    public static ConcurrentMap<Long,Pair<LocalWriteContextFactory,AtomicInteger>> factoryMap = new ConcurrentHashMap<Long, Pair<LocalWriteContextFactory, AtomicInteger>>();
    static{
        factoryMap.put(-1l,Pair.newPair(LocalWriteContextFactory.unmanagedContextFactory(),new AtomicInteger(1)));
    }

    private long conglomId;

    @Override
    public void start(CoprocessorEnvironment env) {
        String tableName = ((RegionCoprocessorEnvironment)env).getRegion().getTableDesc().getNameAsString();
        try{
            conglomId = Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.debug(LOG, "Unable to parse conglomerate id for table %s, " +
                    "index management for batch operations will be diabled",tableName);
            conglomId=-1;
            super.start(env);
            return;
        }

        final Pair<LocalWriteContextFactory,AtomicInteger> factoryPair = Pair.newPair(new LocalWriteContextFactory(conglomId),new AtomicInteger(1));
        Pair<LocalWriteContextFactory, AtomicInteger> originalPair = factoryMap.putIfAbsent(conglomId, factoryPair);
        if(originalPair!=null){
            //someone else already created the factory
            originalPair.getSecond().incrementAndGet();
        }else{
            SpliceDriver.Service service = new SpliceDriver.Service(){

                @Override
                public boolean start() {
                    factoryPair.getFirst().prepare();
                    SpliceDriver.driver().deregisterService(this);
                    return true;
                }

                @Override
                public boolean shutdown() {
                    return true;
                }
            };
            SpliceDriver.driver().registerService(service);
        }
        super.start(env);
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        Pair<LocalWriteContextFactory,AtomicInteger> factoryPair = factoryMap.get(conglomId);
        if(factoryPair!=null &&  factoryPair.getSecond().decrementAndGet()<=0){
            factoryMap.remove(conglomId);
        }
    }

    @Override
    public BulkWriteResult bulkWrite(BulkWrite bulkWrite) throws IOException {
        SpliceLogUtils.trace(LOG,"batchMutate %s",bulkWrite);
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        HRegion region = rce.getRegion();
        region.startRegionOperation();
        try{
            WriteContext context;
            try {
                context = getWriteContext(bulkWrite.getTxnId(),rce);
            } catch (InterruptedException e) {
                //was interrupted while trying to create a write context.
                //we're done, someone else will have to write this batch
                throw new IOException(e);
            }
            for(KVPair mutation:bulkWrite.getMutations()){
//                byte[] row = mutation.getRow();
//                long time = uniqueChecker.check(row);
//                if(time>0){
//                    LOG.error("mutated row "+ BytesUtil.toHex(row)+" at time "+time);
//                }
                context.sendUpstream(mutation); //send all writes along the pipeline
            }
            Map<KVPair,WriteResult> resultMap = context.finish();

            BulkWriteResult response = new BulkWriteResult();
            List<KVPair> mutations = bulkWrite.getMutations();
            int pos=0;
            for(KVPair mutation:mutations){
                WriteResult result = resultMap.get(mutation);
                response.addResult(pos,result);
                pos++;
            }

            return response;
        }finally{
            region.closeRegionOperation();
        }
    }


    private WriteContext getWriteContext(String txnId,RegionCoprocessorEnvironment rce) throws IOException, InterruptedException {
        Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = getContextPair(conglomId);
        return ctxFactoryPair.getFirst().create(txnId,rce);
    }



    @SuppressWarnings("unchecked")
	@Override
    public WriteResult deleteFirstAfter(String transactionId, byte[] rowKey, byte[] limit) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        final HRegion region = rce.getRegion();
        Scan scan = SpliceUtils.createScan(transactionId);
        scan.setStartRow(rowKey);
        scan.setStopRow(limit);
//        scan.setCaching(1);
//        scan.setBatch(1);
        //TODO -sf- make us only pull back one entry instead of everything
        EntryPredicateFilter predicateFilter = EntryPredicateFilter.emptyPredicate();
        scan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());

        RegionScanner scanner = region.getCoprocessorHost().preScannerOpen(scan);
        if(scanner==null)
            scanner = region.getScanner(scan);

        List<KeyValue> row = Lists.newArrayList();
        boolean shouldContinue;
        do{
            shouldContinue = scanner.next(row);
            if(row.size()<=0) continue; //nothing returned

            byte[] rowBytes =  row.get(0).getRow();
            if(Bytes.compareTo(rowBytes,limit)<0){
                Mutation mutation = Mutations.getDeleteOp(transactionId,rowBytes);
                mutation.setAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
                if(mutation instanceof Put){
                    try{
                        @SuppressWarnings("deprecation")
                        OperationStatus[] statuses = region.put(new Pair[]{Pair.newPair((Put)mutation,null)});
                        OperationStatus status = statuses[0];
                        switch (status.getOperationStatusCode()) {
                            case NOT_RUN:
                                return WriteResult.notRun();
                            case SUCCESS:
                                return WriteResult.success();
                            default:
                                return WriteResult.failed(status.getExceptionMsg());
                        }
                    }catch(IOException ioe){
                        return WriteResult.failed(ioe.getMessage());
                    }
                }else{
                    try{
                        region.delete((Delete)mutation,true);
                        return WriteResult.success();
                    }catch(IOException ioe){
                        return WriteResult.failed(ioe.getMessage());
                    }
                }
            }else{
                //we've gone past our limit value without finding anything, so return notRun() to indicate
                //no action
                return WriteResult.notRun();
            }
        }while(shouldContinue);

        //no rows were found, so nothing to delete
        return WriteResult.notRun();
    }

    private static Pair<LocalWriteContextFactory, AtomicInteger> getContextPair(long conglomId) {
        Pair<LocalWriteContextFactory,AtomicInteger> ctxFactoryPair = factoryMap.get(conglomId);
        if(ctxFactoryPair==null){
            ctxFactoryPair = Pair.newPair(new LocalWriteContextFactory(conglomId),new AtomicInteger());
            Pair<LocalWriteContextFactory, AtomicInteger> existing = factoryMap.putIfAbsent(conglomId, ctxFactoryPair);
            if(existing!=null){
                ctxFactoryPair = existing;
            }
        }
        return ctxFactoryPair;
    }

    public static LocalWriteContextFactory getContextFactory(long baseConglomId) {
        Pair<LocalWriteContextFactory,AtomicInteger> ctxPair = getContextPair(baseConglomId);
        return ctxPair.getFirst();
    }
}
