package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.MutationRequest;
import com.splicemachine.hbase.MutationResponse;
import com.splicemachine.hbase.MutationResult;
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
    public MutationResponse batchMutate(MutationRequest mutationsToApply) throws IOException {
        SpliceLogUtils.trace(LOG,"batchMutate %s",mutationsToApply);
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        WriteContext context;
        try {
            context = getWriteContext(rce);
        } catch (InterruptedException e) {
            //was interrupted while trying to create a write context.
            //we're done, someone else will have to write this batch
            throw new IOException(e);
        }
        for(Mutation mutation:mutationsToApply.getMutations()){
            context.sendUpstream(mutation); //send all writes along the pipeline
        }
        Map<Mutation,MutationResult> resultMap = context.finish();

        MutationResponse response = new MutationResponse();
        List<Mutation> mutations = mutationsToApply.getMutations();
        int pos=0;
        for(Mutation mutation:mutations){
            MutationResult result = resultMap.get(mutation);
            response.addResult(pos,result);
            pos++;
        }

        return response;
    }

    private WriteContext getWriteContext(RegionCoprocessorEnvironment rce) throws IOException, InterruptedException {
        Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = getContextPair(conglomId);
        return ctxFactoryPair.getFirst().create(rce);
    }


    @SuppressWarnings("unchecked")
	@Override
    public MutationResult deleteFirstAfter(String transactionId, byte[] rowKey, byte[] limit) throws IOException {
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
                mutation.setAttribute(IndexSet.INDEX_UPDATED,IndexSet.INDEX_ALREADY_UPDATED);
                if(mutation instanceof Put){
                    try{
                        @SuppressWarnings("deprecation")
                        OperationStatus[] statuses = region.put(new Pair[]{Pair.newPair((Put)mutation,null)});
                        OperationStatus status = statuses[0];
                        switch (status.getOperationStatusCode()) {
                            case NOT_RUN:
                                return MutationResult.notRun();
                            case SUCCESS:
                                return MutationResult.success();
                            default:
                                return new MutationResult(MutationResult.Code.FAILED,status.getExceptionMsg());
                        }
                    }catch(IOException ioe){
                        return new MutationResult(MutationResult.Code.FAILED,ioe.getMessage());
                    }
                }else{
                    try{
                        region.delete((Delete)mutation,true);
                        return MutationResult.success();
                    }catch(IOException ioe){
                        return new MutationResult(MutationResult.Code.FAILED,ioe.getMessage());
                    }
                }
            }else{
                //we've gone past our limit value without finding anything, so return notRun() to indicate
                //no action
                return MutationResult.notRun();
            }
        }while(shouldContinue);

        //no rows were found, so nothing to delete
        return MutationResult.notRun();
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
