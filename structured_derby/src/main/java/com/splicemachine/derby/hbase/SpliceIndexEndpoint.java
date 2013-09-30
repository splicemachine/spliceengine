package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.BulkWrite;
import com.splicemachine.hbase.writer.BulkWriteResult;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.coprocessors.RollForwardQueueMap;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
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
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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

    public static ConcurrentMap<Long,Pair<LocalWriteContextFactory,AtomicInteger>> factoryMap = new NonBlockingHashMap<Long, Pair<LocalWriteContextFactory, AtomicInteger>>();
    static{
        factoryMap.put(-1l,Pair.newPair(LocalWriteContextFactory.unmanagedContextFactory(),new AtomicInteger(1)));
    }

    private static MetricName receptionName = new MetricName("com.splicemachine","receiverStats","time");
    private static MetricName throughputMeterName = new MetricName("com.splicemachine","receiverStats","success");
    private static MetricName failedMeterName = new MetricName("com.splicemachine","receiverStats","failed");

    private long conglomId;

    private RollForwardQueue<byte[],ByteBuffer> queue;
    private Timer timer=SpliceDriver.driver().getRegistry().newTimer(receptionName, TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
    private Meter throughputMeter = SpliceDriver.driver().getRegistry().newMeter(throughputMeterName,"successfulRows",TimeUnit.SECONDS);
    private Meter failedMeter =SpliceDriver.driver().getRegistry().newMeter(failedMeterName,"failedRows",TimeUnit.SECONDS);

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
        assert bulkWrite!=null;
        assert bulkWrite.getTxnId()!=null;

        SpliceLogUtils.trace(LOG,"batchMutate %s",bulkWrite);
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        HRegion region = rce.getRegion();
        region.startRegionOperation();
        long start = System.nanoTime();
        try{
            WriteContext context;
            if(queue==null)
                queue = RollForwardQueueMap.lookupRollForwardQueue(((RegionCoprocessorEnvironment) this.getEnvironment()).getRegion().getTableDesc().getNameAsString());
            try {
                context = getWriteContext(bulkWrite.getTxnId(),rce,queue,bulkWrite.getMutations().size());
            } catch (InterruptedException e) {
                //was interrupted while trying to create a write context.
                //we're done, someone else will have to write this batch
                throw new IOException(e);
            }
            for(KVPair mutation:bulkWrite.getMutations()){
                context.sendUpstream(mutation); //send all writes along the pipeline
            }
            Map<KVPair,WriteResult> resultMap = context.finish();

            BulkWriteResult response = new BulkWriteResult();
            List<KVPair> mutations = bulkWrite.getMutations();
            int pos=0;
            int failed=0;
            for(KVPair mutation:mutations){
                WriteResult result = resultMap.get(mutation);
                if(result.getCode()== WriteResult.Code.FAILED){
                    failed++;
                }
                response.addResult(pos,result);
                pos++;
            }

            int numSuccessWrites = bulkWrite.getMutations().size()-failed;
            throughputMeter.mark(numSuccessWrites);
            failedMeter.mark(failed);
            return response;
        }finally{
            region.closeRegionOperation();
            timer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
        }
    }


    private WriteContext getWriteContext(String txnId,RegionCoprocessorEnvironment rce,RollForwardQueue<byte[],ByteBuffer> queue,int writeSize) throws IOException, InterruptedException {
        Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = getContextPair(conglomId);
        return ctxFactoryPair.getFirst().create(txnId,rce,queue,writeSize);
    }

    @SuppressWarnings("unchecked")
	@Override
    public WriteResult deleteFirstAfter(String transactionId, byte[] rowKey, byte[] limit) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        final HRegion region = rce.getRegion();
        Scan scan = SpliceUtils.createScan(transactionId);
        scan.setStartRow(rowKey);
        scan.setStopRow(limit);
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
