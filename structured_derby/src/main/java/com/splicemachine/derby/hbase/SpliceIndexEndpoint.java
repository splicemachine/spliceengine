package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.scheduler.SimpleThreadedTaskScheduler;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.BulkWrite;
import com.splicemachine.hbase.writer.BulkWriteResult;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import javax.management.*;

import java.io.IOException;
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
    protected static AtomicInteger activeWriteThreads = new AtomicInteger(0); 
    public static volatile int ipcReserved = 10;
    private static volatile int maxWorkers = 10;
    private static volatile int flushQueueSizeBlock = SpliceConstants.flushQueueSizeBlock; 
    private static volatile int compactionQueueSizeBlock = SpliceConstants.compactionQueueSizeBlock;

    
    public static ConcurrentMap<Long,Pair<LocalWriteContextFactory,AtomicInteger>> factoryMap = new NonBlockingHashMap<Long, Pair<LocalWriteContextFactory, AtomicInteger>>();
    static{
        factoryMap.put(-1l,Pair.newPair(LocalWriteContextFactory.unmanagedContextFactory(),new AtomicInteger(1)));
    }

    private static MetricName receptionName = new MetricName("com.splicemachine","receiverStats","time");
    private static MetricName throughputMeterName = new MetricName("com.splicemachine","receiverStats","success");
    private static MetricName failedMeterName = new MetricName("com.splicemachine","receiverStats","failed");
    private static MetricName rejectedMeterName = new MetricName("com.splicemachine","receiverStats","rejected");

    private long conglomId;
		private TransactionalRegion region;

		private Timer timer=SpliceDriver.driver().getRegistry().newTimer(receptionName, TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
    private Meter throughputMeter = SpliceDriver.driver().getRegistry().newMeter(throughputMeterName, "successfulRows", TimeUnit.SECONDS);
    private Meter failedMeter =SpliceDriver.driver().getRegistry().newMeter(failedMeterName,"failedRows",TimeUnit.SECONDS);
    private Meter rejectedMeter =SpliceDriver.driver().getRegistry().newMeter(rejectedMeterName,"rejectedRows",TimeUnit.SECONDS);

		private RegionServerMetrics metrics;

		@Override
		public void start(CoprocessorEnvironment env) {
				final RegionCoprocessorEnvironment rce = ((RegionCoprocessorEnvironment)env);
				HRegionServer rs = (HRegionServer) rce.getRegionServerServices();
				metrics = rs.getMetrics();
				String tableName = rce.getRegion().getTableDesc().getNameAsString();
				try{
						conglomId = Long.parseLong(tableName);
				}catch(NumberFormatException nfe){
						SpliceLogUtils.debug(LOG, "Unable to parse conglomerate id for table %s, " +
										"index management for batch operations will be diabled",tableName);
						conglomId=-1;
						super.start(env);
						return;
				}
				maxWorkers = env.getConfiguration().getInt("splice.task.maxWorkers",SimpleThreadedTaskScheduler.DEFAULT_MAX_WORKERS);
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
                    region = TransactionalRegions.get(rce.getRegion(), new SegmentedRollForward.Action() {
                        @Override
                        public void submitAction(HRegion region, byte[] startKey, byte[] stopKey, SegmentedRollForward.Context context) {
                            throw new UnsupportedOperationException("IMPLEMENT");
                        }
                    });
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
		public byte[] bulkWrite(byte[] bulkWriteBytes) throws IOException {
				assert bulkWriteBytes!=null;
				if(!shouldAllowWrite()){
						rejectedMeter.mark();
						BulkWriteResult result = new BulkWriteResult(new WriteResult(WriteResult.Code.REGION_TOO_BUSY));
						return result.toBytes();
				}

				BulkWrite bulkWrite = BulkWrite.fromBytes(bulkWriteBytes);
				assert bulkWrite.getTxn()!=null;

//				SpliceLogUtils.trace(LOG,"batchMutate %s",bulkWrite);
				RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
				HRegion region = rce.getRegion();

				long start = System.nanoTime();
				try {
						region.startRegionOperation();
				} catch (IOException ioe) {
						activeWriteThreads.decrementAndGet();
						throw ioe;
				}

				try{
						WriteContext context;
//						if(queue==null)
//								queue = HTransactorFactory.getRollForward(region);
//								queue = RollForwardQueueMap.lookupRollForwardQueue(((RegionCoprocessorEnvironment) this.getEnvironment()).getRegion().getTableDesc().getNameAsString());
						try {
								context = getWriteContext(bulkWrite.getTxn(),bulkWrite.getMutations().size(),rce);
						} catch (InterruptedException e) {
								//was interrupted while trying to create a write context.
								//we're done, someone else will have to write this batch
								throw new IOException(e);
						}

						Object[] bufferArray = bulkWrite.getBuffer();
						int size = bulkWrite.getSize();
						for (int i = 0; i<size; i++) {
								context.sendUpstream((KVPair) bufferArray[i]); //send all writes along the pipeline
						}
						Map<KVPair,WriteResult> resultMap = context.finish();

						BulkWriteResult response = new BulkWriteResult();
						int failed=0;
						for (int i = 0; i<size; i++) {
								@SuppressWarnings("RedundantCast") WriteResult result = resultMap.get((KVPair)bufferArray[i]);
								if(result.getCode()== WriteResult.Code.FAILED){
										failed++;
								}
								response.addResult(i,result);
						}

						SpliceLogUtils.trace(LOG,"Returning response %s",response);
						int numSuccessWrites = size-failed;
						throughputMeter.mark(numSuccessWrites);
						failedMeter.mark(failed);
						return response.toBytes();
				}finally{
						region.closeRegionOperation();
						activeWriteThreads.decrementAndGet();
						timer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
				}
		}

		private boolean shouldAllowWrite() {
				if(metrics.compactionQueueSize.get()> compactionQueueSizeBlock || metrics.flushQueueSize.get()> flushQueueSizeBlock){
						return false;
				}
				boolean allowed;
				do{
						int currentActive = activeWriteThreads.get();
						if(currentActive>=(SpliceConstants.ipcThreads-maxWorkers-ipcReserved))
								return false;
						allowed = activeWriteThreads.compareAndSet(currentActive,currentActive+1);
				}while(!allowed);
				return true;
		}


		private WriteContext getWriteContext(TxnView txn, int writeSize, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = getContextPair(conglomId);
        return ctxFactoryPair.getFirst().create(txn,region, writeSize, env);
    }

    @SuppressWarnings("unchecked")
	@Override
    public WriteResult deleteFirstAfter(String transactionId, byte[] rowKey, byte[] limit) throws IOException {
        throw new UnsupportedOperationException("REMOVE");
//        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
//        final HRegion region = rce.getRegion();
//        Scan scan = SpliceUtils.createScan(transactionId);
//        scan.setStartRow(rowKey);
//        scan.setStopRow(limit);
//        //TODO -sf- make us only pull back one entry instead of everything
//        EntryPredicateFilter predicateFilter = EntryPredicateFilter.emptyPredicate();
//        scan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
//
//        RegionScanner scanner = region.getCoprocessorHost().preScannerOpen(scan);
//        if(scanner==null)
//            scanner = region.getScanner(scan);
//
//        List<KeyValue> row = Lists.newArrayList();
//        boolean shouldContinue;
//        do{
//            shouldContinue = scanner.next(row);
//            if(row.size()<=0) continue; //nothing returned
//
//            byte[] rowBytes =  row.get(0).getRow();
//            if(Bytes.compareTo(rowBytes,limit)<0){
//                Mutation mutation = Mutations.getDeleteOp(transactionId,rowBytes);
//                mutation.setAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
//                if(mutation instanceof Put){
//                    try{
//                        @SuppressWarnings("deprecation")
//                        OperationStatus[] statuses = region.put(new Pair[]{Pair.newPair((Put)mutation,null)});
//                        OperationStatus status = statuses[0];
//                        switch (status.getOperationStatusCode()) {
//                            case NOT_RUN:
//                                return WriteResult.notRun();
//                            case SUCCESS:
//                                return WriteResult.success();
//                            default:
//                                return WriteResult.failed(status.getExceptionMsg());
//                        }
//                    }catch(IOException ioe){
//                        return WriteResult.failed(ioe.getMessage());
//                    }
//                }else{
//                    try{
//                        region.delete((Delete)mutation,true);
//                        return WriteResult.success();
//                    }catch(IOException ioe){
//                        return WriteResult.failed(ioe.getMessage());
//                    }
//                }
//            }else{
//                //we've gone past our limit value without finding anything, so return notRun() to indicate
//                //no action
//                return WriteResult.notRun();
//            }
//        }while(shouldContinue);
//
//        //no rows were found, so nothing to delete
//        return WriteResult.notRun();
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

    public static void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName coordinatorName = new ObjectName("com.splicemachine.derby.hbase:type=ActiveWriteHandlers");
        mbs.registerMBean(ActiveWriteHandlers.get(),coordinatorName);
    }

		public static class ActiveWriteHandlers implements ActiveWriteHandlersIface {
				private static final ActiveWriteHandlers INSTANCE = new ActiveWriteHandlers();
				private  ActiveWriteHandlers () {}

				public static ActiveWriteHandlers get(){ return INSTANCE; }
				@Override public int getActiveWriteThreads() { return activeWriteThreads.get(); }
				@Override public int getIpcReservedPool() { return ipcReserved; }
				@Override public void setIpcReservedPool(int ipcReservedPool) { ipcReserved = ipcReservedPool; }
				@Override public int getFlushQueueSizeLimit() { return flushQueueSizeBlock; }
				@Override public void setFlushQueueSizeLimit(int flushQueueSizeLimit) { flushQueueSizeBlock = flushQueueSizeLimit; }
				@Override public int getCompactionQueueSizeLimit(){ return compactionQueueSizeBlock; }
				@Override public void setCompactionQueueSizeLimit(int compactionQueueSizeLimit) { compactionQueueSizeBlock = compactionQueueSizeLimit; }
		}

		@MXBean
		@SuppressWarnings("UnusedDeclaration")
		public interface ActiveWriteHandlersIface {
				public int getActiveWriteThreads();
				public int getIpcReservedPool();
				public void setIpcReservedPool(int rpcReservedPool);
				public int getFlushQueueSizeLimit();
				public void setFlushQueueSizeLimit(int flushQueueSizeLimit);
				public int getCompactionQueueSizeLimit();
				public void setCompactionQueueSizeLimit(int compactionQueueSizeLimit);
		}

}
