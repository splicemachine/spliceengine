package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.BulkWritesResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.pipeline.writehandler.IndexWriteBufferFactory;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.management.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */


public class SpliceBaseIndexEndpoint {
		private static final Logger LOG = Logger.getLogger(SpliceBaseIndexEndpoint.class);
		public static volatile int ipcReserved = 10;
		private static final SpliceWriteControl writeControl;
    static{
        int ipcThreads = SpliceConstants.ipcThreads-SpliceConstants.taskWorkers-ipcReserved;
        int maxIndependentWrites = SpliceConstants.maxIndependentWrites;
        int maxDependentWrites = SpliceConstants.maxDependentWrites;
        writeControl = new SpliceWriteControl((int)ipcThreads/2,(int)ipcThreads/2,maxDependentWrites,maxIndependentWrites);
    }

		private static MetricName receptionName = new MetricName("com.splicemachine","receiverStats","time");
		private static MetricName rejectedMeterName = new MetricName("com.splicemachine","receiverStats","rejected");

		private long conglomId;
		private TransactionalRegion region;

		private Timer timer=SpliceDriver.driver().getRegistry().newTimer(receptionName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
		private Meter rejectedMeter =SpliceDriver.driver().getRegistry().newMeter(rejectedMeterName,"rejectedRows",TimeUnit.SECONDS);

        private static final RegionWritePipeline.PipelineMeters pipelineMeter = new RegionWritePipeline.PipelineMeters();

    private RegionWritePipeline regionWritePipeline;
    private static final AtomicLong rejectedCount = new AtomicLong(0l);

    private RegionCoprocessorEnvironment rce;

    public void start(CoprocessorEnvironment env) {
        rce = ((RegionCoprocessorEnvironment)env);
        String tableName = rce.getRegion().getTableDesc().getNameAsString();
        final WriteContextFactory<TransactionalRegion> factory;
        try{
            conglomId = Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.debug(LOG, "Unable to parse conglomerate id for table %s, " +
                    "index management for batch operations will be diabled",tableName);
            conglomId=-1;
        }
        factory = PipelineContextFactories.getWriteContext(conglomId);

        Service service = new Service() {
            @Override public boolean shutdown() { return true; }
            @Override
            public boolean start() {
                factory.prepare();
                if(conglomId>=0){
                    region = TransactionalRegions.get(rce.getRegion());
                }else{
                    region = TransactionalRegions.nonTransactionalRegion(rce.getRegion());
                }
                regionWritePipeline = new RegionWritePipeline(rce,rce.getRegion(),factory,region,pipelineMeter);
                SpliceDriver.driver().deregisterService(this);
                return true;
            }
        };
        SpliceDriver.driver().registerService(service);
    }

    public void stop(CoprocessorEnvironment env) {
    	if (regionWritePipeline != null)
    		regionWritePipeline.close();
    }


    public BulkWritesResult bulkWrite(BulkWrites bulkWrites) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "bulkWrite %s ",bulkWrites);
        BulkWritesResult result = new BulkWritesResult();
        Object[] buffer = bulkWrites.getBulkWrites().buffer;
        int size =  bulkWrites.getBulkWrites().size();
        long start = System.nanoTime();
        // start
//				List<Pair<BulkWriteResult,RegionWritePipeline>> startPoints = Lists.newArrayListWithExpectedSize(size);
        WriteBufferFactory indexWriteBufferFactory = new IndexWriteBufferFactory();
        boolean dependent = regionWritePipeline.isDependent();
        SpliceWriteControl.Status status = null;
        int kvPairSize = (int) bulkWrites.getKVPairSize();
        try {
        	status = (dependent)?writeControl.performDependentWrite(kvPairSize):writeControl.performIndependentWrite(kvPairSize);
            if(status.equals(SpliceWriteControl.Status.REJECTED)){
                //we cannot write to this
                rejectAll(result,size);
                rejectedCount.addAndGet(size);
                return result;
            }
            Map<BulkWrite,Pair<BulkWriteResult,RegionWritePipeline>> writePairMap=Maps.newIdentityHashMap();
            List<BulkWrite> toWrite = Lists.newArrayListWithExpectedSize(size);
            for(int i=0;i<size;i++){
                BulkWrite bulkWrite = (BulkWrite) buffer[i];
                assert bulkWrite!=null;
                RegionWritePipeline writePipeline = SpliceDriver.driver().getWritePipeline(bulkWrite.getEncodedStringName());
                BulkWriteResult r;
                if(writePipeline==null){
                    if (LOG.isTraceEnabled())
                        SpliceLogUtils.trace(LOG, "endpoint not found for region %s on region %s", bulkWrite.getEncodedStringName(), rce.getRegion().getRegionNameAsString());
                    r = new BulkWriteResult(WriteResult.notServingRegion());
                }else{
                    //we might be able to write this one
                    r = new BulkWriteResult();
                    toWrite.add(bulkWrite);
                }
                writePairMap.put(bulkWrite, Pair.newPair(r, writePipeline));
            }
            assert writePairMap.size()==size: "Some BulkWrites were not added to the writePairMap";
            for (int i =0;i<toWrite.size(); i++) {
                BulkWrite next = toWrite.get(i);
                Pair<BulkWriteResult,RegionWritePipeline> writePair = writePairMap.get(next);
                BulkWriteResult newR = writePair.getSecond().submitBulkWrite(next,indexWriteBufferFactory,writePair.getSecond().getRegionCoprocessorEnvironment());
                writePair.setFirst(newR);
            }
            //complete the writes
            for(Map.Entry<BulkWrite,Pair<BulkWriteResult,RegionWritePipeline>> entry:writePairMap.entrySet()){
                Pair<BulkWriteResult,RegionWritePipeline> pair = entry.getValue();
                if(pair.getSecond()!=null){
                    BulkWriteResult e = pair.getSecond().finishWrite(pair.getFirst(), entry.getKey());
                    pair.setFirst(e);
                }
            }
            for(int i=0;i< size;i++){
                BulkWrite bw = (BulkWrite)buffer[i];
                Pair<BulkWriteResult,RegionWritePipeline> pair = writePairMap.get(bw);
                result.addResult(pair.getFirst());
            }
            timer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
            return result;
        } finally {
        	switch (status) {
			case REJECTED:
				break;
			case DEPENDENT:
				writeControl.finishDependentWrite(kvPairSize);
				break;
			case INDEPENDENT:
				writeControl.finishIndependentWrite(kvPairSize);						
				break;
        
        	}
        }
    }
		private void rejectAll(BulkWritesResult result, int numResults) {
				this.rejectedMeter.mark();
				for (int i = 0; i < numResults; i++) {
						result.addResult(new BulkWriteResult(WriteResult.pipelineTooBusy(rce.getRegion().getRegionNameAsString())));
				}
		}

    public byte[] bulkWrites(byte[] bulkWriteBytes) throws IOException {
        assert bulkWriteBytes != null;
        BulkWrites bulkWrites = PipelineUtils.fromCompressedBytes(bulkWriteBytes, BulkWrites.class);
        return PipelineUtils.toCompressedBytes(bulkWrite(bulkWrites));
    }

		public static void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
				ObjectName coordinatorName = new ObjectName("com.splicemachine.derby.hbase:type=ActiveWriteHandlers");
				mbs.registerMBean(ActiveWriteHandlers.get(),coordinatorName);
		}

    public RegionWritePipeline getWritePipeline() {
        return regionWritePipeline;
    }

    public static class ActiveWriteHandlers implements ActiveWriteHandlersIface {
        private static final ActiveWriteHandlers INSTANCE = new ActiveWriteHandlers();
        private  ActiveWriteHandlers () {}

        public static ActiveWriteHandlers get(){ return INSTANCE; }
        @Override public int getIpcReservedPool() { return ipcReserved; }
        @Override public int getMaxDependentWriteThreads() { return writeControl.maxDependentWriteThreads; }
        @Override public int getMaxIndependentWriteThreads() { return writeControl.maxIndependentWriteThreads; }
        @Override public int getMaxDependentWriteCount() { return writeControl.maxDependentWriteCount; }
        @Override public int getMaxIndependentWriteCount() { return writeControl.maxIndependentWriteCount; }

        @Override public double getOverallAvgThroughput() { return pipelineMeter.throughput(); }
        @Override public double get1MThroughput() { return pipelineMeter.oneMThroughput(); }
        @Override public double get5MThroughput() { return pipelineMeter.fiveMThroughput(); }
        @Override public double get15MThroughput() { return pipelineMeter.fifteenMThroughput(); }
        @Override public long getTotalRejected() { return rejectedCount.get(); }

        
        @Override
        public void setMaxIndependentWriteThreads(int newMaxIndependentWriteThreads) {
            writeControl.maxIndependentWriteCount = newMaxIndependentWriteThreads;
        }

        @Override
        public void setMaxDependentWriteThreads(int newMaxDependentWriteThreads) {
            writeControl.maxDependentWriteCount = newMaxDependentWriteThreads;
        }

        @Override
        public void setMaxIndependentWriteCount(int newMaxIndependentWriteCount) {
            writeControl.maxIndependentWriteCount = newMaxIndependentWriteCount;
        }

        @Override
        public void setMaxDependentWriteCount(int newMaxDependentWriteCount) {
            writeControl.maxDependentWriteCount = newMaxDependentWriteCount;
        }

		@Override
		public int getDependentWriteCount() {
			return writeControl.getWriteStatus().get().getDependentWriteCount();
		}

		@Override
		public int getDependentWriteThreads() {
			return writeControl.getWriteStatus().get().getDependentWriteThreads();
		}
		@Override
		public int getIndependentWriteCount() {
			return writeControl.getWriteStatus().get().getIndependentWriteCount();
		}

		@Override
		public int getIndependentWriteThreads() {
			return writeControl.getWriteStatus().get().getIndependentWriteThreads();
		}

    }

    @MXBean
    @SuppressWarnings("UnusedDeclaration")
    public interface ActiveWriteHandlersIface {
        public int getIpcReservedPool();
        int getIndependentWriteThreads();
		int getIndependentWriteCount();
		int getDependentWriteThreads();
		int getDependentWriteCount();
		void setMaxDependentWriteCount(int newMaxDependentWriteCount);
		void setMaxIndependentWriteCount(int newMaxIndependentWriteCount);
		void setMaxDependentWriteThreads(int newMaxDependentWriteThreads);
		void setMaxIndependentWriteThreads(int newMaxIndependentWriteThreads);
		int getMaxIndependentWriteCount();
		int getMaxDependentWriteCount();
		int getMaxIndependentWriteThreads();
		int getMaxDependentWriteThreads();
        public double getOverallAvgThroughput();
        public double get1MThroughput();
        public double get5MThroughput();
        public double get15MThroughput();
        long getTotalRejected();
    }

}