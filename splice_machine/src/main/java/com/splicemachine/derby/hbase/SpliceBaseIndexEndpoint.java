package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.scheduler.SimpleThreadedTaskScheduler;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.BulkWritesResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.pipeline.writecontextfactory.LocalWriteContextFactory;
import com.splicemachine.pipeline.writehandler.IndexWriteBufferFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import javax.management.*;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
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


public class SpliceBaseIndexEndpoint {
		private static final Logger LOG = Logger.getLogger(SpliceBaseIndexEndpoint.class);
		public static volatile int ipcReserved = 10;
		private static volatile int taskWorkers = SpliceConstants.taskWorkers;
		private static volatile int flushQueueSizeBlock = SpliceConstants.flushQueueSizeBlock;
		private static volatile int compactionQueueSizeBlock = SpliceConstants.compactionQueueSizeBlock;
		private static volatile WriteSemaphore control = new WriteSemaphore((SpliceConstants.ipcThreads-taskWorkers-ipcReserved)/2,(SpliceConstants.ipcThreads-taskWorkers-ipcReserved)/2,SpliceConstants.maxDependentWrites,SpliceConstants.maxIndependentWrites);

		private static MetricName receptionName = new MetricName("com.splicemachine","receiverStats","time");
		private static MetricName rejectedMeterName = new MetricName("com.splicemachine","receiverStats","rejected");

		private long conglomId;
		private TransactionalRegion region;

		private Timer timer=SpliceDriver.driver().getRegistry().newTimer(receptionName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
		private Meter rejectedMeter =SpliceDriver.driver().getRegistry().newMeter(rejectedMeterName,"rejectedRows",TimeUnit.SECONDS);

        private RegionWritePipeline.PipelineMeters pipelineMeter = new RegionWritePipeline.PipelineMeters();

    private RegionWritePipeline regionWritePipeline;

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
                regionWritePipeline = new RegionWritePipeline(rce.getRegion(),factory,region,pipelineMeter);
                SpliceDriver.driver().deregisterService(this);
                return true;
            }
        };
        SpliceDriver.driver().registerService(service);
    }

    public void stop(CoprocessorEnvironment env) {
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
        List<Pair<BulkWriteResult,RegionWritePipeline>> startPoints = Lists.newArrayListWithExpectedSize(size);
        WriteBufferFactory indexWriteBufferFactory = new IndexWriteBufferFactory();
        boolean dependent = regionWritePipeline.isDependent();
        WriteSemaphore.Status status;
        int kvPairSize = bulkWrites.numEntries();
        status = (dependent)?control.acquireDependentPermit(kvPairSize):control.acquireIndependentPermit(kvPairSize);
        if(status== WriteSemaphore.Status.REJECTED) {
            rejectAll(result, size);
            return result;
        }

        try {
            for (int i = 0; i< size; i++) {
                BulkWrite bulkWrite = (BulkWrite) buffer[i];
                assert bulkWrite!=null;
                // Grab the instances endpoint and not this one
                RegionWritePipeline writePipeline = SpliceDriver.driver().getWritePipeline(bulkWrite.getEncodedStringName());
                if (writePipeline == null) {
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG, "endpoint not found for region %s on region %s",bulkWrite.getEncodedStringName(), rce.getRegion().getRegionNameAsString());
                    startPoints.add(Pair.<BulkWriteResult,RegionWritePipeline>newPair(new BulkWriteResult(WriteResult.notServingRegion()),null));
                }
                else {
                    startPoints.add(Pair.newPair(writePipeline.submitBulkWrite(bulkWrite, indexWriteBufferFactory, rce), writePipeline));
                }
            }

            // complete
            for (int i = 0; i< size; i++) {
                BulkWrite bulkWrite = (BulkWrite) buffer[i];
                Pair<BulkWriteResult,RegionWritePipeline> pair = startPoints.get(i);
                if(pair.getSecond()==null)
                    result.addResult(pair.getFirst());
                else
                    result.addResult(pair.getSecond().finishWrite(pair.getFirst(),bulkWrite));
            }
            timer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
            return result;
        } finally {
            switch (status) {
                case REJECTED:
                    break;
                case DEPENDENT:
                    control.releaseDependentPermit(kvPairSize);
                    break;
                case INDEPENDENT:
                    control.releaseIndependentPermit(kvPairSize);
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
				@Override public void setIpcReservedPool(int ipcReservedPool) { ipcReserved = ipcReservedPool; }
				@Override public int getFlushQueueSizeLimit() { return flushQueueSizeBlock; }
				@Override public void setFlushQueueSizeLimit(int flushQueueSizeLimit) { flushQueueSizeBlock = flushQueueSizeLimit; }
				@Override public int getCompactionQueueSizeLimit(){ return compactionQueueSizeBlock; }
				@Override public void setCompactionQueueSizeLimit(int compactionQueueSizeLimit) { compactionQueueSizeBlock = compactionQueueSizeLimit; }
				@Override public int getDependentWriteThreads() { return control.getDependentThreadCount(); }
				@Override public int getIndependentWriteThreads() { return control.getIndependentThreadCount(); }
				@Override public int getDependentWriteCount() {return control.getDependentRowPermitCount(); }
				@Override public int getIndependentWriteCount() { return control.getIndependentRowPermitCount(); }



		}

		@MXBean
		@SuppressWarnings("UnusedDeclaration")
		public interface ActiveWriteHandlersIface {
				public int getIpcReservedPool();
				public void setIpcReservedPool(int rpcReservedPool);
				public int getFlushQueueSizeLimit();
				public void setFlushQueueSizeLimit(int flushQueueSizeLimit);
				public int getCompactionQueueSizeLimit();
				public void setCompactionQueueSizeLimit(int compactionQueueSizeLimit);
				public int getDependentWriteThreads();
				public int getIndependentWriteThreads();
				public int getDependentWriteCount();
				public int getIndependentWriteCount();
		}

}