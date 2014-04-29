package com.splicemachine.derby.hbase;

import com.google.protobuf.Service;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.scheduler.SimpleThreadedTaskScheduler;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.BulkWrite;
import com.splicemachine.hbase.writer.BulkWriteResult;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.coprocessors.RollForwardQueueMap;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 *         Created on: 3/11/13
 */
public class SpliceIndexEndpoint extends com.splicemachine.coprocessor.SpliceMessage.SpliceIndexService implements org.apache.hadoop.hbase.coprocessor.CoprocessorService, org.apache.hadoop.hbase.Coprocessor {
    private static final Logger LOG = Logger.getLogger(SpliceIndexEndpoint.class);
    public static volatile int ipcReserved = 10;
    public static java.util.concurrent.ConcurrentMap<Long, org.apache.hadoop.hbase.util.Pair<com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger>> factoryMap = new NonBlockingHashMap<Long, Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger>>();

    static {
        factoryMap.put(-1l, Pair.newPair(LocalWriteContextFactory.unmanagedContextFactory(), new java.util.concurrent.atomic.AtomicInteger(1)));
    }

    protected static java.util.concurrent.atomic.AtomicInteger activeWriteThreads = new java.util.concurrent.atomic.AtomicInteger(0);
    private static volatile int maxWorkers = 10;
    private static volatile int flushQueueSizeBlock = 10; // needs to be configurable XXX TODO jleach
    private static volatile int compactionQueueSizeBlock = 1000; // needs to be configurable XXX TODO jleach
    private static MetricName receptionName = new MetricName("com.splicemachine", "receiverStats", "time");
    private static MetricName throughputMeterName = new MetricName("com.splicemachine", "receiverStats", "success");
    private static MetricName failedMeterName = new MetricName("com.splicemachine", "receiverStats", "failed");
    private static MetricName rejectedMeterName = new MetricName("com.splicemachine", "receiverStats", "rejected");
    private RegionCoprocessorEnvironment rce;
    private org.apache.hadoop.hbase.regionserver.MetricsRegionServerWrapper metrics;
    private long conglomId;
    private RollForwardQueue queue = null;
    private Timer timer = SpliceDriver.driver().getRegistry().newTimer(receptionName, java.util.concurrent.TimeUnit.MILLISECONDS, java.util.concurrent.TimeUnit.SECONDS);
    private Meter throughputMeter = SpliceDriver.driver().getRegistry().newMeter(throughputMeterName, "successfulRows", java.util.concurrent.TimeUnit.SECONDS);
    private Meter failedMeter = SpliceDriver.driver().getRegistry().newMeter(failedMeterName, "failedRows", java.util.concurrent.TimeUnit.SECONDS);
    private Meter rejectedMeter = SpliceDriver.driver().getRegistry().newMeter(rejectedMeterName, "rejectedRows", java.util.concurrent.TimeUnit.SECONDS);

    private static Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger> getContextPair(long conglomId) {
        Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger> ctxFactoryPair = factoryMap.get(conglomId);
        if (ctxFactoryPair == null) {
            ctxFactoryPair = Pair.newPair(new LocalWriteContextFactory(conglomId), new java.util.concurrent.atomic.AtomicInteger());
            Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger> existing = factoryMap.putIfAbsent(conglomId, ctxFactoryPair);
            if (existing != null) {
                ctxFactoryPair = existing;
            }
        }
        return ctxFactoryPair;
    }

    public static LocalWriteContextFactory getContextFactory(long baseConglomId) {
        Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger> ctxPair = getContextPair(baseConglomId);
        return ctxPair.getFirst();
    }

    public static void registerJMX(javax.management.MBeanServer mbs) throws javax.management.MalformedObjectNameException, javax.management.NotCompliantMBeanException, javax.management.InstanceAlreadyExistsException, javax.management.MBeanRegistrationException {
        javax.management.ObjectName coordinatorName = new javax.management.ObjectName("com.splicemachine.dery.hbase:type=ActiveWriteHandlers");
        mbs.registerMBean(ActiveWriteHandlers.get(), coordinatorName);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws java.io.IOException {
        rce = ((RegionCoprocessorEnvironment) env);
        HRegionServer rs = (HRegionServer) rce.getRegionServerServices();
        metrics = rs.getMetrics().getRegionServerWrapper();
        String tableName = rce.getRegion().getTableDesc().getNameAsString();
        try {
            conglomId = Long.parseLong(tableName);
        } catch (NumberFormatException nfe) {
            SpliceLogUtils.debug(LOG, "Unable to parse conglomerate id for table %s, " +
                    "index management for batch operations will be diabled", tableName);
            conglomId = -1;
            return;
        }
        maxWorkers = env.getConfiguration().getInt("splice.task.maxWorkers", SimpleThreadedTaskScheduler.DEFAULT_MAX_WORKERS);
        final Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger> factoryPair = Pair.newPair(new LocalWriteContextFactory(conglomId), new java.util.concurrent.atomic.AtomicInteger(1));
        Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger> originalPair = factoryMap.putIfAbsent(conglomId, factoryPair);
        if (originalPair != null) {
            //someone else already created the factory
            originalPair.getSecond().incrementAndGet();
        } else {
            SpliceDriver.Service service = new SpliceDriver.Service() {

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
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger> factoryPair = factoryMap.get(conglomId);
        if (factoryPair != null && factoryPair.getSecond().decrementAndGet() <= 0) {
            factoryMap.remove(conglomId);
        }
    }

    private boolean shouldAllowWrite() {
        if (metrics.getCompactionQueueSize() > compactionQueueSizeBlock || metrics.getFlushQueueSize() > flushQueueSizeBlock) {
            return false;
        }
        boolean allowed;
        do {
            int currentActive = activeWriteThreads.get();
            if (currentActive >= (SpliceConstants.ipcThreads - maxWorkers - ipcReserved))
                return false;
            allowed = activeWriteThreads.compareAndSet(currentActive, currentActive + 1);
        } while (!allowed);
        return true;
    }

    private WriteContext getWriteContext(String txnId, RegionCoprocessorEnvironment rce, RollForwardQueue queue, int writeSize) throws java.io.IOException, InterruptedException {
        Pair<LocalWriteContextFactory, java.util.concurrent.atomic.AtomicInteger> ctxFactoryPair = getContextPair(conglomId);
        return ctxFactoryPair.getFirst().create(txnId, rce, queue, writeSize);
    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void deleteFirstAfter(com.google.protobuf.RpcController controller, com.splicemachine.coprocessor.SpliceMessage.DeleteFirstAfterRequest request,
                                 com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.WriteResult> done) {
        // TODO: JPC - we don't need this method anymore. Remove from ProtoBuf
    }

    @Override
    public void bulkWrite(com.google.protobuf.RpcController rpcController, com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest bulkWriteRequest, com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse> callback) {
        com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.Builder writeResponse = com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse.newBuilder();
        try {
            writeResponse.setBytes(com.google.protobuf.ByteString.copyFrom(bulkWrite(bulkWriteRequest.getBytes().toByteArray())));
        } catch (java.io.IOException e) {
            org.apache.hadoop.hbase.protobuf.ResponseConverter.setControllerException(rpcController, e);
        }
        callback.run(writeResponse.build());
    }

    private byte[] bulkWrite(byte[] bulkWriteBytes) throws java.io.IOException {
        assert bulkWriteBytes != null;
        if (!shouldAllowWrite()) {
            rejectedMeter.mark();
            BulkWriteResult result = new BulkWriteResult(new WriteResult(WriteResult.Code.REGION_TOO_BUSY));
            return result.toBytes();
        }

        BulkWrite bulkWrite = BulkWrite.fromBytes(bulkWriteBytes);
        assert bulkWrite.getTxnId() != null;

//				SpliceLogUtils.trace(LOG,"batchMutate %s",bulkWrite);
        HRegion region = rce.getRegion();

        long start = System.nanoTime();
        try {
            region.startRegionOperation();
        } catch (java.io.IOException ioe) {
            activeWriteThreads.decrementAndGet();
            throw ioe;
        }

        try {
            WriteContext context;
            if (queue == null)
                queue = RollForwardQueueMap.lookupRollForwardQueue(rce.getRegion().getTableDesc()
                        .getNameAsString());
            try {
                context = getWriteContext(bulkWrite.getTxnId(), rce, queue, bulkWrite.getMutations().size());
            } catch (InterruptedException e) {
                //was interrupted while trying to create a write context.
                //we're done, someone else will have to write this batch
                throw new java.io.IOException(e);
            }

            Object[] bufferArray = bulkWrite.getBuffer();
            int size = bulkWrite.getSize();
            for (int i = 0; i < size; i++) {
                context.sendUpstream((KVPair) bufferArray[i]); //send all writes along the pipeline
            }
            java.util.Map<com.splicemachine.hbase.KVPair, com.splicemachine.hbase.writer.WriteResult> resultMap = context.finish();

            BulkWriteResult response = new BulkWriteResult();
            int failed = 0;
            for (int i = 0; i < size; i++) {
                @SuppressWarnings("RedundantCast") WriteResult result = resultMap.get((KVPair) bufferArray[i]);
                if (result.getCode() == WriteResult.Code.FAILED) {
                    failed++;
                }
                response.addResult(i, result);
            }

            SpliceLogUtils.trace(LOG, "Returning response %s", response);
            int numSuccessWrites = size - failed;
            throughputMeter.mark(numSuccessWrites);
            failedMeter.mark(failed);
            return response.toBytes();
        } finally {
            region.closeRegionOperation();
            activeWriteThreads.decrementAndGet();
            timer.update(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    @javax.management.MXBean
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

    public static class ActiveWriteHandlers implements ActiveWriteHandlersIface {
        private static final ActiveWriteHandlers INSTANCE = new ActiveWriteHandlers();

        private ActiveWriteHandlers() {
        }

        public static ActiveWriteHandlers get() {
            return INSTANCE;
        }

        @Override
        public int getActiveWriteThreads() {
            return activeWriteThreads.get();
        }

        @Override
        public int getIpcReservedPool() {
            return ipcReserved;
        }

        @Override
        public void setIpcReservedPool(int ipcReservedPool) {
            ipcReserved = ipcReservedPool;
        }

        @Override
        public int getFlushQueueSizeLimit() {
            return flushQueueSizeBlock;
        }

        @Override
        public void setFlushQueueSizeLimit(int flushQueueSizeLimit) {
            flushQueueSizeBlock = flushQueueSizeLimit;
        }

        @Override
        public int getCompactionQueueSizeLimit() {
            return compactionQueueSizeBlock;
        }

        @Override
        public void setCompactionQueueSizeLimit(int compactionQueueSizeLimit) {
            compactionQueueSizeBlock = compactionQueueSizeLimit;
        }
    }
}
