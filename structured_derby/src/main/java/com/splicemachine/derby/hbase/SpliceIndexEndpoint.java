package com.splicemachine.derby.hbase;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerWrapper;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.coprocessor.SpliceMessage.BulkWriteRequest;
import com.splicemachine.coprocessor.SpliceMessage.BulkWriteResponse;
import com.splicemachine.coprocessor.SpliceMessage.SpliceIndexService;
import com.splicemachine.derby.impl.job.scheduler.SimpleThreadedTaskScheduler;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.BulkWrite;
import com.splicemachine.hbase.writer.BulkWriteResult;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.coprocessors.RollForwardQueueMap;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 *         Created on: 3/11/13
 */
public class SpliceIndexEndpoint extends SpliceIndexService implements CoprocessorService, Coprocessor {
    private static final Logger LOG = Logger.getLogger(SpliceIndexEndpoint.class);
    public static volatile int ipcReserved = 10;
    public static ConcurrentMap<Long, Pair<LocalWriteContextFactory, AtomicInteger>> factoryMap = new
            NonBlockingHashMap<Long, Pair<LocalWriteContextFactory, AtomicInteger>>();

    static {
        factoryMap.put(-1l, Pair.newPair(LocalWriteContextFactory.unmanagedContextFactory(), new AtomicInteger(1)));
    }

    protected static AtomicInteger activeWriteThreads = new AtomicInteger(0);
    private static volatile int maxWorkers = 10;
    private static volatile int flushQueueSizeBlock = 10; // needs to be configurable XXX TODO jleach
    private static volatile int compactionQueueSizeBlock = 1000; // needs to be configurable XXX TODO jleach
    private static MetricName receptionName = new MetricName("com.splicemachine", "receiverStats", "time");
    private Timer timer = SpliceDriver.driver().getRegistry().newTimer(receptionName, TimeUnit.MILLISECONDS,
                                                                       TimeUnit.SECONDS);
    private static MetricName throughputMeterName = new MetricName("com.splicemachine", "receiverStats", "success");
    private Meter throughputMeter = SpliceDriver.driver().getRegistry().newMeter(throughputMeterName,
                                                                                 "successfulRows", TimeUnit.SECONDS);
    private static MetricName failedMeterName = new MetricName("com.splicemachine", "receiverStats", "failed");
    private Meter failedMeter = SpliceDriver.driver().getRegistry().newMeter(failedMeterName, "failedRows",
                                                                             TimeUnit.SECONDS);
    private static MetricName rejectedMeterName = new MetricName("com.splicemachine", "receiverStats", "rejected");
    private Meter rejectedMeter = SpliceDriver.driver().getRegistry().newMeter(rejectedMeterName, "rejectedRows",
                                                                               TimeUnit.SECONDS);
    RegionCoprocessorEnvironment rce;
    private MetricsRegionServerWrapper metrics;
    private long conglomId;
    private RollForwardQueue queue = null;

    private static Pair<LocalWriteContextFactory, AtomicInteger> getContextPair(long conglomId) {
        Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = factoryMap.get(conglomId);
        if (ctxFactoryPair == null) {
            ctxFactoryPair = Pair.newPair(new LocalWriteContextFactory(conglomId), new AtomicInteger());
            Pair<LocalWriteContextFactory, AtomicInteger> existing = factoryMap.putIfAbsent(conglomId, ctxFactoryPair);
            if (existing != null) {
                ctxFactoryPair = existing;
            }
        }
        return ctxFactoryPair;
    }

    public static LocalWriteContextFactory getContextFactory(long baseConglomId) {
        Pair<LocalWriteContextFactory, AtomicInteger> ctxPair = getContextPair(baseConglomId);
        return ctxPair.getFirst();
    }

    public static void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException,
            InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName coordinatorName = new ObjectName("com.splicemachine.dery.hbase:type=ActiveWriteHandlers");
        mbs.registerMBean(ActiveWriteHandlers.get(), coordinatorName);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
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
        maxWorkers = env.getConfiguration().getInt("splice.task.maxWorkers",
                                                   SimpleThreadedTaskScheduler.DEFAULT_MAX_WORKERS);
        final Pair<LocalWriteContextFactory, AtomicInteger> factoryPair = Pair.newPair(new LocalWriteContextFactory
                                                                                               (conglomId),
                                                                                       new AtomicInteger(1));
        Pair<LocalWriteContextFactory, AtomicInteger> originalPair = factoryMap.putIfAbsent(conglomId, factoryPair);
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
        Pair<LocalWriteContextFactory, AtomicInteger> factoryPair = factoryMap.get(conglomId);
        if (factoryPair != null && factoryPair.getSecond().decrementAndGet() <= 0) {
            factoryMap.remove(conglomId);
        }
    }

    public byte[] bulkWrite(byte[] bulkWriteBytes) throws IOException {
        assert bulkWriteBytes != null;
        if (!shouldAllowWrite()) {
            rejectedMeter.mark();
            BulkWriteResult result = new BulkWriteResult(new WriteResult(WriteResult.Code.REGION_TOO_BUSY));
            return result.toBytes();
        }

        BulkWrite bulkWrite = BulkWrite.fromBytes(bulkWriteBytes);
        assert bulkWrite.getTxnId() != null;

        SpliceLogUtils.trace(LOG, "batchMutate %s", bulkWrite);
        HRegion region = rce.getRegion();

        long start = System.nanoTime();
        try {
            region.startRegionOperation();
        } catch (IOException ioe) {
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
                throw new IOException(e);
            }

            Object[] bufferArray = bulkWrite.getBuffer();
            int size = bulkWrite.getSize();
            for (int i = 0; i < size; i++) {
                context.sendUpstream((KVPair) bufferArray[i]); //send all writes along the pipeline
            }
            Map<KVPair, WriteResult> resultMap = context.finish();

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
            timer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    public WriteResult deleteFirstAfter(String transactionId, byte[] rowKey, byte[] limit) throws IOException {
        final HRegion region = rce.getRegion();
        Scan scan = SpliceUtils.createScan(transactionId);
        scan.setStartRow(rowKey);
        scan.setStopRow(limit);
        //TODO -sf- make us only pull back one entry instead of everything
        EntryPredicateFilter predicateFilter = EntryPredicateFilter.emptyPredicate();
        scan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL, predicateFilter.toBytes());

        RegionScanner scanner = region.getCoprocessorHost().preScannerOpen(scan);
        if (scanner == null)
            scanner = region.getScanner(scan);

        List<Cell> row = Lists.newArrayList();
        boolean shouldContinue;
        do {
            shouldContinue = scanner.next(row);
            if (row.size() <= 0) continue; //nothing returned

            byte[] rowBytes = row.get(0).getRowArray();
            if (Bytes.compareTo(rowBytes, limit) < 0) {
                Mutation mutation = Mutations.getDeleteOp(transactionId, rowBytes);
                mutation.setAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,
                                      SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
                if (mutation instanceof Put) {
                    try {
                        OperationStatus[] statuses = region.batchMutate(new Mutation[]{mutation});
                        OperationStatus status = statuses[0];
                        switch (status.getOperationStatusCode()) {
                            case NOT_RUN:
                                return WriteResult.notRun();
                            case SUCCESS:
                                return WriteResult.success();
                            default:
                                return WriteResult.failed(status.getExceptionMsg());
                        }
                    } catch (IOException ioe) {
                        return WriteResult.failed(ioe.getMessage());
                    }
                } else {
                    try {
                        region.delete((Delete) mutation);
                        return WriteResult.success();
                    } catch (IOException ioe) {
                        return WriteResult.failed(ioe.getMessage());
                    }
                }
            } else {
                //we've gone past our limit value without finding anything, so return notRun() to indicate
                //no action
                return WriteResult.notRun();
            }
        } while (shouldContinue);

        //no rows were found, so nothing to delete
        return WriteResult.notRun();
    }

    private boolean shouldAllowWrite() {
        if (metrics.getCompactionQueueSize() > compactionQueueSizeBlock || metrics.getFlushQueueSize() >
                flushQueueSizeBlock) {
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

    private WriteContext getWriteContext(String txnId, RegionCoprocessorEnvironment rce, RollForwardQueue queue,
                                         int writeSize) throws IOException, InterruptedException {
        Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = getContextPair(conglomId);
        return ctxFactoryPair.getFirst().create(txnId, rce, queue, writeSize);
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

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void bulkWrite(RpcController rpcController, BulkWriteRequest bulkWriteRequest,RpcCallback<BulkWriteResponse> callback) {
		BulkWriteResponse.Builder writeResponse = BulkWriteResponse.newBuilder();
		try {
			writeResponse.setBytes(ByteString.copyFrom(bulkWrite(bulkWriteRequest.getBytes().toByteArray())));
		} catch (IOException e) {
			      ResponseConverter.setControllerException(rpcController, e);
		}
		callback.run(writeResponse.build());
	}

    @Override
    public void deleteFirstAfter(RpcController controller, SpliceMessage.DeleteFirstAfterRequest request,
                                 RpcCallback<SpliceMessage.WriteResult> done) {
        // TODO: implement
    }
}
