package com.splicemachine.derby.hbase;

import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.impl.*;
import com.splicemachine.pipeline.api.Service;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactory;
import com.splicemachine.pipeline.impl.*;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactoryManager;
import com.splicemachine.pipeline.writehandler.IndexCallBufferFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.TrafficControl;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.management.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 *         Created on: 3/11/13
 */
public class SpliceBaseIndexEndpoint {
    private static final Logger LOG = Logger.getLogger(SpliceBaseIndexEndpoint.class);
    public static final int ipcReserved = 10;
    private static final SpliceWriteControl writeControl;
    public static final TrafficControl independentTrafficControl;

    static {
        int ipcThreads = SpliceConstants.ipcThreads - SpliceConstants.taskWorkers - ipcReserved;
        int maxIndependentWrites = SpliceConstants.maxIndependentWrites;
        int maxDependentWrites = SpliceConstants.maxDependentWrites;
        writeControl = new SpliceWriteControl(ipcThreads / 2, ipcThreads / 2, maxDependentWrites, maxIndependentWrites);
        independentTrafficControl = writeControl.independentTrafficControl();
    }

    private static MetricName receptionName = new MetricName("com.splicemachine", "receiverStats", "time");
    private static MetricName rejectedMeterName = new MetricName("com.splicemachine", "receiverStats", "rejected");

    private long conglomId;
    private TransactionalRegion region;

    private Timer timer = SpliceDriver.driver().getRegistry().newTimer(receptionName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private Meter rejectedMeter = SpliceDriver.driver().getRegistry().newMeter(rejectedMeterName, "rejectedRows", TimeUnit.SECONDS);

    private static final RegionWritePipeline.PipelineMeters pipelineMeter = new RegionWritePipeline.PipelineMeters();

    private RegionWritePipeline regionWritePipeline;
    private static final AtomicLong rejectedCount = new AtomicLong(0l);

    private RegionCoprocessorEnvironment rce;

    public void start(CoprocessorEnvironment env) {
        rce = ((RegionCoprocessorEnvironment) env);
        String tableName = rce.getRegion().getTableDesc().getNameAsString();
        final WriteContextFactory<TransactionalRegion> factory;
        try {
            conglomId = Long.parseLong(tableName);
        } catch (NumberFormatException nfe) {
            SpliceLogUtils.debug(LOG, "Unable to parse conglomerate id for table %s, " +
                    "index management for batch operations will be disabled", tableName);
            conglomId = -1;
        }
        factory = WriteContextFactoryManager.getWriteContext(conglomId);

        Service service = new Service() {
            @Override
            public boolean shutdown() {
                return true;
            }

            @Override
            public boolean start() {
                factory.prepare();
                if (conglomId >= 0) {
                    region = TransactionalRegions.get(rce.getRegion());
                } else {
                    region = TransactionalRegions.nonTransactionalRegion(rce.getRegion());
                }
                regionWritePipeline = new RegionWritePipeline(rce, rce.getRegion(), factory, region, pipelineMeter);
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
//        BulkWritesResult result = new BulkWritesResult();
        Collection<BulkWrite> bws = bulkWrites.getBulkWrites();
        int size =  bulkWrites.getBulkWrites().size();
        List<BulkWriteResult> result = new ArrayList<>(size);
        // start
//				List<Pair<BulkWriteResult,RegionWritePipeline>> startPoints = Lists.newArrayListWithExpectedSize(size);
        IndexCallBufferFactory indexWriteBufferFactory = new IndexCallBufferFactory();
        boolean dependent = regionWritePipeline.isDependent();
        SpliceWriteControl.Status status;
        int kvPairSize = bulkWrites.numEntries();
        status = (dependent) ? writeControl.performDependentWrite(kvPairSize) : writeControl.performIndependentWrite(kvPairSize);
        if (status.equals(SpliceWriteControl.Status.REJECTED)) {
            //we cannot write to this
            rejectAll(result, size);
            rejectedCount.addAndGet(size);
            return new BulkWritesResult(result);
        }
        try {

            Map<BulkWrite, Pair<BulkWriteResult, RegionWritePipeline>> writePairMap = getBulkWritePairMap(bws, size);

            //
            // Submit the bulk writes for which we found a RegionWritePipeline.
            //
            for (Map.Entry<BulkWrite, Pair<BulkWriteResult, RegionWritePipeline>> entry : writePairMap.entrySet()) {
                Pair<BulkWriteResult, RegionWritePipeline> pair = entry.getValue();
                RegionWritePipeline writePipeline = pair.getSecond();
                if (writePipeline != null) {
                    BulkWrite bulkWrite = entry.getKey();
                    BulkWriteResult submitResult = writePipeline.submitBulkWrite(bulkWrites.getTxn(), bulkWrite,indexWriteBufferFactory, writePipeline.getRegionCoprocessorEnvironment());
                    pair.setFirst(submitResult);
                }
            }

            //
            // Same iteration, now calling finishWrite() for each BulkWrite
            //
            for (Map.Entry<BulkWrite, Pair<BulkWriteResult, RegionWritePipeline>> entry : writePairMap.entrySet()) {
                Pair<BulkWriteResult, RegionWritePipeline> pair = entry.getValue();
                RegionWritePipeline writePipeline = pair.getSecond();
                if (writePipeline != null) {
                    BulkWrite bulkWrite = entry.getKey();
                    BulkWriteResult writeResult = pair.getFirst();
                    BulkWriteResult finishResult = writePipeline.finishWrite(writeResult, bulkWrite);
                    pair.setFirst(finishResult);
                }
            }

            //
            // Collect the overall results.
            //
            for (Map.Entry<BulkWrite,Pair<BulkWriteResult,RegionWritePipeline>> entry: writePairMap.entrySet()) {
                Pair<BulkWriteResult, RegionWritePipeline> pair = entry.getValue();
                result.add(pair.getFirst());
            }
            return new BulkWritesResult(result);
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
		private void rejectAll(Collection<BulkWriteResult> result, int numResults) {
				this.rejectedMeter.mark();
				for (int i = 0; i < numResults; i++) {
						result.add(new BulkWriteResult(WriteResult.pipelineTooBusy(rce.getRegion().getRegionNameAsString())));
				}
		}

    /**
     * Just builds this map:  BulkWrite -> (BulkWriteResult, RegionWritePipeline) where the RegionWritePipeline may
     * be null for some BulkWrites.
     */
    private Map<BulkWrite, Pair<BulkWriteResult, RegionWritePipeline>> getBulkWritePairMap(Collection<BulkWrite> buffer, int size) {
        Map<BulkWrite, Pair<BulkWriteResult, RegionWritePipeline>> writePairMap = Maps.newIdentityHashMap();
        for(BulkWrite bw:buffer){
            RegionWritePipeline writePipeline = SpliceDriver.driver().getWritePipeline(bw.getEncodedStringName());
            BulkWriteResult writeResult;
            if (writePipeline != null) {
                //we might be able to write this one
                writeResult = new BulkWriteResult();
            } else {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "endpoint not found for region %s on region %s", bw.getEncodedStringName(), rce.getRegion().getRegionNameAsString());
                writeResult = new BulkWriteResult(WriteResult.notServingRegion());
            }
            writePairMap.put(bw, Pair.newPair(writeResult, writePipeline));
        }
        return writePairMap;
    }

    public byte[] bulkWrites(byte[] bulkWriteBytes) throws IOException {
        assert bulkWriteBytes != null;
        BulkWrites bulkWrites = PipelineEncoding.decode(bulkWriteBytes);
//        BulkWrites bulkWrites = PipelineUtils.fromCompressedBytes(bulkWriteBytes,BulkWrites.class);
        return PipelineUtils.toCompressedBytes(bulkWrite(bulkWrites));
    }

    public static void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName coordinatorName = new ObjectName("com.splicemachine.derby.hbase:type=ActiveWriteHandlers");
        mbs.registerMBean(ActiveWriteHandlers.get(), coordinatorName);
    }

    public RegionWritePipeline getWritePipeline() {
        return regionWritePipeline;
    }

    public static class ActiveWriteHandlers implements ActiveWriteHandlersIface {
        private static final ActiveWriteHandlers INSTANCE = new ActiveWriteHandlers();

        private ActiveWriteHandlers() {
        }

        public static ActiveWriteHandlers get() {
            return INSTANCE;
        }

        @Override
        public int getIpcReservedPool() {
            return ipcReserved;
        }

        @Override
        public int getMaxDependentWriteThreads() {
            return writeControl.maxDependentWriteThreads;
        }

        @Override
        public int getMaxIndependentWriteThreads() {
            return writeControl.maxIndependentWriteThreads;
        }

        @Override
        public int getMaxDependentWriteCount() {
            return writeControl.maxDependentWriteCount;
        }

        @Override
        public int getMaxIndependentWriteCount() {
            return writeControl.maxIndependentWriteCount;
        }

        @Override
        public double getOverallAvgThroughput() {
            return pipelineMeter.throughput();
        }

        @Override
        public double get1MThroughput() {
            return pipelineMeter.oneMThroughput();
        }

        @Override
        public double get5MThroughput() {
            return pipelineMeter.fiveMThroughput();
        }

        @Override
        public double get15MThroughput() {
            return pipelineMeter.fifteenMThroughput();
        }

        @Override
        public long getTotalRejected() {
            return rejectedCount.get();
        }

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