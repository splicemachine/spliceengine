package com.splicemachine.derby.hbase;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactory;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.writehandler.IndexCallBufferFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The entry/starting point for BulkWrites remotely (on the region server for the table they will mutate).
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class RegionWritePipeline {

    private static final BulkWriteResult NOT_SERVING_REGION = new BulkWriteResult(null, WriteResult.notServingRegion());
    private static final BulkWriteResult INTERRUPTED = new BulkWriteResult(null, WriteResult.interrupted());
    private static final BulkWriteResult INDEX_NOT_SETUP = new BulkWriteResult(null, WriteResult.indexNotSetup());

    private final WriteContextFactory<TransactionalRegion> ctxFactory;
    private final HRegion region;
    private final TransactionalRegion txnRegion;
    private final PipelineMeters pipelineMeters;
    private final RegionCoprocessorEnvironment rce;

    public RegionWritePipeline(RegionCoprocessorEnvironment rce,
                               HRegion region,
                               WriteContextFactory<TransactionalRegion> ctxFactory,
                               TransactionalRegion txnRegion,
                               PipelineMeters pipelineMeters) {
        this.rce = rce;
        this.region = region;
        this.ctxFactory = ctxFactory;
        this.txnRegion = txnRegion;
        this.pipelineMeters = pipelineMeters;
    }

    public RegionCoprocessorEnvironment getRegionCoprocessorEnvironment() {
        return rce;
    }

    public void close() {
        ctxFactory.close();
        txnRegion.discard();
    }


    public BulkWriteResult submitBulkWrite(TxnView txn,
                                           BulkWrite toWrite,
                                                   IndexCallBufferFactory writeBufferFactory,
                                                   RegionCoprocessorEnvironment rce) throws IOException{
        assert txn!=null: "No transaction specified!";

        /*
         * We don't need to actually start a region operation here,
         * because we know that the actual writes won't happen
         * until we call finishWrite() below. We do a quick check
         * to make sure that the region isn't closing, but other
         * than that, we don't need to do anything regional here
         */
        if (region.isClosed() || region.isClosing()) {
            return NOT_SERVING_REGION;
        }

        WriteContext context;
        try {
            context = ctxFactory.create(writeBufferFactory, txn, txnRegion, rce);
        } catch (InterruptedException e) {
            return INTERRUPTED;
        } catch (IndexNotSetUpException e) {
            return INDEX_NOT_SETUP;
        }
        Collection<KVPair> kvPairs = toWrite.getMutations();
        for(KVPair kvPair:kvPairs){
            context.sendUpstream(kvPair);
        }
        return new BulkWriteResult(context, WriteResult.success());
    }

    public BulkWriteResult finishWrite(BulkWriteResult intermediateResult, BulkWrite write) throws IOException {
        WriteContext ctx = intermediateResult.getWriteContext();
        if (ctx == null)
            return intermediateResult; //already failed

        try {
            region.startRegionOperation();
        } catch (NotServingRegionException nsre) {
            return new BulkWriteResult(WriteResult.notServingRegion());
        } catch (RegionTooBusyException rtbe) {
            return new BulkWriteResult(WriteResult.regionTooBusy());
        } catch (InterruptedIOException iioe) {
            return new BulkWriteResult(WriteResult.interrupted());
        }
        try {
            ctx.flush();
            Map<KVPair, WriteResult> rowResultMap = ctx.close();
            BulkWriteResult response = new BulkWriteResult();
            int failed = 0;
            int size = write.getSize();
            Collection<KVPair> kvPairs = write.getMutations();
            int i=0;
            for(KVPair kvPair:kvPairs){
                WriteResult result = rowResultMap.get(kvPair);
                if(!result.isSuccess())
                    failed++;
                response.addResult(i,result);
                i++;
            }
            if (failed > 0) {
                response.setGlobalStatus(WriteResult.partial());
            } else
                response.setGlobalStatus(WriteResult.success());

            pipelineMeters.mark(size - failed, failed);
            return response;
        } catch (NotServingRegionException nsre) {
            return new BulkWriteResult(WriteResult.notServingRegion());
        } catch (RegionTooBusyException rtbe) {
            return new BulkWriteResult(WriteResult.regionTooBusy());
        } catch (InterruptedIOException iioe) {
            return new BulkWriteResult(WriteResult.interrupted());
        } finally{
            region.closeRegionOperation();
        }
    }

    public boolean isDependent(TxnView txn) throws IOException, InterruptedException {
        return ctxFactory.hasDependentWrite(txn);
    }

    public static class PipelineMeters {
        private static MetricName throughputMeterName = new MetricName("com.splicemachine", "receiverStats", "success");
        private static MetricName failedMeterName = new MetricName("com.splicemachine", "receiverStats", "failed");
        private Meter throughputMeter;
        private Meter failedMeter;

        public PipelineMeters() {
            this.throughputMeter = SpliceDriver.driver().getRegistry().newMeter(throughputMeterName, "successfulRows", TimeUnit.SECONDS);
            this.failedMeter = SpliceDriver.driver().getRegistry().newMeter(failedMeterName, "rejectedRows", TimeUnit.SECONDS);
        }

        public void mark(int numSuccess, int numFailed) {
            throughputMeter.mark(numSuccess);
            failedMeter.mark(numFailed);
        }

        public double throughput() {
            return throughputMeter.meanRate();
        }

        public double fifteenMThroughput() {
            return throughputMeter.fifteenMinuteRate();
        }

        public double fiveMThroughput() {
            return throughputMeter.fiveMinuteRate();
        }

        public double oneMThroughput() {
            return throughputMeter.oneMinuteRate();
        }
    }
}
