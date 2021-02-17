/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.client;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.callbuffer.PipingCallBuffer;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Lists;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class BulkWriteAction implements Callable<WriteStats>{
    private static final Logger LOG=Logger.getLogger(BulkWriteAction.class);
    private static final Logger RETRY_LOG=Logger.getLogger(BulkWriteAction.class.getName()+".retries");
    private static final AtomicLong idGen=new AtomicLong(0L);
    private MergingWriteStats stats;
    private BulkWrites bulkWrites;
    private final List<Throwable> errors= Lists.newArrayListWithExpectedSize(0);
    private final WriteConfiguration writeConfiguration;
    private final ActionStatusReporter statusReporter;
    private final byte[] tableName;
    private final long id=idGen.incrementAndGet();
    private final BulkWriterFactory writerFactory;
    private final PipelineExceptionFactory pipelineExceptionFactory;
    private final Clock clock;
    private final MetricFactory metricFactory;
    private final Timer writeTimer;
    private final Counter retryCounter;
    private final Counter globalErrorCounter;
    private final Counter rejectedCounter;
    private final Counter partialFailureCounter;
    private final PartitionFactory partitionFactory;
    private PipingCallBuffer retryPipingCallBuffer=null; // retryCallBuffer


    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public BulkWriteAction(byte[] tableName,
                           BulkWrites writes,
                           WriteConfiguration writeConfiguration,
                           ActionStatusReporter statusReporter,
                           BulkWriterFactory writerFactory,
                           PipelineExceptionFactory pipelineExceptionFactory,
                           PartitionFactory partitionFactory,
                           Clock clock){
        assert writes != null : "writes passed into BWA are null";
        this.tableName = tableName;
        this.bulkWrites = writes;
        this.writeConfiguration = writeConfiguration;
        this.statusReporter = statusReporter;
        this.writerFactory = writerFactory;
        MetricFactory possibleMetricFactory = writeConfiguration.getMetricFactory();
        this.metricFactory = possibleMetricFactory == null ? Metrics.noOpMetricFactory() : possibleMetricFactory;
        this.clock = clock;

        stats = new MergingWriteStats(metricFactory);

        this.rejectedCounter = metricFactory.newCounter();
        this.globalErrorCounter = metricFactory.newCounter();
        this.partialFailureCounter = metricFactory.newCounter();
        this.retryCounter = metricFactory.newCounter();
        this.writeTimer = metricFactory.newTimer();
        this.pipelineExceptionFactory = pipelineExceptionFactory;
        this.partitionFactory = partitionFactory;
    }

    @Override
    public WriteStats call() throws Exception{
        statusReporter.numExecutingFlushes.incrementAndGet();
        reportSize();
        long start = System.currentTimeMillis();
        try {
            Timer totalTimer = metricFactory.newTimer();
            totalTimer.startTiming();
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "Calling BulkWriteAction: id=%d, initialBulkWritesSize=%d, initialKVPairSize=%d",
                        id, bulkWrites.numEntries(), bulkWrites.numEntries());
            }
            execute(bulkWrites);
            totalTimer.stopTiming();
            if (metricFactory.isActive())
                return new SimpleWriteStats(stats, null);
            else
                return WriteStats.NOOP_WRITE_STATS;
        } catch(Exception e) {
            return new SimpleWriteStats(stats, e);
        } finally {
            long timeTakenMs = System.currentTimeMillis() - start;
            long numRecords = bulkWrites.numEntries();
            writeConfiguration.writeComplete(timeTakenMs, numRecords);
            statusReporter.complete(timeTakenMs);
            bulkWrites = null;
        }
    }

    private void reportSize(){
        long bufferEntries=bulkWrites.numEntries();
        int numRegions=bulkWrites.numRegions();
        statusReporter.totalFlushEntries.addAndGet(bufferEntries);
        statusReporter.totalFlushRegions.addAndGet(numRegions);

        reportSize(numRegions, statusReporter.maxFlushRegions, Long.MAX_VALUE);
        reportSize(numRegions, statusReporter.minFlushRegions, Long.MIN_VALUE);

        reportSize(bufferEntries, statusReporter.maxFlushEntries, Long.MAX_VALUE);
        reportSize(bufferEntries, statusReporter.minFlushEntries, Long.MIN_VALUE);

        long bufferSizeBytes=bulkWrites.getBufferHeapSize();
        statusReporter.totalFlushSizeBytes.addAndGet(bufferSizeBytes);

        reportSize(bufferSizeBytes, statusReporter.maxFlushSizeBytes, Long.MAX_VALUE);
        reportSize(bufferSizeBytes, statusReporter.minFlushSizeBytes, Long.MIN_VALUE);
    }

    private void reportSize(long bufferEntries, AtomicLong atomicValue,
                               long minOrMax) {
        boolean success;
        do {
            long currentValue = atomicValue.get();
            boolean alreadySetOrMore = minOrMax == Long.MIN_VALUE
                    ? (currentValue <= bufferEntries) : (currentValue >= bufferEntries);
            success = alreadySetOrMore || atomicValue.compareAndSet(currentValue, bufferEntries);
        } while (!success);
    }

    private void execute(BulkWrites bulkWrites) throws Exception {
        assert bulkWrites!=null:"bulk writes passed in are null";
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Executing BulkWriteAction: id=%d, bulkWrites=%s",id,bulkWrites);
        boolean first = true;
        LinkedList<BulkWrites> writesToPerform=Lists.newLinkedList();
        writesToPerform.add(bulkWrites);
        WriteAttemptContext ctx = new WriteAttemptContext();
        do{
            if (!first) {
                retryCounter.increment();
            }
            first = false;
            BulkWrites nextWrite=writesToPerform.removeFirst();
            assert nextWrite!=null:"next write is null";
            ctx.reset();
            ctx.attemptCount++;
            if(ctx.attemptCount>100 && ctx.attemptCount%50==0){
                int numRows= nextWrite.numEntries();
                SpliceLogUtils.warn(LOG,"BulkWriteAction[%d rows] is taking a long time with %d attempts: id=%d",numRows,ctx.attemptCount,id);
                LOG.warn("Attempting bulk write "+nextWrite);
            }
            executeSingle(nextWrite,ctx);

            /*
             * We need to do an exponential backoff to ensure that our cache has a chance to invalidate, or
             * simply because we were told to wait a bit by the write pipeline (i.e. we were rejected).
             */
            if(ctx.shouldSleep()){
                clock.sleep(PipelineUtils.getPauseTime(ctx.attemptCount,10),TimeUnit.MILLISECONDS);
            }if(ctx.directRetry)
                writesToPerform.add(nextWrite);
            else if(ctx.nextWriteSet!=null && !ctx.nextWriteSet.isEmpty()){
                ctx.failed();
                //rebuild a new buffer to retry from any records that need retrying
                addToRetryCallBuffer(ctx.nextWriteSet,bulkWrites.getTxn(),bulkWrites.getToken(),ctx.refreshCache);
            }

            if(retryPipingCallBuffer!=null){
                writesToPerform.addAll(retryPipingCallBuffer.getBulkWrites());
                retryPipingCallBuffer.close();
                retryPipingCallBuffer=null;
            }
        } while(!writesToPerform.isEmpty());
    }

    private void executeSingle(BulkWrites nextWrite, WriteAttemptContext ctx) throws Exception {
        retryPipingCallBuffer = null;

        if(!shouldWrite(nextWrite)) return;
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Getting next BulkWrites in loop: id=%d, nextBulkWrites=%s",id,nextWrite);

        try {
            BulkWriter writer = writerFactory.newWriter(tableName);
            writeTimer.startTiming();
            BulkWritesResult bulkWritesResult = writer.write(nextWrite, ctx.refreshCache, writeConfiguration.loadReplaceMode());
            writeTimer.stopTiming();
            processWriteResult(nextWrite, bulkWritesResult.getBulkWriteResults(), ctx);
        } catch (Throwable e) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Caught throwable: id=%d", id);
            globalErrorCounter.increment();
            if (e instanceof FailedRowsException)
                throw new ExecutionException(e);

            //noinspection ThrowableResultOfMethodCallIgnored
            if (pipelineExceptionFactory.processPipelineException(e) instanceof PipelineTooBusy) {
                ctx.rejected();
                /*
                 * the pipeline has indicated that it is too busy to accept any writes just now. So we need
                 * to retry the entire BulkWrites that we were send. But since it's too busy (and not some other error),
                 * there's no need to refresh caches on our account. However, we *do* need to do a backoff to ensure
                 * that we let the Pipeline cool down a bit.
                 */
                if (RETRY_LOG.isDebugEnabled())
                    SpliceLogUtils.debug(RETRY_LOG, "Retrying write after receiving RegionTooBusyException: id=%d", id);

                stats.regionTooBusy.increment();
                ctx.sleep = true;
                ctx.directRetry();
                return;
            }

            processGlobalError(nextWrite, ctx, e);
        }
    }

    private void processGlobalError(BulkWrites bulkWrites, WriteAttemptContext ctx, Throwable e) throws ExecutionException
    {
        WriteResponse writeResponse = writeConfiguration.globalError(e);
        String summary = String.format("id=%d, class=%s, message=%s", id, e.getClass(), e.getMessage());
        switch (writeResponse) {
            case THROW_ERROR:
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "Throwing error after receiving global error: " + summary);
                stats.catchThrownRows.add(bulkWrites.numEntries());
                throw new ExecutionException(e);
            case RETRY:
                errors.add(e);
                ctx.rejected();
                if (RETRY_LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(RETRY_LOG, "Retrying write after receiving global error: " + summary);
                    if (RETRY_LOG.isTraceEnabled()) {
                        RETRY_LOG.trace("Global error exception received: ", e);
                    }
                } else if (ctx.attemptCount > 100 && ctx.attemptCount % 50 == 0) {
                    SpliceLogUtils.debug(LOG, "Retrying write after receiving global error: " + summary);
                }
                ctx.sleep = true;
                ctx.refreshCache = true;
                for (BulkWrite bw : bulkWrites.getBulkWrites()) {
                    ctx.addBulkWritesToRetry(bw.getMutations());
                    stats.catchRetriedRows.add(bw.getSize());
                }
                break;
            default:
                LOG.warn(String.format("Ignoring error after receiving unknown global error %s response: id=%d ", writeResponse, id), e);
                throw new IllegalStateException("Programmer error: Unknown global response: " + writeResponse);
        }
    }

    void processWriteResult(BulkWrites bulkWrites,Collection<BulkWriteResult> results,
                            WriteAttemptContext ctx) throws Throwable
    {
        Iterator<BulkWrite> bws = bulkWrites.getBulkWrites().iterator();
        for (BulkWriteResult bulkWriteResult : results) {
            WriteResponse globalResponse = writeConfiguration.processGlobalResult(bulkWriteResult);
            BulkWrite currentBulkWrite = bws.next();
            switch (globalResponse) {
                case SUCCESS:
                    stats.writtenCounter.add(currentBulkWrite.getSize());
                    break; //success can be ignored
                case THROW_ERROR:
                    stats.thrownErrorsRows.add(currentBulkWrite.getSize());
                    throwExceptionFromFailedRows(bulkWriteResult);
                case RETRY:
                    stats.retriedRows.add(currentBulkWrite.getSize());
                    /*
                     * The entire BulkWrite needs to be retried--either because it was rejected outright,
                     * or because the region moved/split/something else.
                     */
                    ctx.rejected();

                    logRetry(ctx, bulkWriteResult, currentBulkWrite);

                    ctx.addBulkWritesToRetry(currentBulkWrite.getMutations());
                    ctx.refreshCache = ctx.refreshCache || bulkWriteResult.getGlobalResult().refreshCache();
                    ctx.sleep = true; //always sleep due to rejection, even if we don't need to refresh the cache
                    break;
                case PARTIAL:
                    processPartialWrite(currentBulkWrite, bulkWriteResult, ctx);
                    break;
                case IGNORE:
                    stats.ignoredRows.add(currentBulkWrite.getSize());
                    SpliceLogUtils.warn(LOG, "Global response indicates ignoring, so we ignore");
                    break;
                default:
                    SpliceLogUtils.error(LOG, "Global response went down default path, assert");
                    throw new IllegalStateException("Programmer error: Unknown global response: " + globalResponse);
            }
        }
    }

    void processPartialWrite(BulkWrite currentBulkWrite, BulkWriteResult bulkWriteResult,
                             WriteAttemptContext ctx) throws Exception {
        stats.partialRows.add(currentBulkWrite.getSize());
        partialFailureCounter.increment();
        WriteResponse writeResponse = writeConfiguration.partialFailure(bulkWriteResult, currentBulkWrite);
        switch (writeResponse) {
            case THROW_ERROR:
                stats.partialThrownErrorRows.add(currentBulkWrite.getSize());
                throwExceptionFromFailedRows(bulkWriteResult);
            case RETRY:
                processPartialWriteRetry(currentBulkWrite, bulkWriteResult, ctx, writeResponse);
                break;
            case IGNORE:
                stats.partialIgnoredRows.add(currentBulkWrite.getSize());
                SpliceLogUtils.warn(RETRY_LOG,
                        "Ignoring write after receiving unknown partial %s response: id=%d, bulkWriteResult=%s",
                        writeResponse, id, bulkWriteResult);
                break;
            default:
                throw new IllegalStateException("Programmer error: Unknown partial response: " + writeResponse);
        }
    }

    private void processPartialWriteRetry(BulkWrite currentBulkWrite, BulkWriteResult bulkWriteResult,
                                          WriteAttemptContext ctx, WriteResponse writeResponse) throws Exception {
        logPartialRetry(ctx, bulkWriteResult, currentBulkWrite, writeResponse);

        Collection<KVPair> toRetry = PipelineUtils.getMutationsToRetry(currentBulkWrite, bulkWriteResult, id);
        if (!toRetry.isEmpty()) {
            ctx.addBulkWritesToRetry(toRetry);
            // only redo cache if you have a failure not a lock contention issue
            boolean isFailure = bulkWriteResult.getFailedRows() != null && !bulkWriteResult.getFailedRows().isEmpty();
            ctx.refreshCache = ctx.refreshCache || isFailure;
            ctx.sleep = true; //always sleep
        }
        stats.writtenCounter.add(currentBulkWrite.getSize() - bulkWriteResult.getNotRunRows().size() -
                bulkWriteResult.getFailedRows().size());
        stats.partialRetriedRows.add(toRetry.size());
    }

    /**
     * Return an error message describing the types and number of failures in the BatchWrite.
     *
     * @param failedRows the rows which failed, and their respective error messages
     * @return error message describing the failed rows
     */
    private String getFailedRowsMessage(IntObjectHashMap<WriteResult> failedRows){

        if(failedRows!=null && !failedRows.isEmpty()){

            // Aggregate the error counts by code.
            HashMap<Code, Integer> errorCodeToCountMap=new HashMap<>();
            for(IntObjectCursor<WriteResult> failedRowCursor : failedRows){
                WriteResult wr=failedRowCursor.value;
                Code errorCode=(wr==null?null:wr.getCode());
                Integer errorCount=errorCodeToCountMap.get(errorCode);
                errorCodeToCountMap.put(errorCode,(errorCode==null || errorCount==null?1:errorCount+1));
            }

            // Make a string out of the error map.
            StringBuilder buf=new StringBuilder();
            buf.append("{ ");
            boolean first=true;
            for(Map.Entry<Code, Integer> entry : errorCodeToCountMap.entrySet()){
                if(!first){
                    buf.append(", ");
                }else{
                    first=false;
                }
                buf.append(String.format("%s=%s",entry.getKey(),entry.getValue()));
            }
            buf.append(" }");
            return buf.toString();
        }else{
            return "NONE";
        }
    }

    private boolean shouldWrite(BulkWrites nextWrite){
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"[%d] next bulkWrites %s",id,nextWrite);
        if(nextWrite==null ||nextWrite.numEntries()==0){
            if(LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"No actual writes in BulkWrites %s",nextWrite);
            return false;
        }
        return true;
    }

    /** this is used to mark exceptions that are from failed rows (throwExceptionFromFailedRows)
     * (in contast to pipeline exceptions)
     */
    public class FailedRowsException extends Exception {
        public IntObjectHashMap<WriteResult> res;
        FailedRowsException(IntObjectHashMap<WriteResult> res) {
            this.res = res;
        }
    }

    private void throwExceptionFromFailedRows(BulkWriteResult response) throws FailedRowsException {
        IntObjectHashMap<WriteResult> failedRows = response.getFailedRows();
        Exception first = null;
        FailedRowsException err = new FailedRowsException(failedRows);
        for (IntObjectCursor<WriteResult> cursor : failedRows) {
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable e = pipelineExceptionFactory.processErrorResult(cursor.value);
            if (e instanceof WriteConflict) { //TODO -sf- find a way to add in StandardExceptions here
                throw err;
            } else if (first == null)
                first = err;
        }
        throw err;
    }

    private void addToRetryCallBuffer(Collection<KVPair> retryBuffer,TxnView txn,byte[] token,boolean refreshCache) throws Exception{
        if (retryBuffer == null) return; //actually nothing to do--probably doesn't happen, but just to be safe
        if (RETRY_LOG.isDebugEnabled())
            SpliceLogUtils.debug(RETRY_LOG, "[%d] addToRetryCallBuffer %d rows, refreshCache=%s", id, retryBuffer.size(), refreshCache);
        if (refreshCache) {
            writerFactory.invalidateCache(tableName);
        }
        if (retryPipingCallBuffer == null)
            retryPipingCallBuffer = new PipingCallBuffer(partitionFactory.getTable(Bytes.toString(tableName)), txn,
                    token, null, PipelineUtils.noOpFlushHook, writeConfiguration, null, false);
        if (refreshCache)
            retryPipingCallBuffer.rebuild();
        retryPipingCallBuffer.addAll(retryBuffer);
    }

    private class WriteAttemptContext{
        boolean refreshCache = false;
        boolean sleep = false;
        /*
         * Either directRetrySet !=null or nextWriteSet !=null. Otherwise, it's an error (since nextWriteSet is
         * necessarily a subset of the rows contained in directWriteSet).
         *
         */
        Collection<KVPair> nextWriteSet;
        boolean directRetry;
        int attemptCount = 0;

        int rejectedCount;
        int failedCount;


        boolean shouldSleep(){
            return sleep || refreshCache;
        }

        void reset(){
            refreshCache = false;
            sleep = false;
            nextWriteSet = null;
            directRetry = false;
        }

        void addBulkWritesToRetry(Collection<KVPair> writes){
            assert !directRetry: "Cannot add a partial failure and a global retry at the same time";
            if(nextWriteSet==null)
                nextWriteSet = writes;
            else nextWriteSet.addAll(writes);
        }

        void directRetry(){
            assert nextWriteSet==null: "Cannot add a global retry when we already have a partial failure sure";
            directRetry = true;
        }

        void rejected(){
            rejectedCount++;
            rejectedCounter.increment();
            statusReporter.rejectedCount.incrementAndGet();
        }

        void failed(){
            failedCount++;
        }
    }

    private void logRetry(WriteAttemptContext ctx, BulkWriteResult bulkWriteResult, BulkWrite currentBulkWrite) {
        if (RETRY_LOG.isDebugEnabled())
            SpliceLogUtils.debug(RETRY_LOG,
                    "Retrying write after receiving global RETRY response: id=%d, bulkWriteResult=%s, bulkWrite=%s",
                    id, bulkWriteResult, currentBulkWrite);
        else if (ctx.attemptCount > 100 && ctx.attemptCount % 50 == 0) {
            SpliceLogUtils.warn(LOG,
                    "Retrying write after receiving global RETRY response: id=%d, bulkWriteResult=%s, bulkWrite=%s",
                    id, bulkWriteResult, currentBulkWrite);
        }
    }

    private void logPartialRetry(WriteAttemptContext ctx, BulkWriteResult bulkWriteResult, BulkWrite currentBulkWrite,
                                 WriteResponse writeResponse) {
        if (RETRY_LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(RETRY_LOG,
                    "Retrying write after receiving partial %s response: id=%d, bulkWriteResult=%s, failureCountsByType=%s",
                    writeResponse, id, bulkWriteResult, getFailedRowsMessage(bulkWriteResult.getFailedRows()));
        } else if (RETRY_LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(RETRY_LOG,
                    "Retrying write after receiving partial %s response: id=%d, bulkWriteResult=%s",
                    writeResponse, id, bulkWriteResult);
        } else if (ctx.attemptCount > 100 && ctx.attemptCount % 50 == 0) {
            SpliceLogUtils.warn(LOG,
                    "Retrying write after receiving partial RETRY response: id=%d, bulkWriteResult=%s, bulkWrite=%s",
                    id, bulkWriteResult, currentBulkWrite);
        }
    }
}
