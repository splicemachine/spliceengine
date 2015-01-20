package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.api.BulkWritesInvoker;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.callbuffer.PipingCallBuffer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.utils.PipelineConstants;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.utils.Sleeper;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWriteAction implements Callable<WriteStats> {
		private static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
		private static final Logger LOG = Logger.getLogger(BulkWriteAction.class);
		private static final AtomicLong idGen = new AtomicLong(0l);
		private BulkWrites bulkWrites;
		private final List<Throwable> errors = Lists.newArrayListWithExpectedSize(0);
		private final WriteConfiguration writeConfiguration;
		private final RegionCache regionCache;
		private final ActionStatusReporter statusReporter;
		private final byte[] tableName;
		private final long id = idGen.incrementAndGet();
		private final BulkWritesInvoker.Factory invokerFactory;
		private final Sleeper sleeper;
		private final MetricFactory metricFactory;
		private final Timer writeTimer;
		private final Counter retryCounter;
		private final Counter globalErrorCounter;
		private final Counter rejectedCounter;
		private final Counter partialFailureCounter;
		private PipingCallBuffer retryPipingCallBuffer = null; // retryCallBuffer

		public BulkWriteAction(byte[] tableName,
													 BulkWrites bulkWrites,
													 RegionCache regionCache,
													 WriteConfiguration writeConfiguration,
													 HConnection connection,
													 ActionStatusReporter statusReporter) {
				this(tableName,bulkWrites,regionCache,
								writeConfiguration,statusReporter,
								derbyFactory.getBulkWritesInvoker(connection, tableName),
								Sleeper.THREAD_SLEEPER);
		}

		BulkWriteAction(byte[] tableName,
										BulkWrites writes,
										RegionCache regionCache,
										WriteConfiguration writeConfiguration,
										ActionStatusReporter statusReporter,
										BulkWritesInvoker.Factory invokerFactory){
				this(tableName,writes,regionCache,
								writeConfiguration,statusReporter,
								invokerFactory,Sleeper.THREAD_SLEEPER);
		}

		BulkWriteAction(byte[] tableName,
										BulkWrites writes,
										RegionCache regionCache,
										WriteConfiguration writeConfiguration,
										ActionStatusReporter statusReporter,
										BulkWritesInvoker.Factory invokerFactory,
										Sleeper sleeper){
				this.tableName = tableName;
				this.bulkWrites = writes;
				this.regionCache = regionCache;
				this.writeConfiguration = writeConfiguration;
				this.statusReporter = statusReporter;
				this.invokerFactory = invokerFactory;
				MetricFactory possibleMetricFactory = writeConfiguration.getMetricFactory();
				this.metricFactory = possibleMetricFactory==null?Metrics.noOpMetricFactory():possibleMetricFactory;
				this.sleeper = metricFactory.isActive()?new Sleeper.TimedSleeper(sleeper,metricFactory):sleeper;
				this.rejectedCounter = metricFactory.newCounter();
				this.globalErrorCounter = metricFactory.newCounter();
				this.partialFailureCounter = metricFactory.newCounter();
				this.retryCounter = metricFactory.newCounter();
				this.writeTimer = metricFactory.newTimer();
		}

    @Override
    public WriteStats call() throws Exception {
        statusReporter.numExecutingFlushes.incrementAndGet();
        reportSize();
        long start = System.currentTimeMillis();
        try{
            Timer totalTimer = metricFactory.newTimer();
            totalTimer.startTiming();
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "[%d] initialBulkWritesSize=%d, initialKVPairSize=%d",id,bulkWrites.numEntries(),bulkWrites.numEntries());
            execute(bulkWrites);
            totalTimer.stopTiming();
            if(metricFactory.isActive())
                return new SimpleWriteStats(bulkWrites.numEntries(),bulkWrites.numEntries(),
                        retryCounter.getTotal(),
                        globalErrorCounter.getTotal(),
                        partialFailureCounter.getTotal(),
                        rejectedCounter.getTotal(),
                        sleeper.getSleepStats(),
                        writeTimer.getTime(),
                        totalTimer.getTime());
            else
                return WriteStats.NOOP_WRITE_STATS;
        } finally{
            long timeTakenMs = System.currentTimeMillis() - start;
            long numRecords = bulkWrites.numEntries();
            writeConfiguration.writeComplete(timeTakenMs,numRecords);
            statusReporter.complete(timeTakenMs);
            bulkWrites = null;
        }
    }

		private void reportSize() {
				boolean success;
				long bufferEntries = bulkWrites.numEntries();
				int numRegions = bulkWrites.numRegions();
				statusReporter.totalFlushEntries.addAndGet(bufferEntries);
				statusReporter.totalFlushRegions.addAndGet(numRegions);

				do{
						long currentMax = statusReporter.maxFlushRegions.get();
						success = currentMax >= bufferEntries || statusReporter.maxFlushRegions.compareAndSet(currentMax, bufferEntries);
				}while(!success);

				do{
						long currentMin = statusReporter.minFlushRegions.get();
						success = currentMin <= bufferEntries || statusReporter.minFlushRegions.compareAndSet(currentMin, bufferEntries);
				}while(!success);

				do{
						long currentMax = statusReporter.maxFlushEntries.get();
						success = currentMax >= bufferEntries || statusReporter.maxFlushEntries.compareAndSet(currentMax, bufferEntries);
				}while(!success);

				do{
						long currentMin = statusReporter.minFlushEntries.get();
						success = currentMin <= bufferEntries || statusReporter.minFlushEntries.compareAndSet(currentMin, bufferEntries);
				}while(!success);

				long bufferSizeBytes = bulkWrites.getBufferHeapSize();
				statusReporter.totalFlushSizeBytes.addAndGet(bufferSizeBytes);
				do{
						long currentMax = statusReporter.maxFlushSizeBytes.get();
						success = currentMax >= bufferSizeBytes || statusReporter.maxFlushSizeBytes.compareAndSet(currentMax, bufferSizeBytes);
				}while(!success);

				do{
						long currentMin = statusReporter.minFlushSizeBytes.get();
						success = currentMin <= bufferSizeBytes || statusReporter.maxFlushSizeBytes.compareAndSet(currentMin, bufferSizeBytes);
				}while(!success);

		}

    private void execute(BulkWrites bulkWrites) throws Exception{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "[%d] execute bulkWrites %s",id, bulkWrites);
        retryCounter.increment();
        boolean thrown=false;
        int numAttempts = 0;
        int maximumRetries = writeConfiguration.getMaximumRetries();
        LinkedList<BulkWrites> writesToPerform = Lists.newLinkedList();
        writesToPerform.add(bulkWrites);
        do{
            BulkWrites nextWrite = writesToPerform.removeFirst();
            retryPipingCallBuffer = null;
            if (!shouldWrite(nextWrite)) continue;
            SpliceLogUtils.trace(LOG,"[%d] %s",id,nextWrite);
            try{
                BulkWritesInvoker invoker = invokerFactory.newInstance();
                writeTimer.startTiming();
                BulkWritesResult bulkWritesResult = invoker.invoke(nextWrite,numAttempts>0);
                writeTimer.stopTiming();
                Iterator<BulkWrite> bws = nextWrite.getBulkWrites().iterator();
                Collection<BulkWriteResult> results = bulkWritesResult.getBulkWriteResults();
                for (BulkWriteResult bulkWriteResult:results) {
                    WriteResponse globalResponse = writeConfiguration.processGlobalResult(bulkWriteResult);
                    BulkWrite currentBulkWrite = bws.next();
                    switch (globalResponse) {
                        case SUCCESS:
//														rowsSuccessfullyWritten +=currentBulkWrite.getSize();
                            continue;
                        case THROW_ERROR:
                            thrown=true;
                            throw parseIntoException(bulkWriteResult);
                        case RETRY:
														/*
														 * The entire BulkWrite needs to be retried--either because it was rejected outright,
														 * or because the region moved/split/something else.
														 */
                            rejectedCounter.increment();
                            statusReporter.rejectedCount.incrementAndGet();

                            if(LOG.isTraceEnabled())
                                SpliceLogUtils.trace(LOG, "[%d] Retry write {%s} after receiving global result %s", id, currentBulkWrite, bulkWriteResult.getGlobalResult());

                            addToRetryCallBuffer(currentBulkWrite.getMutations(),nextWrite.getTxn(),bulkWriteResult.getGlobalResult().refreshCache());
                            sleeper.sleep(PipelineUtils.getWaitTime(maximumRetries - numAttempts + 1, writeConfiguration.getPause()));
                            continue;
                        case PARTIAL:
                            partialFailureCounter.increment();

                            WriteResponse writeResponse = writeConfiguration.partialFailure(bulkWriteResult,currentBulkWrite);
                            switch (writeResponse) {
                                case THROW_ERROR:
                                    thrown=true;
                                    throw parseIntoException(bulkWriteResult);
                                case RETRY:
                                    if(LOG.isTraceEnabled()) SpliceLogUtils.trace(LOG, "[%d] Retrying write after receiving %s", id, writeResponse);

                                    Collection<KVPair> toRetry = PipelineUtils.doPartialRetry(currentBulkWrite, bulkWriteResult,errors,id);
//																		rowsSuccessfullyWritten +=currentBulkWrite.getSize()-toRetry.size();
                                    addToRetryCallBuffer(toRetry,nextWrite.getTxn(),true);
                                    sleeper.sleep(PipelineUtils.getWaitTime(maximumRetries-numAttempts+1,writeConfiguration.getPause()));
                                    break;
                                default:
                                    if(LOG.isTraceEnabled())
                                        SpliceLogUtils.trace(LOG, "[%d] Ignoring write after receiving partial error %s", id, writeResponse);
                            }
                            break;
                        case IGNORE:
                            SpliceLogUtils.trace(LOG,"Global response indicates ignoring, so we ignore");
                            break;
                        default:
                            SpliceLogUtils.error(LOG, "Global Response went down default path, assert");
                            throw new IllegalStateException("Programmer error: Unknown Global response: "+ globalResponse);
                    }
                }
            } catch (Throwable e) {
                if(LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "[%d] Caught Throwable", id);
                globalErrorCounter.increment();
                if(thrown)
                    throw new ExecutionException(e);
                if (derbyFactory.getExceptionHandler().isRegionTooBusyException(e)) {
                    if(LOG.isTraceEnabled())
                        SpliceLogUtils.trace(LOG, "[%d] Retrying write after receiving a RegionTooBusyException", id);
                    sleeper.sleep(PipelineUtils.getWaitTime(maximumRetries - numAttempts + 1, writeConfiguration.getPause()));
                    writesToPerform.add(nextWrite);
                    continue;
                }

                WriteResponse writeResponse = writeConfiguration.globalError(e);
                switch(writeResponse){
                    case THROW_ERROR:
                        if(LOG.isTraceEnabled())
                            SpliceLogUtils.trace(LOG, "[%d] Throwing error after receiving a Global error %s:%s", id, e.getClass(), e.getMessage());
                        throw new ExecutionException(e);
                    case RETRY:
                        errors.add(e);
                        if(LOG.isTraceEnabled())
                            SpliceLogUtils.trace(LOG, "[%d] Retrying write after receiving a Global error %s:%s", id, e.getClass(), e.getMessage());
                        rejectedCounter.increment();
                        statusReporter.rejectedCount.incrementAndGet();
                        TxnView nextTxn = nextWrite.getTxn();
                        boolean first = true;
                        for(BulkWrite bw:nextWrite.getBulkWrites()){
                            addToRetryCallBuffer(bw.getMutations(),nextTxn,first);
                            first=false;
                        }
                        sleeper.sleep(PipelineUtils.getWaitTime(maximumRetries - numAttempts + 1, writeConfiguration.getPause()));
                        break;
                    default:
                        if(LOG.isInfoEnabled())
                            LOG.info("Ignoring error ", e);
                }
            }
            numAttempts++;
            addToWritesToPerform(retryPipingCallBuffer,writesToPerform);
            retryPipingCallBuffer = null;
            if (numAttempts > 100 && numAttempts%50==0)
                SpliceLogUtils.warn(LOG, "[%d] BulkWriteAction Taking Long Time with [%d] attempts", id,numAttempts);
        }while(writesToPerform.size()>0);
    }

    private boolean shouldWrite(BulkWrites nextWrite) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.debug(LOG, "[%d] next bulkWrites %s", id, nextWrite);
        if (nextWrite.getBulkWrites().size() == 0) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "no actual writes in BulkWrites %s",nextWrite);
            return false;
        }
        return true;
    }

		private Exception parseIntoException(BulkWriteResult response) {
				IntObjectOpenHashMap<WriteResult> failedRows = response.getFailedRows();
				Set<Throwable> errors = Sets.newHashSet();
				for (IntObjectCursor<WriteResult> cursor:failedRows) {
						Throwable e = Exceptions.fromString(cursor.value);
						if(e instanceof StandardException|| e instanceof WriteConflict)
								e.printStackTrace();
						errors.add(e);
				}
				return new RetriesExhaustedWithDetailsException(Lists.newArrayList(errors),Collections.<Row>emptyList(),Collections.<String>emptyList());
		}
		private void addToRetryCallBuffer(Collection<KVPair> retryBuffer, TxnView txn, boolean refreshCache) throws Exception {
				if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "[%d] addToRetryCallBuffer %d rows",id,retryBuffer==null?0:retryBuffer.size());
				if (retryPipingCallBuffer == null)
						retryPipingCallBuffer = new PipingCallBuffer(tableName,txn,null,regionCache,PipelineConstants.noOpFlushHook,writeConfiguration,null);
				if (refreshCache) {
						retryPipingCallBuffer.rebuildBuffer();
						regionCache.invalidate(tableName);
				}
				retryPipingCallBuffer.addAll(retryBuffer);
		}
		private void addToWritesToPerform(PipingCallBuffer retryPipingCallBuffer,List<BulkWrites> writesToPerform) throws Exception {
				if (retryPipingCallBuffer != null) {
						writesToPerform.addAll(retryPipingCallBuffer.getBulkWrites());
				}
		}

}
