package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.stats.Counter;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.Timer;
import com.splicemachine.utils.Sleeper;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
final class BulkWriteAction implements Callable<WriteStats> {

		private static final Logger LOG = Logger.getLogger(BulkWriteAction.class);
    private static final AtomicLong idGen = new AtomicLong(0l);

    private BulkWrite bulkWrite;
    private final List<Throwable> errors = Lists.newArrayListWithExpectedSize(0);
    private final Writer.WriteConfiguration writeConfiguration;
    private final RegionCache regionCache;
		private final ActionStatusReporter statusReporter;
    private final byte[] tableName;
    private final long id = idGen.incrementAndGet();

		private final BulkWriteInvoker.Factory invokerFactory;
		private final Sleeper sleeper;

		private final MetricFactory metricFactory;
		private final Timer writeTimer;
		private final Counter retryCounter;
		private final Counter globalErrorCounter;
		private final Counter rejectedCounter;
		private final Counter partialFailureCounter;

		public BulkWriteAction(byte[] tableName,
                           BulkWrite bulkWrite,
                           RegionCache regionCache,
                           Writer.WriteConfiguration writeConfiguration,
                           HConnection connection,
                           ActionStatusReporter statusReporter) {
				this(tableName,bulkWrite,regionCache,
								writeConfiguration,statusReporter,
								new BulkWriteChannelInvoker.Factory(connection,tableName),Sleeper.THREAD_SLEEPER);
    }

		BulkWriteAction(byte[] tableName,
										BulkWrite write,
										RegionCache regionCache,
										Writer.WriteConfiguration writeConfiguration,
										ActionStatusReporter statusReporter,
										BulkWriteInvoker.Factory invokerFactory){
				this(tableName,write,regionCache,
								writeConfiguration,statusReporter,
								invokerFactory,Sleeper.THREAD_SLEEPER);
		}

		BulkWriteAction(byte[] tableName,
										BulkWrite write,
										RegionCache regionCache,
										Writer.WriteConfiguration writeConfiguration,
										ActionStatusReporter statusReporter,
										BulkWriteInvoker.Factory invokerFactory,
										Sleeper sleeper){
				this.tableName = tableName;
				this.bulkWrite = write;
				this.regionCache = regionCache;
				this.writeConfiguration = writeConfiguration;
				this.statusReporter = statusReporter;
				this.invokerFactory = invokerFactory;
				MetricFactory possibleMetricFactory = writeConfiguration.getMetricFactory();
				if(possibleMetricFactory!=null)
						this.metricFactory = possibleMetricFactory;
				else
						this.metricFactory = Metrics.noOpMetricFactory();

				if(metricFactory.isActive())
						this.sleeper = new Sleeper.TimedSleeper(sleeper,metricFactory);
				else
						this.sleeper = sleeper;

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
						execute(bulkWrite);
						totalTimer.stopTiming();
						if(metricFactory.isActive())
								return new SimpleWriteStats(bulkWrite.getBufferSize(),bulkWrite.getSize(),
												retryCounter.getTotal(),
												globalErrorCounter.getTotal(),
												partialFailureCounter.getTotal(),
												rejectedCounter.getTotal(),
												sleeper.getSleepStats(),
												writeTimer.getTime(),
												totalTimer.getTime());
						else
								return WriteStats.NOOP_WRITE_STATS;
        }finally{
            //called no matter what
						long end = System.currentTimeMillis();
						long timeTakenMs = end - start;
						int numRecords = bulkWrite.getMutations().size();
            writeConfiguration.writeComplete(timeTakenMs,numRecords);
						statusReporter.complete(timeTakenMs);
            /*
             * Because we are a callable, a Future will hold on to a reference to us for the lifetime
             * of the operation. While the Future code will attempt to clean up as much of those futures
             * as possible during normal processing, a reference to this BulkWriteAction may remain on the
             * heap for some time. We can't hold on to the underlying buffer, though, or else we will
             * end up (potentially) keeping huge chunks of the write buffers in memory for arbitrary lengths
             * of time.
             *
             * To this reason, we help out the collector by dereferencing the actual BulkWrite once we're finished
             * with it. This should allow most flushes to be collected once they have completed.
             */
            bulkWrite = null;
        }
    }

    private void reportSize() {
        boolean success;
        long bufferEntries = bulkWrite.getMutations().size();
        statusReporter.totalFlushEntries.addAndGet(bufferEntries);
        do{
            long currentMax = statusReporter.maxFlushEntries.get();
            success = currentMax >= bufferEntries || statusReporter.maxFlushEntries.compareAndSet(currentMax, bufferEntries);
        }while(!success);

        do{
            long currentMin = statusReporter.minFlushEntries.get();
            success = currentMin <= bufferEntries || statusReporter.minFlushEntries.compareAndSet(currentMin, bufferEntries);
        }while(!success);

        long bufferSizeBytes = bulkWrite.getBufferSize();
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

		private static final Predicate<BulkWrite> nonEmptyPredicate = new Predicate<BulkWrite>() {
				@Override
				public boolean apply(@Nullable BulkWrite input) {
						return input!=null && input.getSize()>0;
				}
		};

		private void execute(BulkWrite bulkWrite) throws Exception{
				retryCounter.increment();
				boolean thrown=false;
				int numAttempts = 0;
				int maximumRetries = writeConfiguration.getMaximumRetries();
				LinkedList<BulkWrite> writesToPerform = Lists.newLinkedList();
				writesToPerform.add(bulkWrite);
				do{
						BulkWrite nextWrite = writesToPerform.removeFirst();
						SpliceLogUtils.trace(LOG,"[%d] %s",id,nextWrite);
						try{
								BulkWriteInvoker invoker = invokerFactory.newInstance();
								writeTimer.startTiming();
								BulkWriteResult response = invoker.invoke(nextWrite,numAttempts>0);
								writeTimer.stopTiming();

								WriteResult globalResult = processGlobalResult(response.getGlobalResult(),maximumRetries-numAttempts);
								if(globalResult.getCode()== WriteResult.Code.REGION_TOO_BUSY){
										if(LOG.isTraceEnabled())
												SpliceLogUtils.trace(LOG,"[%d] Retrying write after receiving a RegionTooBusyException",id);
										writesToPerform.add(nextWrite); //add it back in to ensure that it tries again
										continue;
								}
								SpliceLogUtils.trace(LOG, "[%d] %s", id, response);
								IntObjectOpenHashMap<WriteResult> failedRows = response.getFailedRows();
								if(failedRows!=null && failedRows.size()>0){
										partialFailureCounter.increment();
										Writer.WriteResponse writeResponse = writeConfiguration.partialFailure(response,nextWrite);
										switch (writeResponse) {
												case THROW_ERROR:
														thrown=true;
														throw parseIntoException(response);
												case RETRY:
														if(LOG.isDebugEnabled())
																SpliceLogUtils.debug(LOG,"[%d] Retrying write after receiving partial error %s",id,response);
														//add in all the non-empty new BulkWrites to try again
														writesToPerform.addAll(Collections2.filter(doPartialRetry(nextWrite, response), nonEmptyPredicate));
														break;
												default:
														if(LOG.isDebugEnabled())
																SpliceLogUtils.debug(LOG,"[%d] Ignoring write after receiving partial error %s",id,response);
														//return
										}
								}
						} catch (Throwable e) {
								globalErrorCounter.increment();
								if(thrown)
										throw new ExecutionException(e);

								if (e instanceof RegionTooBusyException) {
										if(LOG.isTraceEnabled())
												SpliceLogUtils.trace(LOG, "[%d] Retrying write after receiving a RegionTooBusyException", id);
										sleeper.sleep(WriteUtils.getWaitTime(maximumRetries - numAttempts + 1, writeConfiguration.getPause()));
										writesToPerform.add(nextWrite);
										continue;
								}

								Writer.WriteResponse writeResponse = writeConfiguration.globalError(e);
								switch(writeResponse){
										case THROW_ERROR:
												throw new ExecutionException(e);
										case RETRY:
												errors.add(e);
												if(LOG.isDebugEnabled())
														SpliceLogUtils.debug(LOG, "[%d] Retrying write after receiving a Global error %s:%s", id, e.getClass(), e.getMessage());
												writesToPerform.addAll(Collections2.filter(retry(maximumRetries, nextWrite),nonEmptyPredicate));
												break;
										default:
												if(LOG.isInfoEnabled())
														LOG.info("Ignoring error ", e);
								}
						}
						numAttempts++;
				}while(numAttempts< maximumRetries && writesToPerform.size()>0);

				if(writesToPerform.size()>0 && errors.size()>0)
						throw new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
		}

		private WriteResult processGlobalResult(WriteResult globalResult, int attemptNumber) throws Throwable {
				if(globalResult!=null){
						switch(globalResult.getCode()){
								case FAILED:
								case WRITE_CONFLICT:
								case PRIMARY_KEY_VIOLATION:
								case UNIQUE_VIOLATION:
								case FOREIGN_KEY_VIOLATION:
								case CHECK_VIOLATION:
								case NOT_SERVING_REGION:
								case WRONG_REGION:
										throw Exceptions.fromString(globalResult);
								case REGION_TOO_BUSY:
													/*
											 		 * The Region is noted as too busy, so just retry after a brief sleep
												   */
										rejectedCounter.increment();
										statusReporter.rejectedCount.incrementAndGet();
										sleeper.sleep(WriteUtils.getWaitTime(attemptNumber+ 1, writeConfiguration.getPause()));
										return globalResult;
						}
				}
				return WriteResult.success();
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

    private List<BulkWrite> doPartialRetry(BulkWrite bulkWrite, BulkWriteResult response) throws Exception {
        IntArrayList notRunRows = response.getNotRunRows();
        IntObjectOpenHashMap<WriteResult> failedRows = response.getFailedRows();

				ObjectArrayList<KVPair> allWrites = bulkWrite.getMutations();
				Object[] allWritesBuffer = allWrites.buffer;

				ObjectArrayList<KVPair> toRetry  = ObjectArrayList.newInstanceWithCapacity(notRunRows.size()+failedRows.size());
				for(int i=0;i<notRunRows.size();i++){
					toRetry.add((KVPair)allWritesBuffer[notRunRows.get(i)]);
				}

        List<String> errorMsgs = Lists.newArrayListWithCapacity(failedRows.size());

				if(LOG.isTraceEnabled()){
						int[] errorCounts = new int[11];
						for(IntObjectCursor<WriteResult> failedCursor:failedRows){
								errorCounts[failedCursor.value.getCode().ordinal()]++;
						}
						SpliceLogUtils.trace(LOG,"[%d] %d failures with types: %s",id,failedRows.size(),Arrays.toString(errorCounts));
				}
				if(failedRows.size()>0){
						for(IntObjectCursor<WriteResult> cursor:failedRows){
								errorMsgs.add(cursor.value.getErrorMessage());
						}

						if(errorMsgs.size()>0)
								errors.add(new WriteFailedException(errorMsgs));

						for(IntObjectCursor<WriteResult> cursor: failedRows){
								if(cursor.value.canRetry())
										toRetry.add((KVPair) allWritesBuffer[cursor.key]);
						}
				}

        if(toRetry.size()>0){
            return retryFailedWrites(writeConfiguration.getMaximumRetries(), bulkWrite.getTxnId(), toRetry);
        }
				return Collections.emptyList();
    }

    private List<BulkWrite> retryFailedWrites(int tries, String txnId, ObjectArrayList<KVPair> failedWrites) throws Exception {
        if(tries<0)
            throw new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
				List<BulkWrite> newBuckets = getBulkWrites(txnId);
        if(WriteUtils.bucketWrites(failedWrites, newBuckets)){
						if(LOG.isTraceEnabled()){
//								List<Integer> sizes = Lists.transform(newBuckets,new Function<BulkWrite, Integer>() {
//										@Override
//										public Integer apply(@Nullable BulkWrite input) {
//												if(input==null) return 0;
//												return input.getSize();
//										}
//								});
								SpliceLogUtils.trace(LOG,"[%d] Retrying writes with %d buckets: %s",id, newBuckets.size(),newBuckets);
						}
						return newBuckets;
        }else{
            return retryFailedWrites(tries-1,txnId,failedWrites);
        }
    }

		private List<BulkWrite> getBulkWrites(String txnId) throws Exception {
				Set<HRegionInfo> regionInfo = getRegionsFromCache(writeConfiguration.getMaximumRetries());
				return getWriteBuckets(txnId, regionInfo);
		}

		private List<BulkWrite> retry(int tries, BulkWrite bulkWrite) throws Exception {
        return retryFailedWrites(tries, bulkWrite.getTxnId(), bulkWrite.getMutations());
    }

    private List<BulkWrite> getWriteBuckets(String txnId,Set<HRegionInfo> regionInfos){
        List<BulkWrite> writes = Lists.newArrayListWithCapacity(regionInfos.size());
        for(HRegionInfo info:regionInfos){
            writes.add(new BulkWrite(txnId, info.getStartKey()));
        }
        return writes;
    }

    private Set<HRegionInfo> getRegionsFromCache(int numTries) throws Exception {
        Set<HRegionInfo> values;
        do{
            numTries--;
						sleeper.sleep(WriteUtils.getWaitTime(writeConfiguration.getMaximumRetries()-numTries+1,writeConfiguration.getPause()));
            regionCache.invalidate(tableName);
            values = regionCache.getRegions(tableName);
        }while(numTries>=0 && (values==null||values.size()<=0));

        if(numTries<0){
           throw new DoNotRetryIOException("Unable to obtain region information");
        }
        return values;
    }

    public static class ActionStatusReporter{
        final AtomicInteger numExecutingFlushes = new AtomicInteger(0);
        final AtomicLong totalFlushesSubmitted = new AtomicLong(0l);
        final AtomicLong failedBufferFlushes = new AtomicLong(0l);
        final AtomicLong writeConflictBufferFlushes = new AtomicLong(0l);
        final AtomicLong notServingRegionFlushes = new AtomicLong(0l);
        final AtomicLong wrongRegionFlushes = new AtomicLong(0l);
        final AtomicLong timedOutFlushes = new AtomicLong(0l);

        final AtomicLong globalFailures = new AtomicLong(0l);
        final AtomicLong partialFailures = new AtomicLong(0l);

        final AtomicLong maxFlushTime = new AtomicLong(0l);
        final AtomicLong minFlushTime = new AtomicLong(Long.MAX_VALUE);

        final AtomicLong maxFlushSizeBytes = new AtomicLong(0l);
        final AtomicLong minFlushSizeBytes = new AtomicLong(0l);
        final AtomicLong totalFlushSizeBytes = new AtomicLong(0l);

        final AtomicLong maxFlushEntries = new AtomicLong(0l);
        final AtomicLong minFlushEntries = new AtomicLong(0l);

        final AtomicLong totalFlushEntries = new AtomicLong(0l);
        final AtomicLong totalFlushTime = new AtomicLong(0l);
				final AtomicLong rejectedCount = new AtomicLong(0l);

				public void reset(){
            totalFlushesSubmitted.set(0);
            failedBufferFlushes.set(0);
            writeConflictBufferFlushes.set(0);
            notServingRegionFlushes.set(0);
            wrongRegionFlushes.set(0);
            timedOutFlushes.set(0);

            globalFailures.set(0);
            partialFailures.set(0);
            maxFlushTime.set(0);
            minFlushTime.set(0);
            totalFlushTime.set(0);

            maxFlushSizeBytes.set(0);
            minFlushEntries.set(0);
            totalFlushSizeBytes.set(0);

            maxFlushEntries.set(0);
            minFlushEntries.set(0);
            totalFlushEntries.set(0);
        }

        public void complete(long timeTakenMs) {
            totalFlushTime.addAndGet(timeTakenMs);
            numExecutingFlushes.decrementAndGet();
        }
    }
}
