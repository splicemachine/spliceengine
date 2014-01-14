package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.NoRetryExecRPCInvoker;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.log4j.Logger;
import org.jruby.util.collections.IntHashMap;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
final class BulkWriteAction implements Callable<Void> {
    private static final Class<BatchProtocol> batchProtocolClass = BatchProtocol.class;
    @SuppressWarnings("unchecked")
    private static final Class<? extends CoprocessorProtocol>[] protoClassArray = new Class[]{batchProtocolClass};

    private static final Logger LOG = Logger.getLogger(BulkWriteAction.class);
    private static final AtomicLong idGen = new AtomicLong(0l);

    private BulkWrite bulkWrite;
    private final List<Throwable> errors = new CopyOnWriteArrayList<Throwable>();
    private final Writer.WriteConfiguration writeConfiguration;
    private final RegionCache regionCache;
    private final HConnection connection;
    private final ActionStatusReporter statusReporter;
    private final byte[] tableName;
    private final long id = idGen.incrementAndGet();

    public BulkWriteAction(byte[] tableName,
                           BulkWrite bulkWrite,
                           RegionCache regionCache,
                           Writer.WriteConfiguration writeConfiguration,
                           HConnection connection,
                           ActionStatusReporter statusReporter) {
        this.tableName = tableName;
        this.bulkWrite = bulkWrite;
        this.regionCache = regionCache;
        this.writeConfiguration = writeConfiguration;
        this.connection = connection;
        this.statusReporter = statusReporter;
    }

    @Override
    public Void call() throws Exception {
        statusReporter.numExecutingFlushes.incrementAndGet();
        reportSize();
        long start = System.currentTimeMillis();
        try{
            tryWrite(writeConfiguration.getMaximumRetries(),Collections.singletonList(bulkWrite),false);
        }finally{
            //called no matter what
						long end = System.currentTimeMillis();
						long timeTakenMs = end - start;
						int numRecords = bulkWrite.getMutations().size();
            writeConfiguration.writeComplete(timeTakenMs,numRecords);
						statusReporter.complete(timeTakenMs, numRecords);
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
        return null;
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

		private void tryWrite(int numTriesLeft,List<BulkWrite> bulkWrites,boolean refreshCache) throws Exception {
				if(numTriesLeft<0)
						throw new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
				for(BulkWrite bulkWrite:bulkWrites){
						if (!bulkWrite.getMutations().isEmpty()) // Remove calls when writes are put back into buckets and the bucket is empty.
								doRetry(numTriesLeft,bulkWrite,refreshCache);
				}
    }

    private void doRetry(int tries, BulkWrite bulkWrite,boolean refreshCache) throws Exception{
        Configuration configuration = SpliceConstants.config;
        NoRetryExecRPCInvoker invoker = new NoRetryExecRPCInvoker(configuration,connection,
                batchProtocolClass,tableName,bulkWrite.getRegionKey(),refreshCache);
        BatchProtocol instance = (BatchProtocol) Proxy.newProxyInstance(configuration.getClassLoader(),
                protoClassArray, invoker);
        boolean thrown=false;
        try{
            SpliceLogUtils.trace(LOG,"[%d] %s",id,bulkWrite);
						byte[] bytes = instance.bulkWrite(bulkWrite.toBytes());
						BulkWriteResult response = BulkWriteResult.fromBytes(bytes);
            SpliceLogUtils.trace(LOG, "[%d] %s", id, response);
            IntHashMap<WriteResult> failedRows = response.getFailedRows();
            if(failedRows!=null && failedRows.size()>0){
                Writer.WriteResponse writeResponse = writeConfiguration.partialFailure(response,bulkWrite);
                switch (writeResponse) {
                    case THROW_ERROR:
                        thrown=true;
                        throw parseIntoException(response);
                    case RETRY:
                        if(LOG.isDebugEnabled())
                            LOG.debug(String.format("Retrying write after receiving partial error %s",response));
                        doPartialRetry(tries,bulkWrite,response);
                    default:
                        //return
                }
            }
        }catch(Exception e){
            if(thrown)
                throw e;

            Writer.WriteResponse writeResponse = writeConfiguration.globalError(e);
            switch(writeResponse){
                case THROW_ERROR:
                    throw e;
                case RETRY:
                    errors.add(e);
                    if(LOG.isDebugEnabled())
                        LOG.debug("Retrying write after receiving global error",e);
                    if (e instanceof RegionTooBusyException) {
                        Thread.sleep(WriteUtils.getWaitTime(writeConfiguration.getMaximumRetries()-tries+1, writeConfiguration.getPause()));
                        doRetry(tries,bulkWrite,false);
                    } else {
                    	retry(tries, bulkWrite);
                    }
            }
        }
    }

    private Exception parseIntoException(BulkWriteResult response) {
        IntHashMap<WriteResult> failedRows = response.getFailedRows();
        List<Throwable> errors = Lists.newArrayList();
        for(Integer failedRow:failedRows.keySet()){
            errors.add(Exceptions.fromString(failedRows.get(failedRow)));
        }
        return new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
    }

    private void doPartialRetry(int tries, BulkWrite bulkWrite, BulkWriteResult response) throws Exception {

        IntArrayList notRunRows = response.getNotRunRows();
        IntHashMap<WriteResult> failedRows = response.getFailedRows();
        Set<Integer> rowsToRetry = Sets.newHashSet(failedRows.keySet());

				int size = notRunRows.size();
				for(int i=0;i<size;i++){
					rowsToRetry.add(notRunRows.buffer[i]);
				}

        Collection<WriteResult> results = failedRows.values();
        List<String> errorMsgs = Lists.newArrayListWithCapacity(results.size());
        for(WriteResult result:results){
            errorMsgs.add(result.getErrorMessage());
        }

        errors.add(new WriteFailedException(errorMsgs));

        ObjectArrayList<KVPair> allWrites = bulkWrite.getMutations();
        ObjectArrayList<KVPair> failedWrites = ObjectArrayList.newInstanceWithCapacity(rowsToRetry.size());
				for(int rowToRetry:rowsToRetry){
            failedWrites.add(allWrites.get(rowToRetry));
        }

        if(failedWrites.size()>0){
            retryFailedWrites(tries, bulkWrite.getTxnId(), failedWrites);
        }
    }

    private void retryFailedWrites(int tries, String txnId, ObjectArrayList<KVPair> failedWrites) throws Exception {
        if(tries<0)
            throw new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
        Set<HRegionInfo> regionInfo = getRegionsFromCache(writeConfiguration.getMaximumRetries());
        List<BulkWrite> newBuckets = getWriteBuckets(txnId,regionInfo);
        if(WriteUtils.bucketWrites(failedWrites, newBuckets)){
            tryWrite(tries-1,newBuckets,true);
        }else{
            retryFailedWrites(tries-1,txnId,failedWrites);
        }
    }

    private void retry(int tries, BulkWrite bulkWrite) throws Exception {
        retryFailedWrites(tries, bulkWrite.getTxnId(), bulkWrite.getMutations());
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
            Thread.sleep(WriteUtils.getWaitTime(writeConfiguration.getMaximumRetries()-numTries+1, writeConfiguration.getPause()));
            regionCache.invalidate(tableName);
            values = regionCache.getRegions(tableName);
        }while(numTries>=0 && (values==null||values.size()<=0));

        if(numTries<0){
           throw new IOException("Unable to obtain region information");
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

        public void complete(long timeTakenMs,int numRecords) {
            totalFlushTime.addAndGet(timeTakenMs);
            numExecutingFlushes.decrementAndGet();
        }
    }
}
