package com.splicemachine.pipeline.client;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Lists;
import com.splicemachine.access.api.PartitionFactory;
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
import com.splicemachine.utils.Sleeper;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class BulkWriteAction implements Callable<WriteStats>{
    private static final Logger LOG=Logger.getLogger(BulkWriteAction.class);
    private static final Logger RETRY_LOG=Logger.getLogger(BulkWriteAction.class.getName()+".retries");
    private static final AtomicLong idGen=new AtomicLong(0l);
    private BulkWrites bulkWrites;
    private final List<Throwable> errors=Lists.newArrayListWithExpectedSize(0);
    private final WriteConfiguration writeConfiguration;
    private final ActionStatusReporter statusReporter;
    private final byte[] tableName;
    private final long id=idGen.incrementAndGet();
    private final BulkWriterFactory writerFactory;
    private final PipelineExceptionFactory pipelineExceptionFactory;
    private final Sleeper sleeper;
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
                           Sleeper sleeper){
        this.tableName=tableName;
        this.bulkWrites=writes;
        this.writeConfiguration=writeConfiguration;
        this.statusReporter=statusReporter;
        this.writerFactory=writerFactory;
        MetricFactory possibleMetricFactory=writeConfiguration.getMetricFactory();
        this.metricFactory=possibleMetricFactory==null?Metrics.noOpMetricFactory():possibleMetricFactory;
        this.sleeper=metricFactory.isActive()?new Sleeper.TimedSleeper(sleeper,metricFactory):sleeper;
        this.rejectedCounter=metricFactory.newCounter();
        this.globalErrorCounter=metricFactory.newCounter();
        this.partialFailureCounter=metricFactory.newCounter();
        this.retryCounter=metricFactory.newCounter();
        this.writeTimer=metricFactory.newTimer();
        this.pipelineExceptionFactory = pipelineExceptionFactory;
        this.partitionFactory = partitionFactory;
    }

    @Override
    public WriteStats call() throws Exception{
        statusReporter.numExecutingFlushes.incrementAndGet();
        reportSize();
        long start=System.currentTimeMillis();
        try{
            Timer totalTimer=metricFactory.newTimer();
            totalTimer.startTiming();
            if(LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"Calling BulkWriteAction: id=%d, initialBulkWritesSize=%d, initialKVPairSize=%d",id,bulkWrites.numEntries(),bulkWrites.numEntries());
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
        }finally{
            long timeTakenMs=System.currentTimeMillis()-start;
            long numRecords=bulkWrites.numEntries();
            writeConfiguration.writeComplete(timeTakenMs,numRecords);
            statusReporter.complete(timeTakenMs);
            bulkWrites=null;
        }
    }

    private void reportSize(){
        boolean success;
        long bufferEntries=bulkWrites.numEntries();
        int numRegions=bulkWrites.numRegions();
        statusReporter.totalFlushEntries.addAndGet(bufferEntries);
        statusReporter.totalFlushRegions.addAndGet(numRegions);

        do{
            long currentMax=statusReporter.maxFlushRegions.get();
            success=currentMax>=bufferEntries || statusReporter.maxFlushRegions.compareAndSet(currentMax,bufferEntries);
        }while(!success);

        do{
            long currentMin=statusReporter.minFlushRegions.get();
            success=currentMin<=bufferEntries || statusReporter.minFlushRegions.compareAndSet(currentMin,bufferEntries);
        }while(!success);

        do{
            long currentMax=statusReporter.maxFlushEntries.get();
            success=currentMax>=bufferEntries || statusReporter.maxFlushEntries.compareAndSet(currentMax,bufferEntries);
        }while(!success);

        do{
            long currentMin=statusReporter.minFlushEntries.get();
            success=currentMin<=bufferEntries || statusReporter.minFlushEntries.compareAndSet(currentMin,bufferEntries);
        }while(!success);

        long bufferSizeBytes=bulkWrites.getBufferHeapSize();
        statusReporter.totalFlushSizeBytes.addAndGet(bufferSizeBytes);
        do{
            long currentMax=statusReporter.maxFlushSizeBytes.get();
            success=currentMax>=bufferSizeBytes || statusReporter.maxFlushSizeBytes.compareAndSet(currentMax,bufferSizeBytes);
        }while(!success);

        do{
            long currentMin=statusReporter.minFlushSizeBytes.get();
            success=currentMin<=bufferSizeBytes || statusReporter.maxFlushSizeBytes.compareAndSet(currentMin,bufferSizeBytes);
        }while(!success);

    }

    private void execute(BulkWrites bulkWrites) throws Exception{
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Executing BulkWriteAction: id=%d, bulkWrites=%s",id,bulkWrites);
        retryCounter.increment();
        boolean thrown=false;
        int numAttempts=0;
        LinkedList<BulkWrites> writesToPerform=Lists.newLinkedList();
        writesToPerform.add(bulkWrites);
        do{
            BulkWrites nextWrite=writesToPerform.removeFirst();
            retryPipingCallBuffer=null;
            if(!shouldWrite(nextWrite)) continue;
            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"Getting next BulkWrites in loop: id=%d, nextBulkWrites=%s",id,nextWrite);
            try{
                BulkWriter writer=writerFactory.newWriter();
                writeTimer.startTiming();
                BulkWritesResult bulkWritesResult=writer.write(nextWrite,numAttempts>0);
                writeTimer.stopTiming();
                Iterator<BulkWrite> bws=nextWrite.getBulkWrites().iterator();
                Collection<BulkWriteResult> results=bulkWritesResult.getBulkWriteResults();
                for(BulkWriteResult bulkWriteResult : results){
                    WriteResponse globalResponse=writeConfiguration.processGlobalResult(bulkWriteResult);
                    BulkWrite currentBulkWrite=bws.next();
                    switch(globalResponse){
                        case SUCCESS:
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

                            if(RETRY_LOG.isDebugEnabled())
                                SpliceLogUtils.debug(RETRY_LOG,
                                        "Retrying write after receiving global RETRY response: id=%d, bulkWriteResult=%s, bulkWrite=%s",
                                        id,bulkWriteResult,currentBulkWrite);
                            addToRetryCallBuffer(currentBulkWrite.getMutations(),nextWrite.getTxn(),bulkWriteResult.getGlobalResult().refreshCache());
                            continue;
                        case PARTIAL:
                            partialFailureCounter.increment();

                            WriteResponse writeResponse=writeConfiguration.partialFailure(bulkWriteResult,currentBulkWrite);
                            switch(writeResponse){
                                case THROW_ERROR:
                                    thrown=true;
                                    throw parseIntoException(bulkWriteResult);
                                case RETRY:
                                    if(RETRY_LOG.isTraceEnabled()){
                                        SpliceLogUtils.trace(RETRY_LOG,
                                                "Retrying write after receiving partial %s response: id=%d, bulkWriteResult=%s, failureCountsByType=%s",
                                                writeResponse,id,bulkWriteResult,getFailedRowsMessage(bulkWriteResult.getFailedRows()));
                                    }else if(RETRY_LOG.isDebugEnabled()){
                                        SpliceLogUtils.debug(RETRY_LOG,
                                                "Retrying write after receiving partial %s response: id=%d, bulkWriteResult=%s",
                                                writeResponse,id,bulkWriteResult);
                                    }

                                    Collection<KVPair> toRetry=PipelineUtils.doPartialRetry(currentBulkWrite,bulkWriteResult,id);
//																		rowsSuccessfullyWritten +=currentBulkWrite.getSize()-toRetry.size();
                                    // only sleep and redo cache if you have a failure not a lock contention issue
                                    boolean isFailure=bulkWriteResult.getFailedRows()!=null && bulkWriteResult.getFailedRows().size()>0;
                                    addToRetryCallBuffer(toRetry,nextWrite.getTxn(),isFailure);
//                                    if (isFailure) // Retry immediately if you do not have failed rows!
//                                        sleeper.sleep(PipelineUtils.getWaitTime(numAttempts,writeConfiguration.getPause()));
                                    break;
                                default:
                                    if(RETRY_LOG.isDebugEnabled())
                                        SpliceLogUtils.debug(RETRY_LOG,
                                                "Ignoring write after receiving unknown partial %s response: id=%d, bulkWriteResult=%s",
                                                writeResponse,id,bulkWriteResult);
                            }
                            break;
                        case IGNORE:
                            if(LOG.isTraceEnabled())
                                SpliceLogUtils.trace(LOG,"Global response indicates ignoring, so we ignore");
                            break;
                        default:
                            SpliceLogUtils.error(LOG,"Global response went down default path, assert");
                            throw new IllegalStateException("Programmer error: Unknown global response: "+globalResponse);
                    }
                }
            }catch(Throwable e){
                if(LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG,"Caught throwable: id=%d",id);
                globalErrorCounter.increment();
                if(thrown)
                    throw new ExecutionException(e);
                //noinspection ThrowableResultOfMethodCallIgnored
                if(pipelineExceptionFactory.processPipelineException(e) instanceof PipelineTooBusy){
                    if(RETRY_LOG.isDebugEnabled())
                        SpliceLogUtils.debug(RETRY_LOG,"Retrying write after receiving RegionTooBusyException: id=%d",id);
//                    sleeper.sleep(PipelineUtils.getWaitTime(numAttempts, writeConfiguration.getPause()));
                    writesToPerform.add(nextWrite);
                    continue;
                }

                WriteResponse writeResponse=writeConfiguration.globalError(e);
                switch(writeResponse){
                    case THROW_ERROR:
                        if(LOG.isTraceEnabled())
                            SpliceLogUtils.trace(LOG,"Throwing error after receiving global error: id=%d, class=%s, message=%s",id,e.getClass(),e.getMessage());
                        throw new ExecutionException(e);
                    case RETRY:
                        errors.add(e);
                        if(RETRY_LOG.isDebugEnabled()){
                            SpliceLogUtils.debug(RETRY_LOG,"Retrying write after receiving global error: id=%d, class=%s, message=%s",id,e.getClass(),e.getMessage());
                            if(RETRY_LOG.isTraceEnabled()){
                                RETRY_LOG.trace("Global error exception received: ",e);
                            }
                        }
                        rejectedCounter.increment();
                        statusReporter.rejectedCount.incrementAndGet();
                        TxnView nextTxn=nextWrite.getTxn();
                        boolean first=true;
                        for(BulkWrite bw : nextWrite.getBulkWrites()){
                            addToRetryCallBuffer(bw.getMutations(),nextTxn,first);
                            first=false;
                        }
//                        sleeper.sleep(PipelineUtils.getWaitTime(numAttempts, writeConfiguration.getPause()));
                        break;
                    default:
                        if(LOG.isInfoEnabled())
                            LOG.info(String.format("Ignoring error after receiving unknown global error %s response: id=%d ",writeResponse,id),e);
                }
            }
            numAttempts++;
            addToWritesToPerform(writesToPerform);
            retryPipingCallBuffer=null;
            if(numAttempts>100 && numAttempts%50==0)
                SpliceLogUtils.warn(LOG,"BulkWriteAction is taking a long time with %d attempts: id=%d",numAttempts,id);
        }while(writesToPerform.size()>0);
    }

    /**
     * Return an error message describing the types and number of failures in the BatchWrite.
     *
     * @param failedRows the rows which failed, and their respective error messages
     * @return error message describing the failed rows
     */
    private String getFailedRowsMessage(IntObjectOpenHashMap<WriteResult> failedRows){

        if(failedRows!=null && failedRows.size()>0){

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
        if(nextWrite.getBulkWrites().size()==0){
            if(LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"No actual writes in BulkWrites %s",nextWrite);
            return false;
        }
        return true;
    }

    private Exception parseIntoException(BulkWriteResult response){
        IntObjectOpenHashMap<WriteResult> failedRows=response.getFailedRows();
//        Set<Throwable> errors=Sets.newHashSet();
        Exception first = null;
        for(IntObjectCursor<WriteResult> cursor : failedRows){
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable e=pipelineExceptionFactory.processErrorResult(cursor.value);
            if(e instanceof WriteConflict){ //TODO -sf- find a way to add in StandardExceptions here
                return (Exception)e;
            }else if(first==null)
                first = (Exception)e;
        }
        return first;
    }

    private void addToRetryCallBuffer(Collection<KVPair> retryBuffer,TxnView txn,boolean refreshCache) throws Exception{
        if(retryBuffer==null) return; //actually nothing to do--probably doesn't happen, but just to be safe
        if(RETRY_LOG.isDebugEnabled())
            SpliceLogUtils.debug(RETRY_LOG,"[%d] addToRetryCallBuffer %d rows, refreshCache=%s",id,retryBuffer.size(),refreshCache);
        if(retryPipingCallBuffer==null)
            retryPipingCallBuffer=new PipingCallBuffer(partitionFactory.getTable(Bytes.toString(tableName)),txn,null,PipelineUtils.noOpFlushHook,writeConfiguration,null,false);
        if(refreshCache){
            writerFactory.invalidateCache(tableName);
            retryPipingCallBuffer.rebuild();
        }
        retryPipingCallBuffer.addAll(retryBuffer);
    }

    private void addToWritesToPerform(List<BulkWrites> writesToPerform) throws Exception{
        if(retryPipingCallBuffer!=null){
            writesToPerform.addAll(retryPipingCallBuffer.getBulkWrites());
        }
    }

}
