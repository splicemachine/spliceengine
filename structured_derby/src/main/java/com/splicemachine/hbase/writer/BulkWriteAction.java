package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.NoRetryExecRPCInvoker;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
final class BulkWriteAction implements Callable<Void> {
    private static final Class<BatchProtocol> batchProtocolClass = BatchProtocol.class;
    @SuppressWarnings("unchecked")
    private static final Class<? extends CoprocessorProtocol>[] protoClassArray = new Class[]{batchProtocolClass};

    private final BulkWrite bulkWrite;
    private final List<Throwable> errors = new CopyOnWriteArrayList<Throwable>();
    private final Writer.RetryStrategy retryStrategy;
    private final RegionCache regionCache;
    private final HConnection connection;
    private final byte[] tableName;

    public BulkWriteAction(byte[] tableName,
                           BulkWrite bulkWrite,
                           RegionCache regionCache,
                           Writer.RetryStrategy retryStrategy,
                           HConnection connection) {
        this.tableName = tableName;
        this.bulkWrite = bulkWrite;
        this.regionCache = regionCache;
        this.retryStrategy = retryStrategy;
        this.connection = connection;
    }

    @Override
    public Void call() throws Exception {
        tryWrite(retryStrategy.getMaximumRetries(),Collections.singletonList(bulkWrite));
        return null;
    }

    private void tryWrite(int numTriesLeft,List<BulkWrite> bulkWrites) throws Exception {
        if(numTriesLeft<=0)
            throw new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
        for(BulkWrite bulkWrite:bulkWrites){
            doRetry(numTriesLeft,bulkWrite);
        }
    }

    private void doRetry(int tries, BulkWrite bulkWrite) throws Exception{
        Configuration configuration = SpliceConstants.config;
        NoRetryExecRPCInvoker invoker = new NoRetryExecRPCInvoker(configuration,connection,
                batchProtocolClass,tableName,bulkWrite.getRegionKey(),tries< retryStrategy.getMaximumRetries());
        BatchProtocol instance = (BatchProtocol) Proxy.newProxyInstance(configuration.getClassLoader(),
                protoClassArray, invoker);
        boolean thrown=false;
        try{
            BulkWriteResult response = instance.bulkWrite(bulkWrite);
            Map<Integer,WriteResult> failedRows = response.getFailedRows();
            if(failedRows!=null && failedRows.size()>0){
                Writer.WriteResponse writeResponse = retryStrategy.partialFailure(response,bulkWrite);
                switch (writeResponse) {
                    case THROW_ERROR:
                        thrown=true;
                        throw parseIntoException(response);
                    case RETRY:
                        doPartialRetry(tries,bulkWrite,response);
                    default:
                        //return
                }
            }
        }catch(Exception e){
            if(thrown)
                throw e;

            Writer.WriteResponse writeResponse = retryStrategy.globalError(e);
            switch(writeResponse){
                case THROW_ERROR:
                    throw e;
                case RETRY:
                    retry(tries, bulkWrite);
            }
        }
    }

    private Exception parseIntoException(BulkWriteResult response) {
        Map<Integer,WriteResult> failedRows = response.getFailedRows();
        List<Throwable> errors = Lists.newArrayList();
        for(Integer failedRow:failedRows.keySet()){
            errors.add(Exceptions.fromString(failedRows.get(failedRow)));
        }
        return new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
    }

    private void doPartialRetry(int tries, BulkWrite bulkWrite, BulkWriteResult response) throws Exception {

        List<Integer> notRunRows = response.getNotRunRows();
        Map<Integer,WriteResult> failedRows = response.getFailedRows();
        Set<Integer> rowsToRetry = Sets.newHashSet();
        rowsToRetry.addAll(notRunRows);
        rowsToRetry.addAll(failedRows.keySet());

        Collection<WriteResult> results = failedRows.values();
        List<String> errorMsgs = Lists.newArrayListWithCapacity(results.size());
        for(WriteResult result:results){
            errorMsgs.add(result.getErrorMessage());
        }

        errors.add(new WriteFailedException(errorMsgs));

        List<KVPair> allWrites = bulkWrite.getMutations();
        List<KVPair> failedWrites = Lists.newArrayListWithCapacity(rowsToRetry.size());
        for(Integer rowToRetry:rowsToRetry){
            failedWrites.add(allWrites.get(rowToRetry));
        }

        if(failedWrites.size()>0){
            Set<HRegionInfo> regionInfo = getRegionsFromCache(retryStrategy.getMaximumRetries());
            tryWrite(tries - 1, WriteUtils.bucketWrites(failedWrites, bulkWrite.getTxnId(), regionInfo));
        }
    }

    private void retry(int tries, BulkWrite bulkWrite) throws Exception {
        Set<HRegionInfo> info = getRegionsFromCache(retryStrategy.getMaximumRetries());
        tryWrite(tries - 1, WriteUtils.bucketWrites(bulkWrite.getMutations(), bulkWrite.getTxnId(),info));
    }

    private Set<HRegionInfo> getRegionsFromCache(int numTries) throws Exception {
        Set<HRegionInfo> values;
        do{
            numTries--;
            Thread.sleep(WriteUtils.getWaitTime(retryStrategy.getMaximumRetries()-numTries+1,retryStrategy.getPause()));
            regionCache.invalidate(tableName);
            values = regionCache.getRegions(tableName);
        }while(numTries>=0 && (values==null||values.size()<=0));

        if(numTries<0){
           throw new IOException("Unable to obtain region information");
        }
        return values;
    }
}
