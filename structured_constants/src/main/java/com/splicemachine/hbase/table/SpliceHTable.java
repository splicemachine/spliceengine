package com.splicemachine.hbase.table;

import com.google.common.collect.Lists;
import com.splicemachine.concurrent.KeyedCompletionService;
import com.splicemachine.concurrent.KeyedFuture;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.NoRetryExecRPCInvoker;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * @author Scott Fines
 *         Created on: 10/23/13
 */
public class SpliceHTable extends HTable {
    private final HConnection connection;
    private final ExecutorService tableExecutor;
    private final byte[] tableName;
    private final RegionCache regionCache;
    private final int maxRetries = SpliceConstants.numRetries;
    private static Logger LOG = Logger.getLogger(SpliceHTable.class);

    
    public SpliceHTable(byte[] tableName, HConnection connection, ExecutorService pool,
                        RegionCache regionCache) throws IOException {
        super(tableName, connection, pool);
        this.regionCache = regionCache;
        this.tableName = tableName;
        this.tableExecutor = pool;
        this.connection = connection;
    }

    @Override
    public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        try {
            SortedSet<HRegionInfo> regions = regionCache.getRegions(tableName);
            byte[][] startKeys = new byte[regions.size()][];
            byte[][] endKeys = new byte[regions.size()][];
            int regionPos=0;
            for(HRegionInfo regionInfo:regions){
                startKeys[regionPos] = regionInfo.getStartKey();
                endKeys[regionPos] = regionInfo.getEndKey();
                regionPos++;
            }
            return Pair.newPair(startKeys,endKeys);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }

    @Override
    public <T extends CoprocessorProtocol, R> void coprocessorExec(final Class<T> protocol,
                                                                   byte[] startKey,
                                                                   byte[] endKey,
                                                                   final Batch.Call<T, R> callable, final Batch.Callback<R> callback) throws Throwable {
        List<Pair<byte[], byte[]>> keysToUse = getKeys(startKey, endKey,0);

        KeyedCompletionService<ExecContext,R> completionService = new KeyedCompletionService<ExecContext,R>(tableExecutor);
        int outstandingFutures = 0;
        for(Pair<byte[],byte[]> key: keysToUse){
            ExecContext context = new ExecContext(key);
            submit(protocol, callable, callback, completionService, context);
            outstandingFutures++;
        }
        /*
         * Wait for all the futures to complete.
         *
         * Some Futures may have failed in a retryable manner (NotServingRegionException or WrongRegionException).
         * In those cases, you should resubmit, but since we got data out of the region cache, we should
         * invalidate and backoff before retrying.
         */

        while(outstandingFutures>0){
            KeyedFuture<ExecContext,R> completedFuture = completionService.take();
            try{
                outstandingFutures--;
                completedFuture.get();
            }catch(ExecutionException ee){
                Throwable cause = ee.getCause();
                if(cause instanceof NotServingRegionException ||
                        cause instanceof WrongRegionException){
                    /*
                     * We sent it to the wrong place, so we need to resubmit it. But since we
                     * pulled it from the cache, we first invalidate that cache
                     */
                    regionCache.invalidate(tableName);

                    ExecContext context = completedFuture.getKey();
                    Pair<byte[],byte[]> failedKeys = context.keyBoundary;
                    context.errors.add(cause);
                    List<Pair<byte[],byte[]>> resubmitKeys = getKeys(failedKeys.getFirst(),failedKeys.getSecond(),0);
                    for(Pair<byte[],byte[]> keys:resubmitKeys){
                        ExecContext newContext = new ExecContext(keys,context.errors,context.attemptCount+1);
                        submit(protocol,callable,callback,completionService,newContext);
                        outstandingFutures++;
                    }
                }else{
                    throw ee.getCause();
                }
            }
        }
    }

    private List<Pair<byte[], byte[]>> getKeys(byte[] startKey, byte[] endKey,int attemptCount) throws IOException {
        if(attemptCount>maxRetries) {
        	SpliceLogUtils.error(LOG, "Unable to obtain full region set from cache");
            throw new RetriesExhaustedException("Unable to obtain full region set from cache after "+ attemptCount+" attempts on table " + tableName + " with startKey " + startKey + " and end key " + endKey);
        }
        Pair<byte[][],byte[][]> startEndKeys = getStartEndKeys();
        byte[][] starts = startEndKeys.getFirst();
        byte[][] ends = startEndKeys.getSecond();

        List<Pair<byte[],byte[]>> keysToUse = Lists.newArrayList();
        for(int i=0;i<starts.length;i++){
            byte[] start = starts[i];
            byte[] end = ends[i];
            Pair<byte[],byte[]> intersect = BytesUtil.intersect(startKey, endKey, start, end);
            if(intersect!=null){
                keysToUse.add(intersect);
            }
        }

        if(keysToUse.size()<=0){
        	if (LOG.isTraceEnabled())
        		SpliceLogUtils.error(LOG, "Keys to use miss");
            regionCache.invalidate(tableName);
            return getKeys(startKey, endKey, attemptCount+1);
        }
        //make sure all our regions are adjacent to the region below us
        Collections.sort(keysToUse,new Comparator<Pair<byte[],byte[]>>(){

            @Override
            public int compare(Pair<byte[], byte[]> o1, Pair<byte[], byte[]> o2) {
                return Bytes.compareTo(o1.getFirst(),o2.getFirst());
            }
        });

        //make sure the start key of the first pair is the start key of the query
        Pair<byte[],byte[]> start = keysToUse.get(0);
        if(!Arrays.equals(start.getFirst(),startKey)){
        	if (LOG.isTraceEnabled())
        		SpliceLogUtils.error(LOG, "First Key Miss, invalidate");
        	regionCache.invalidate(tableName);
            return getKeys(startKey,endKey,attemptCount+1);
        }
        for(int i=1;i<keysToUse.size();i++){
            Pair<byte[],byte[]> next = keysToUse.get(i);
            Pair<byte[],byte[]> last = keysToUse.get(i-1);
            if(!Arrays.equals(next.getFirst(),last.getSecond())){
            	if (LOG.isTraceEnabled())
            		SpliceLogUtils.error(LOG, "Keys are not contiguous miss, invalidate");
                //we are missing some data, so recursively try again
                regionCache.invalidate(tableName);
                return getKeys(startKey,endKey,attemptCount+1);
            }
        }

        //make sure the end key of the last pair is the end key of the query
        Pair<byte[],byte[]> end = keysToUse.get(keysToUse.size()-1);
        if(!Arrays.equals(end.getSecond(),endKey)){
        	if (LOG.isTraceEnabled())
        		SpliceLogUtils.error(LOG, "Last Key Miss, invalidate");
            regionCache.invalidate(tableName);
            return getKeys(startKey, endKey, attemptCount+1);
        }


        return keysToUse;
    }

    private <T extends CoprocessorProtocol, R> void submit(final Class<T> protocol,
                                                           final Batch.Call<T, R> callable,
                                                           final Batch.Callback<R> callback,
                                                           KeyedCompletionService<ExecContext,R> completionService,
                                                           ExecContext context) throws RetriesExhaustedWithDetailsException {
        if(context.attemptCount>maxRetries){
            throw new RetriesExhaustedWithDetailsException(context.errors, null,null);
        }
        final Pair<byte[],byte[]> keys = context.keyBoundary;
        final byte[] startKeyToUse = keys.getFirst();
        completionService.submit(context,new Callable<R>() {
            @Override
            public R call() throws Exception {
                NoRetryExecRPCInvoker invoker = new NoRetryExecRPCInvoker(getConfiguration(), connection, protocol, tableName, startKeyToUse, true);
                @SuppressWarnings("unchecked") T instance = (T) Proxy.newProxyInstance(getConfiguration().getClassLoader(), new Class[]{protocol}, invoker);
                R result;
                if(callable instanceof BoundCall){
                    result = ((BoundCall<T,R>) callable).call(startKeyToUse,keys.getSecond(),instance);
                }else
                    result = callable.call(instance);
                if(callback!=null)
                    callback.update(invoker.getRegionName(),startKeyToUse,result);

                return result;
            }
        });
    }

    private static class ExecContext{
        private final Pair<byte[],byte[]> keyBoundary;
        private final List<Throwable> errors;
        private int attemptCount =0;

        private ExecContext(Pair<byte[], byte[]> keyBoundary) {
            this.keyBoundary = keyBoundary;
            this.errors = Lists.newArrayListWithExpectedSize(0);
        }

        public ExecContext(Pair<byte[], byte[]> keys, List<Throwable> errors, int attemptCount) {
            this.keyBoundary = keys;
            this.errors = errors;
            this.attemptCount = attemptCount;
        }
    }


}
