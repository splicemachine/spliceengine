package com.splicemachine.hbase.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.splicemachine.concurrent.KeyedCompletionService;
import com.splicemachine.concurrent.KeyedFuture;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.NoRetryExecRPCInvoker;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
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
 * Created on: 10/23/13
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

				/*
				 * There are situations where someone submits a coprocessor exec, but they give it the same start and stop key. This means
				 * that it is invoked against only a single row, which can only be in 1 region at a time. Thus, if the exec is failed
				 * and needs to be retried, then the retry should only be retried against a single row
				 *
				 * However, it someone submits a RANGE of rows (e.g. a start key which differs from stop, or an empty start and empty end key),
				 * then the retry should retry against ALL regions which may be contained within that region (meaning that more regions could
				 * be submitted the second time around).
				 *
				 * As a result, we have to make a distinction between executing on a single row, and executing against a range.
				 */
				if((startKey.length!=0 || endKey.length!=0) && Arrays.equals(startKey,endKey))
						execOnSingleRow(protocol,startKey,callable,callback);
				else
						submitOnRange(protocol, startKey, endKey, callable, callback);
    }

		private <T extends CoprocessorProtocol, R> void execOnSingleRow(Class<T> protocol, byte[] startKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws Throwable {
				Pair<byte[], byte[]> containingRegionBounds = getContainingRegion(startKey,0);

				if(LOG.isDebugEnabled())
						SpliceLogUtils.debug(LOG, "Submitting task to region bounded by [%s,%s)",
										Bytes.toStringBinary(containingRegionBounds.getFirst()), Bytes.toStringBinary(containingRegionBounds.getSecond()));
				ExecContext context = new ExecContext(containingRegionBounds);

				/*
				 * Loop through and retry until we either succeed, or get an error that we weren't expecting.
				 */
				while(true){
						try{
								doExecute(protocol,callable,callback,context);
								return; //exec successfully executed
						}catch(IOException ee){
								if(LOG.isDebugEnabled())
										LOG.debug("Exception caught when submitting coprocessor exec",ee);
								if(ee instanceof IncorrectRegionException || ee instanceof NotServingRegionException){
										/*
										 * We sent it to the wrong place, so we need to resubmit it. But since we
										 * pulled it from the cache, we first invalidate that cache
										 */
										wait(context.attemptCount); //wait for a bit to see if it clears up
										regionCache.invalidate(tableName);

										Pair<byte[],byte[]> failedKeys = context.keyBoundary;
										context.errors.add(ee);
										Pair<byte[],byte[]> resubmitKeys = getContainingRegion(startKey,context.attemptCount+1);
										if(LOG.isDebugEnabled())
												SpliceLogUtils.debug(LOG,"Resubmitting task to region bounded by [%s,%s)",
																Bytes.toStringBinary(resubmitKeys.getFirst()),Bytes.toStringBinary(resubmitKeys.getSecond()));

										context = new ExecContext(resubmitKeys,context.errors,context.attemptCount+1);
								}else{
										throw ee.getCause();
								}
						}
				}
		}

		protected <T extends CoprocessorProtocol, R> void submitOnRange(Class<T> protocol,
																																		byte[] startKey,
																																		byte[] endKey,
																																		Batch.Call<T, R> callable, Batch.Callback<R> callback) throws Throwable {
				List<Pair<byte[], byte[]>> keysToUse = getKeysDealWithSameStartStopKey(startKey, endKey,0);

				KeyedCompletionService<ExecContext,R> completionService = new KeyedCompletionService<ExecContext,R>(tableExecutor);
				int outstandingFutures = 0;
				for(Pair<byte[],byte[]> key: keysToUse){
						if(LOG.isDebugEnabled())
								SpliceLogUtils.debug(LOG, "Submitting task to region bounded by [%s,%s)",
												Bytes.toStringBinary(key.getFirst()), Bytes.toStringBinary(key.getSecond()));
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
								if(LOG.isDebugEnabled())
										LOG.debug("Exception caught when submitting coprocessor exec",cause);
								if(cause instanceof IncorrectRegionException || cause instanceof NotServingRegionException){
										/*
										 * We sent it to the wrong place, so we need to resubmit it. But since we
										 * pulled it from the cache, we first invalidate that cache
										 */
										ExecContext context = completedFuture.getKey();
										wait(context.attemptCount); //wait for a bit to see if it clears up
										regionCache.invalidate(tableName);

										Pair<byte[],byte[]> failedKeys = context.keyBoundary;
										context.errors.add(cause);
										List<Pair<byte[],byte[]>> resubmitKeys = getKeysDealWithSameStartStopKey(failedKeys.getFirst(), failedKeys.getSecond(),context.attemptCount+1);
										if(LOG.isDebugEnabled()){
												SpliceLogUtils.debug(LOG,"Found %d regions for exec bounded by [%s,%s)",resubmitKeys.size(),
																Bytes.toStringBinary(failedKeys.getFirst()),
																Bytes.toStringBinary(failedKeys.getSecond()));
										}
										for(Pair<byte[],byte[]> keys:resubmitKeys){
												if(LOG.isDebugEnabled())
														SpliceLogUtils.debug(LOG,"Resubmitting task to region bounded by [%s,%s)",
																		Bytes.toStringBinary(keys.getFirst()),Bytes.toStringBinary(keys.getSecond()));

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

		List<Pair<byte[],byte[]>> getKeysDealWithSameStartStopKey(byte[] startKey, byte[] endKey,int attempt) throws IOException{
        if((startKey.length!=0 || endKey.length!=0) && Arrays.equals(startKey,endKey))
            return Collections.singletonList(getContainingRegion(startKey, attempt));
        return getKeys(startKey, endKey,attempt);
    }

    private Pair<byte[], byte[]> getContainingRegion(byte[] startKey, int attemptCount) throws IOException {
        HRegionLocation regionLocation = this.connection.getRegionLocation(tableName, startKey, attemptCount > 0);
				for(int i=0;i<5;i++){
						if(!regionLocation.getRegionInfo().isSplitParent())
								break;
						else
								this.connection.getRegionLocation(tableName,startKey,true);
				}
				Preconditions.checkArgument(!regionLocation.getRegionInfo().isSplitParent(),"Unable to get a region that is not split!");
        return Pair.newPair(regionLocation.getRegionInfo().getStartKey(), regionLocation.getRegionInfo().getEndKey());
    }

    private void wait(int attemptCount) {
        try {
            Thread.sleep(SpliceHTableUtil.getWaitTime(attemptCount, SpliceConstants.pause));
        } catch (InterruptedException e) {
            Logger.getLogger(SpliceHTable.class).info("Interrupted while sleeping");
        }
    }

    private List<Pair<byte[], byte[]>> getKeys(byte[] startKey, byte[] endKey,int attemptCount) throws IOException {
        if(attemptCount>maxRetries) {
        	SpliceLogUtils.error(LOG, "Unable to obtain full region set from cache");
            throw new RetriesExhaustedException("Unable to obtain full region set from cache after "
                    + attemptCount+" attempts on table " + Bytes.toString(tableName)
                    + " with startKey " + Bytes.toStringBinary(startKey) + " and end key " + Bytes.toStringBinary(endKey));
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
            wait(attemptCount);
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
        		SpliceLogUtils.error(LOG, "First Key Miss, invalidate");
            wait(attemptCount);
            regionCache.invalidate(tableName);
            return getKeys(startKey,endKey,attemptCount+1);
        }
        for(int i=1;i<keysToUse.size();i++){
            Pair<byte[],byte[]> next = keysToUse.get(i);
            Pair<byte[],byte[]> last = keysToUse.get(i-1);
            if(!Arrays.equals(next.getFirst(),last.getSecond())){
            	if (LOG.isTraceEnabled())
            		SpliceLogUtils.error(LOG, "Keys are not contiguous miss, invalidate");
                wait(attemptCount);
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
            wait(attemptCount);
            regionCache.invalidate(tableName);
            return getKeys(startKey, endKey, attemptCount+1);
        }


        return keysToUse;
    }

    private <T extends CoprocessorProtocol, R> void submit(final Class<T> protocol,
                                                           final Batch.Call<T, R> callable,
                                                           final Batch.Callback<R> callback,
                                                           KeyedCompletionService<ExecContext,R> completionService,
                                                           final ExecContext context) throws RetriesExhaustedWithDetailsException {
        if(context.attemptCount>maxRetries){
            throw new RetriesExhaustedWithDetailsException(context.errors, Collections.<Row>emptyList(),Collections.<String>emptyList());
        }
        completionService.submit(context,new Callable<R>() {
            @Override
            public R call() throws Exception {
								return doExecute(protocol,callable,callback,context);
            }
				});
    }

		private <T extends CoprocessorProtocol, R> R doExecute(Class<T> protocol,Batch.Call<T,R> callable, Batch.Callback<R> callback,
																														 ExecContext context) throws IOException {
				Pair<byte[],byte[]> keys = context.keyBoundary;
				 byte[] startKeyToUse = keys.getFirst();
				NoRetryExecRPCInvoker invoker = new NoRetryExecRPCInvoker(getConfiguration(), connection, protocol, tableName, startKeyToUse, context.attemptCount>0);
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
