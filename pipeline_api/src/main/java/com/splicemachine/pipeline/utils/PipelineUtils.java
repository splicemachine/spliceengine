package com.splicemachine.pipeline.utils;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.callbuffer.PreFlushHook;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.sparkproject.guava.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class PipelineUtils{
    private static final Logger LOG=Logger.getLogger(PipelineUtils.class);

    public static final PreFlushHook noOpFlushHook = new PreFlushHook() {
        @Override
        public Collection<KVPair> transform(Collection<KVPair> buffer) throws Exception {
            return new ArrayList<>(buffer);
        }
    };

    public static Collection<KVPair> doPartialRetry(BulkWrite bulkWrite,BulkWriteResult response,long id) throws Exception{
        IntOpenHashSet notRunRows=response.getNotRunRows();
        IntObjectOpenHashMap<WriteResult> failedRows=response.getFailedRows();
        Collection<KVPair> toRetry=new ArrayList<>(failedRows.size()+notRunRows.size());
        List<String> errorMsgs= Lists.newArrayListWithCapacity(failedRows.size());
        int i=0;
        Collection<KVPair> allWrites=bulkWrite.getMutations();
        for(KVPair kvPair : allWrites){
            if(notRunRows.contains(i))
                toRetry.add(kvPair);
            else{
                WriteResult writeResult=failedRows.get(i);
                if(writeResult!=null){
                    errorMsgs.add(writeResult.getErrorMessage());
                    if(writeResult.canRetry())
                        toRetry.add(kvPair);
                }
            }
            i++;
        }
        if(LOG.isTraceEnabled()){
            int[] errorCounts=new int[11];
            for(IntObjectCursor<WriteResult> failedCursor : failedRows){
                errorCounts[failedCursor.value.getCode().ordinal()]++;
            }
            SpliceLogUtils.trace(LOG,"[%d] %d failures with types: %s",id,failedRows.size(),Arrays.toString(errorCounts));
        }

        return toRetry;
    }

    public static long getWaitTime(int retryCount,long pauseInterval){
        /*
         * Exponential backoff here to avoid over-forcing the retry. Use a random
         * interval to avoid having a bunch retry at identical times. The randomness is limited
         * to the maximum wait for the last interval and the minimum wait for the current attempt count;
         */
        int maxWaitFactor = 32;
        long waitTime;
        long jitter;
        if(retryCount==0||retryCount==1){
            waitTime=pauseInterval;
            jitter=pauseInterval/8; //~12.5% of the interval is out jitter window
        }else if(retryCount>maxWaitFactor){
            waitTime=(maxWaitFactor>>1)*pauseInterval;
            jitter = maxWaitFactor>>3; //~12.5% of the scale factor
        }else{
            int wf = maxWaitFactor;
            //find the highest set 1-bit less than the maxWaitFactor
            while(wf>0){
                if((retryCount&wf)!=0){
                    break;
                }else
                    wf>>=1;
            }
            waitTime = (wf>>1)*pauseInterval;
            jitter = Math.max(wf>>4,pauseInterval/8);
        }
        long jitterTime = jitter>0?ThreadLocalRandom.current().nextLong(-jitter,jitter):0;
        return waitTime+jitterTime;
    }

    public static void main(String...args) throws Exception{
        long totalSleepTime = 0l;
        for(int i=0;i<40;i++){
           totalSleepTime+=getWaitTime(i,1000L);
        }
        System.out.println(totalSleepTime);
    }
}
