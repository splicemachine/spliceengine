package com.splicemachine.pipeline.utils;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.tools.HostnameUtil;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class PipelineUtils extends PipelineConstants{
    private static final Logger LOG=Logger.getLogger(PipelineUtils.class);
    private static final String hostName=HostnameUtil.getHostname();


    public static Collection<KVPair> doPartialRetry(BulkWrite bulkWrite,BulkWriteResult response,List<Throwable> errors,long id) throws Exception{
        IntOpenHashSet notRunRows=response.getNotRunRows();
        IntObjectOpenHashMap<WriteResult> failedRows=response.getFailedRows();
        Collection<KVPair> toRetry=new ArrayList<>(failedRows.size()+notRunRows.size());
        List<String> errorMsgs=Lists.newArrayListWithCapacity(failedRows.size());
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
            Map<Code,Integer> errorCounts =new EnumMap<>(Code.class);
            for(IntObjectCursor<WriteResult> failedCursor : failedRows){
                Code code=failedCursor.value.getCode();
                if(!errorCounts.containsKey(code)){
                    errorCounts.put(code,1);
                }else
                    errorCounts.put(code,errorCounts.get(code)+1);
            }
            SpliceLogUtils.trace(LOG,"[%d] %d failures with types: %s",id,failedRows.size(),errorCounts);
        }

        return toRetry;
    }

    public static InputStream getSnappyInputStream(InputStream input) throws IOException{
        if(supportsNative){
            return snappy.createInputStream(input);
        }
        return input;
    }

    public static OutputStream getSnappyOutputStream(OutputStream outputStream) throws IOException{
        if(supportsNative){
            return snappy.createOutputStream(outputStream);
        }
        return outputStream;
    }

    public static long getWaitTime(int retryCount,long pauseInterval){
        /*
         * We want to perform an Exponential backoff here. By that, we mean that the more often
         * we have to retry an operation, the longer we should wait before performing that retry. This
         * uses a bit of fancy logic to determine exactly how long that interval should be.
         *
         * First, we have the base interval "pauseInterval", which is our unit of wait time. Each time we
         * ask to wait, we want to wait in multiples of this pause interval. The exponential part comes
         * from sequencing where that multiple is proportional to the number of times we wait. So the main
         * body of this method is in determining that growth factor (called waitFactor or wf in various places here).
         *
         * We start here with an algorithm of waitFactor=2^(retryCount). Unfortunately, this leads to awkward
         * sequencing in practice, because it would lead to the sequence 2*pauseInterval,4*pI,8*pI,...; if the retry
         * count is 35 (HBase's default), and the pause interval is 1 second, we would wait a total of
         * sum(2^i,i from 1 to 35) ~ 34 billion seconds ~ 1100 years. This isn't a reasonable retry cycle (obviously).
         *
         * To make the sequencing a little more practical, we "tier off" the waitFactor--that is, instead of the
         * sequence 2,4,8,16,... we do something more like 2,2,4,4,4,4,8,8,8,8,8,8,8,8,16,... which will still grow
         * exponentially, but will result in a realistic time frame for waiting (We are basically taking the
         * step function of 2^x). This boils down to the formula
         *
         * waitFactor=2^(highest 1-bit in retryCount).
         *
         * thus, we get
         * retryCount | waitFactor
         * 1          | 2
         * 2          | 4
         * 3          | 4
         * 4          | 8
         * 5          | 8
         * 6          | 8
         * 7          | 8
         * 8          | 16
         * ...
         *
         * this is very nice--in the same scenario, we would wait a total of
         * sum(2^highestOneBit(i),i from 1 to 35) = 470 seconds ~ 7.8 minutes. This is a MUCH nicer sequence.
         *
         * However, it's still SLIGHTLY off, because the first time you wait is 2*pauseInterval, which is contrary
         * to human expectations--you'd like to wait for just 1*pauseInterval for a few times before moving on. To do
         * this we divide the waitFactor by 2, giving the sequence 1,2,2,4,4,4,4,8,8,8,8,8,8,8,8,...
         * which gives us a wait time of about 235 seconds (~4 minutes) while also having nice human friendly outputs.
         *
         * However, if the retry limit is unbounded, this growth sequence is unbounded as well. We want to cap
         * this growth rate at a certain point; after that point we just want to wait a constant amount of time. In
         * a pretty-much-arbitrary way, I (-sf-) chose 32 because it's pretty.
         *
         * Finally, we realize that this is often used for network behavior (in fact, it's only used for network behavior),
         * and we want to avoid contention storms--situations in which N machines are all writing at the same time
         * over and over again, causing spikes in contention at regular periodic intervals. To eliminate this,
         * we add a jitter--a random, bounded value which is added to the total wait time to avoid contention. We choose
         * as a bounding factor the Max(waitFactor/8,pauseInterval/8), which gives us either +=12.5% of the waitFactor
         * or 12.5% of the pauseInterval as our jitter window, whichever is largest.
         *
         */
        int maxWaitFactor=32;
        long waitTime;
        long jitter;
        if(retryCount==0 || retryCount==1){
            waitTime=pauseInterval;
            jitter=pauseInterval/8; //~12.5% of the interval is out jitter window
        }else if(retryCount>maxWaitFactor){
            waitTime=(maxWaitFactor>>1)*pauseInterval;
            jitter=maxWaitFactor>>3; //~12.5% of the scale factor
        }else{
            int wf=maxWaitFactor;
            //find the highest set 1-bit less than the maxWaitFactor
            while(wf>0){
                if((retryCount&wf)!=0){
                    break;
                }else
                    wf>>=1;
            }
            waitTime=(wf>>1)*pauseInterval;
            jitter=Math.max(wf>>4,pauseInterval/8);
        }
        long jitterTime=jitter>0?ThreadLocalRandom.current().nextLong(-jitter,jitter):0;
        return waitTime+jitterTime;
    }

    public static void main(String...args) throws Exception{
        long waitTime = 1L;
        for(int i=2;i<=35;i++){
            int w = 32;
            while(w>0){
                if((i & w)!=0){
                    break;
                }else w>>=1;
            }
            waitTime +=(w>>1);
        }
        System.out.printf("WaitTime = %d s%n",waitTime);
    }

    /**
     * Get the cached regions for the table.  If the cache returns zero regions, invalidate the cache entry for the table and
     * retry a number of times assuming that the information is temporarily unavailable.  If after the retries,
     * there are still zero regions, throw an IOException since all tables should have at least one region.
     *
     * @param regionCache
     * @param tableName
     * @return
     * @throws IOException          if unable to get region information for the table (if the # of regions is zero)
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static SortedSet<Pair<HRegionInfo, ServerName>> getRegions(RegionCache regionCache,byte[] tableName) throws IOException, ExecutionException, InterruptedException{
        SortedSet<Pair<HRegionInfo, ServerName>> regions=regionCache.getRegions(tableName);
        if(regions.size()<=0){
            int numTries=50; // TODO Configurable, increased to 50 from 5 JL
            while(numTries>0){
                Thread.sleep(PipelineUtils.getWaitTime(numTries,200));
                regionCache.invalidate(tableName);
                regions=regionCache.getRegions(tableName);
                if(regions.size()>0) break;
                numTries--;
            }
            if(regions.size()<=0)
                throw new IOException("Unable to get region information for table "+Bytes.toString(tableName));
        }
        return regions;
    }

    public static String getHostName(){
        return hostName;
    }

    public static byte[] toCompressedBytes(Object object) throws IOException{
        Output output=null;
        OutputStream compressedOutput=null;
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        KryoObjectOutput koo;
        KryoPool pool=SpliceKryoRegistry.getInstance();
        Kryo kryo=pool.get();
        try{
            compressedOutput=PipelineUtils.getSnappyOutputStream(baos);
            output=new Output(compressedOutput);
            koo=new KryoObjectOutput(output,kryo);
            koo.writeObject(object);
            koo.flush();
            compressedOutput.flush();
            return baos.toByteArray();
        }finally{
            pool.returnInstance(kryo);
            Closeables.closeQuietly(output);
            Closeables.closeQuietly(compressedOutput);
            Closeables.closeQuietly(baos);
        }
    }

    public static <T> T fromCompressedBytes(byte[] bytes,Class<T> clazz) throws IOException{
        Input input=null;
        ByteArrayInputStream bais=null;
        InputStream compressedInput=null;
        KryoObjectInput koi;
        KryoPool pool=SpliceKryoRegistry.getInstance();
        Kryo kryo=pool.get();
        try{
            bais=new ByteArrayInputStream(bytes);
            compressedInput=PipelineUtils.getSnappyInputStream(bais);
            input=new Input(compressedInput);
            koi=new KryoObjectInput(input,kryo);
            return (T)koi.readObject();
        }catch(ClassNotFoundException e){
            throw new IOException(e);
        }finally{
            pool.returnInstance(kryo);
            Closeables.closeQuietly(input);
            Closeables.closeQuietly(compressedInput);
            Closeables.closeQuietly(bais);
        }
    }

}    