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
import org.apache.hadoop.hbase.client.ConnectionUtils;
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
        return ConnectionUtils.getPauseTime(pauseInterval,retryCount);
    }

    public static void main(String...args) throws Exception{
        long waitTime = 0L;
        for(int i=1;i<=80;i++){
            waitTime+=getWaitTime(i,250L);
        }
        System.out.printf("WaitTime = %d %n",waitTime);
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