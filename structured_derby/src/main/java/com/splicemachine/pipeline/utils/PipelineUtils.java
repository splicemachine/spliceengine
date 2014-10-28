package com.splicemachine.pipeline.utils;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.hbase.table.SpliceHTableUtil;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteFailedException;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class PipelineUtils extends PipelineConstants {
    private static final Logger LOG = Logger.getLogger(PipelineUtils.class);
	static{
		try {
				hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
				throw new RuntimeException(e);
		}
	}


    public static ObjectArrayList<KVPair> doPartialRetry(BulkWrite bulkWrite, BulkWriteResult response, List<Throwable> errors, long id) throws Exception {
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
            return toRetry;
        }
		return PipelineConstants.emptyList;
    }

    public static InputStream getSnappyInputStream(InputStream input) throws IOException {
    	if (supportsNative)
    		return snappy.createInputStream(input);
    	return new DataInputStream(input);
    }

    public static OutputStream getSnappyOutputStream(OutputStream outputStream) throws IOException {
    	if (supportsNative)
    		return snappy.createOutputStream(outputStream);
    	return new DataOutputStream(outputStream);
    }

    public static long getWaitTime(int tryNum,long pause) {
        return SpliceHTableUtil.getWaitTime(tryNum,pause);
    }
    
    public static SortedSet<Pair<HRegionInfo,ServerName>> getRegions(RegionCache regionCache, byte[] tableName) throws IOException, ExecutionException, InterruptedException {
        SortedSet<Pair<HRegionInfo,ServerName>> regions = regionCache.getRegions(tableName);
        if(regions.size()<=0){
            int numTries=50; // TODO Configurable, increased to 50 from 5 JL
            while(numTries>0){
                Thread.sleep(PipelineUtils.getWaitTime(numTries,200));
                regionCache.invalidate(tableName);
                regions = regionCache.getRegions(tableName);
                if(regions.size()>0) break;
                numTries--;
            }
            if(regions.size()<=0)
                throw new IOException("Unable to get region information for table "+ Bytes.toString(tableName));
        }
        return regions;    	
    }    
    
	private static final String hostName;

    
		public static String getHostName() {
				return hostName;
		}
		
		public static byte[] toCompressedBytes(Object object) throws IOException {
			Output output = null;
			OutputStream compressedOutput = null;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			KryoObjectOutput koo;
			KryoPool pool = SpliceDriver.getKryoPool();
			Kryo kryo = pool.get();
			try {
				compressedOutput = PipelineUtils.getSnappyOutputStream(baos);
				output = new Output(compressedOutput);
				koo = new KryoObjectOutput(output,kryo);
				koo.writeObject(object);
				koo.flush();
				compressedOutput.flush();
				return baos.toByteArray();
			} finally {
				pool.returnInstance(kryo);
				Closeables.closeQuietly(output);
				Closeables.closeQuietly(compressedOutput);
				Closeables.closeQuietly(baos);
				output = null;
				compressedOutput = null;
				baos = null;
				koo = null;
			}
		}

		public static <T> T fromCompressedBytes(byte[] bulkWriteBytes, Class<T> clazz) throws IOException {
			Input input = null;
			ByteArrayInputStream bais = null;
			InputStream compressedInput = null;
			KryoObjectInput koi;
			KryoPool pool = SpliceDriver.getKryoPool();
			Kryo kryo = pool.get();
			try {
				bais = new ByteArrayInputStream(bulkWriteBytes);
				compressedInput = PipelineUtils.getSnappyInputStream(bais);
				input = new Input(compressedInput);
				koi = new KryoObjectInput(input,kryo); 
				return (T) koi.readObject();
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			} finally {
				pool.returnInstance(kryo);
				Closeables.closeQuietly(input);
				Closeables.closeQuietly(compressedInput);
				Closeables.closeQuietly(bais);
				input = null;
				compressedInput = null;
				bais = null;
			}
		}
		
}    