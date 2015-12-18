package com.splicemachine.pipeline.utils;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.*;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class PipelineUtils extends PipelineConstants {
    private static final Logger LOG = Logger.getLogger(PipelineUtils.class);
	private static final String hostName = HostnameUtil.getHostname();


		public static Collection<KVPair> doPartialRetry(BulkWrite bulkWrite, BulkWriteResult response, List<Throwable> errors, long id) throws Exception {
				IntOpenHashSet notRunRows = response.getNotRunRows();
				IntObjectOpenHashMap<WriteResult> failedRows = response.getFailedRows();
				Collection<KVPair> toRetry = new ArrayList<>(failedRows.size()+notRunRows.size());
				List<String> errorMsgs = Lists.newArrayListWithCapacity(failedRows.size());
				int i=0;
				Collection<KVPair> allWrites = bulkWrite.getMutations();
				for(KVPair kvPair:allWrites){
						if(notRunRows.contains(i))
								toRetry.add(kvPair);
						else {
								WriteResult writeResult = failedRows.get(i);
								if (writeResult != null) {
										errorMsgs.add(writeResult.getErrorMessage());
										if(writeResult.canRetry())
												toRetry.add(kvPair);
								}
						}
						i++;
				}
				if(LOG.isTraceEnabled()){
						int[] errorCounts = new int[11];
						for(IntObjectCursor<WriteResult> failedCursor:failedRows){
								errorCounts[failedCursor.value.getCode().ordinal()]++;
						}
						SpliceLogUtils.trace(LOG,"[%d] %d failures with types: %s",id,failedRows.size(),Arrays.toString(errorCounts));
				}

				return toRetry;
		}

		public static InputStream getSnappyInputStream(InputStream input) throws IOException {
				if (supportsNative) {
                    return snappy.createInputStream(input);
                }
				return input;
		}

		public static OutputStream getSnappyOutputStream(OutputStream outputStream) throws IOException {
				if (supportsNative) {
                    return snappy.createOutputStream(outputStream);
                }
				return outputStream;
		}

		public static String getHostName() {
				return hostName;
		}

		public static byte[] toCompressedBytes(Object object) throws IOException {
				Output output = null;
				OutputStream compressedOutput = null;
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				KryoObjectOutput koo;
				KryoPool pool = SpliceKryoRegistry.getInstance();
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
				}
		}

		public static <T> T fromCompressedBytes(byte[] bytes, Class<T> clazz) throws IOException {
				Input input = null;
				ByteArrayInputStream bais = null;
				InputStream compressedInput = null;
				KryoObjectInput koi;
				KryoPool pool = SpliceKryoRegistry.getInstance();
				Kryo kryo = pool.get();
				try {
						bais = new ByteArrayInputStream(bytes);
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
				}
		}

}    