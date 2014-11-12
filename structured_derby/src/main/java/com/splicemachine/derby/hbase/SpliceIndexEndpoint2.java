package com.splicemachine.derby.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.scheduler.SimpleThreadedTaskScheduler;
import com.splicemachine.pipeline.api.WriteBufferFactory;
import com.splicemachine.pipeline.coprocessor.BatchProtocol;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.Service;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.BulkWritesResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.pipeline.writecontextfactory.LocalWriteContextFactory;
import com.splicemachine.pipeline.writehandler.IndexWriteBufferFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import javax.management.*;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */


public class SpliceIndexEndpoint2 extends BaseEndpointCoprocessor implements BatchProtocol{
		SpliceBaseIndexEndpoint endpoint;
		
		@Override
		public void start(CoprocessorEnvironment env) {
				endpoint = new SpliceBaseIndexEndpoint();
				endpoint.start(env);
				super.start(env);
		}
		
		@Override
		public void stop(CoprocessorEnvironment env) {
				endpoint.stop(env);
		}
		
		/**
		 *
		 * Can it fail here?
		 *
		 * @param bulkWrites
		 * @return
		 * @throws IOException
		 */
		public BulkWritesResult bulkWrite(BulkWrites bulkWrites) throws IOException {
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "bulkWrite %s ",bulkWrites);
				BulkWritesResult result = new BulkWritesResult();
				Object[] buffer = bulkWrites.getBulkWrites().buffer;
				int size =  bulkWrites.getBulkWrites().size();
				long start = System.nanoTime();
				// start
				List<Pair<BulkWriteResult,SpliceIndexEndpoint2>> startPoints = new ArrayList<Pair<BulkWriteResult,SpliceIndexEndpoint2>>();
				WriteBufferFactory indexWriteBufferFactory = new IndexWriteBufferFactory();
				boolean dependent = isDependent();
				WriteSemaphore.Status status;
				int kvPairSize = bulkWrites.numEntries();
				status = (dependent)?control.acquireDependentPermit(kvPairSize):control.acquireIndependentPermit(kvPairSize);
				if(status== WriteSemaphore.Status.REJECTED) {
						rejectAll(result, size);
						return result;
				}

				try {
						for (int i = 0; i< size; i++) {
								BulkWrite bulkWrite = (BulkWrite) buffer[i];
								assert bulkWrite!=null;
								// Grab the instances endpoint and not this one
								SpliceIndexEndpoint2 endpoint = SpliceDriver.driver().getSpliceIndexEndpoint(bulkWrite.getEncodedStringName());
								if (endpoint == null) {
										if (LOG.isDebugEnabled())
												SpliceLogUtils.debug(LOG, "endpoint not found for region %s on region %s",bulkWrite.getEncodedStringName(), rce.getRegion().getRegionNameAsString());
										startPoints.add(Pair.newPair(new BulkWriteResult(new WriteResult(Code.NOT_SERVING_REGION, String.format("endpoint not found for region %s on region %s",bulkWrite.getEncodedStringName(), rce.getRegion().getRegionNameAsString()))), endpoint));
								}
								else {
										startPoints.add(Pair.newPair(sendUpstream(bulkWrite,endpoint,indexWriteBufferFactory), endpoint));
								}
						}

						// complete
						for (int i = 0; i< size; i++) {
								BulkWrite bulkWrite = (BulkWrite) buffer[i];
								Pair<BulkWriteResult,SpliceIndexEndpoint2> pair = startPoints.get(i);
								result.addResult(flushAndClose(pair.getFirst(),bulkWrite,pair.getSecond()));
						}
						timer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
						return result;
				} finally {
						switch (status) {
								case REJECTED:
										break;
								case DEPENDENT:
										control.releaseDependentPermit(kvPairSize);
										break;
								case INDEPENDENT:
										control.releaseIndependentPermit(kvPairSize);
										break;
						}
				}
		}

		private void rejectAll(BulkWritesResult result, int numResults) {
				this.rejectedMeter.mark();
				for (int i = 0; i < numResults; i++) {
						result.addResult(new BulkWriteResult(WriteResult.pipelineTooBusy(rce.getRegion().getRegionNameAsString())));
				}
		}
		private static BulkWriteResult sendUpstream(BulkWrite bulkWrite, SpliceIndexEndpoint2 endpoint, WriteBufferFactory indexWriteBufferFactory) throws IOException {
				assert bulkWrite.getTxn()!=null;
				assert endpoint != null;
				HRegion region = endpoint.rce.getRegion();
				try {
						region.startRegionOperation();
				}
				catch (NotServingRegionException nsre) {
						SpliceLogUtils.debug(LOG, "hbase not serving region %s",region.getRegionNameAsString());
						return new BulkWriteResult(new WriteResult(Code.NOT_SERVING_REGION,region.getRegionNameAsString()));
				}
				catch (RegionTooBusyException nsre) {
						SpliceLogUtils.debug(LOG, "hbase region too busy %s",region.getRegionNameAsString());
						return new BulkWriteResult(new WriteResult(Code.REGION_TOO_BUSY,region.getRegionNameAsString()));
				}
				catch (InterruptedIOException ioe) {
						SpliceLogUtils.error(LOG, "hbase region interrupted %s",region.getRegionNameAsString());
						return new BulkWriteResult(new WriteResult(Code.INTERRUPTED_EXCEPTON,region.getRegionNameAsString()));
				}
				try{
						WriteContext context;
						try {
								context = endpoint.getWriteContext(indexWriteBufferFactory,bulkWrite.getTxn(),endpoint.region,endpoint.rce,bulkWrite.getMutations().size());
						} catch (InterruptedException e) {
								SpliceLogUtils.debug(LOG, "write context interrupted %s",region.getRegionNameAsString());
								return new BulkWriteResult(new WriteResult(Code.INTERRUPTED_EXCEPTON));
						} catch (IndexNotSetUpException e) {
								SpliceLogUtils.debug(LOG, "write context index not setup exception %s",region.getRegionNameAsString());
								return new BulkWriteResult(new WriteResult(Code.INDEX_NOT_SETUP_EXCEPTION));
						}

						Object[] bufferArray = bulkWrite.getBuffer();
						int size = bulkWrite.getSize();
						for (int i = 0; i<size; i++) {
								context.sendUpstream((KVPair) bufferArray[i]); //send all writes along the pipeline
						}
						return new BulkWriteResult(context,WriteResult.success());
				}
				finally {
						region.closeRegionOperation();
				}
		}

		private static BulkWriteResult flushAndClose(BulkWriteResult writeResult, BulkWrite bulkWrite,SpliceIndexEndpoint2 endpoint) throws IOException {
				WriteContext context = writeResult.getWriteContext();
				if (context==null)
						return writeResult; // Already Failed
				HRegion region = endpoint.rce.getRegion();
				try {
						region.startRegionOperation();
				}
				catch (NotServingRegionException nsre) {
						SpliceLogUtils.debug(LOG, "hbase not serving region %s",region.getRegionNameAsString());
						return new BulkWriteResult(new WriteResult(Code.NOT_SERVING_REGION,region.getRegionNameAsString()));
				}
				catch (RegionTooBusyException nsre) {
						SpliceLogUtils.debug(LOG, "hbase region too busy %s",region.getRegionNameAsString());
						return new BulkWriteResult(new WriteResult(Code.REGION_TOO_BUSY,region.getRegionNameAsString()));
				}
				catch (InterruptedIOException ioe) {
						SpliceLogUtils.error(LOG, "hbase region interrupted %s",region.getRegionNameAsString());
						return new BulkWriteResult(new WriteResult(Code.INTERRUPTED_EXCEPTON,region.getRegionNameAsString()));
				}
				try {
						Object[] bufferArray = bulkWrite.getBuffer();
						context.flush();
						Map<KVPair,WriteResult> resultMap = context.close();
						BulkWriteResult response = new BulkWriteResult();
						int failed=0;
						int size = bulkWrite.getSize();
						for (int i = 0; i<size; i++) {
								@SuppressWarnings("RedundantCast") WriteResult result = resultMap.get((KVPair)bufferArray[i]);
								if(!result.isSuccess()){
					/*
					if (!result.canRetry()) {
						response.setGlobalStatus(result); // Blow up now...
						return response;
					}
					*/
										failed++;
								}
								response.addResult(i,result);
						}
						if (failed > 0)
								response.setGlobalStatus(WriteResult.partial());
						else
								response.setGlobalStatus(WriteResult.success());
						SpliceLogUtils.trace(LOG,"Returning response %s",response);
						int numSuccessWrites = size-failed;
						endpoint.throughputMeter.mark(numSuccessWrites);
						endpoint.failedMeter.mark(failed);
						return response;
				} finally {
						region.closeRegionOperation();
				}

		}

		@Override
		public byte[] bulkWrites(byte[] bulkWriteBytes) throws IOException {
				try {
						assert bulkWriteBytes!=null;
						BulkWrites bulkWrites = PipelineUtils.fromCompressedBytes(bulkWriteBytes, BulkWrites.class);
						return PipelineUtils.toCompressedBytes(bulkWrite(bulkWrites));
				} finally {
						bulkWriteBytes = null; // Dereference bytes passed.
				}
		}

		private WriteContext getWriteContext(WriteBufferFactory indexWriteBufferFactory, TxnView txn, TransactionalRegion region, RegionCoprocessorEnvironment rce,int writeSize) throws IOException, InterruptedException {
				Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = getContextPair(conglomId);
				return ctxFactoryPair.getFirst().create(indexWriteBufferFactory,txn,region,rce);
		}

		private boolean isDependent() throws IOException {
				Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = getContextPair(conglomId);
				return ctxFactoryPair.getFirst().hasDependentWrite();
		}

		private static Pair<LocalWriteContextFactory, AtomicInteger> getContextPair(long conglomId) {
				Pair<LocalWriteContextFactory, AtomicInteger> ctxFactoryPair = factoryMap.get(conglomId);
				if(ctxFactoryPair==null){
						ctxFactoryPair = Pair.newPair(new LocalWriteContextFactory(conglomId),new AtomicInteger());
						Pair<LocalWriteContextFactory, AtomicInteger> existing = factoryMap.putIfAbsent(conglomId, ctxFactoryPair);
						if(existing!=null){
								ctxFactoryPair = existing;
						}
				}
				return ctxFactoryPair;
		}

		public static LocalWriteContextFactory getContextFactory(long baseConglomId) {
				Pair<LocalWriteContextFactory,AtomicInteger> ctxPair = getContextPair(baseConglomId);
				return ctxPair.getFirst();
		}

		public static void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
				ObjectName coordinatorName = new ObjectName("com.splicemachine.derby.hbase:type=ActiveWriteHandlers");
				mbs.registerMBean(ActiveWriteHandlers.get(),coordinatorName);
		}

		public static class ActiveWriteHandlers implements ActiveWriteHandlersIface {
				private static final ActiveWriteHandlers INSTANCE = new ActiveWriteHandlers();
				private  ActiveWriteHandlers () {}

				public static ActiveWriteHandlers get(){ return INSTANCE; }
				@Override public int getIpcReservedPool() { return ipcReserved; }
				@Override public void setIpcReservedPool(int ipcReservedPool) { ipcReserved = ipcReservedPool; }
				@Override public int getFlushQueueSizeLimit() { return flushQueueSizeBlock; }
				@Override public void setFlushQueueSizeLimit(int flushQueueSizeLimit) { flushQueueSizeBlock = flushQueueSizeLimit; }
				@Override public int getCompactionQueueSizeLimit(){ return compactionQueueSizeBlock; }
				@Override public void setCompactionQueueSizeLimit(int compactionQueueSizeLimit) { compactionQueueSizeBlock = compactionQueueSizeLimit; }
				@Override public int getDependentWriteThreads() { return control.getDependentThreadCount(); }
				@Override public int getIndependentWriteThreads() { return control.getIndependentThreadCount(); }
				@Override public int getDependentWriteCount() {return control.getDependentRowPermitCount(); }
				@Override public int getIndependentWriteCount() { return control.getIndependentRowPermitCount(); }



		}

		@MXBean
		@SuppressWarnings("UnusedDeclaration")
		public interface ActiveWriteHandlersIface {
				public int getIpcReservedPool();
				public void setIpcReservedPool(int rpcReservedPool);
				public int getFlushQueueSizeLimit();
				public void setFlushQueueSizeLimit(int flushQueueSizeLimit);
				public int getCompactionQueueSizeLimit();
				public void setCompactionQueueSizeLimit(int compactionQueueSizeLimit);
				public int getDependentWriteThreads();
				public int getIndependentWriteThreads();
				public int getDependentWriteCount();
				public int getIndependentWriteCount();
		}

}