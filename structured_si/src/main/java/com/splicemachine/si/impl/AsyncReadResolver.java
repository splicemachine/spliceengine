package com.splicemachine.si.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.ThreadSafe;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 * Date: 7/1/14
 */
@ThreadSafe
public class AsyncReadResolver  {
		private static final Logger LOG = Logger.getLogger(AsyncReadResolver.class);

		private final RingBuffer<ResolveEvent> ringBuffer;
		private final Disruptor<ResolveEvent> disruptor;

		private final ThreadPoolExecutor consumerThreads;
		private volatile boolean stopped;

		public AsyncReadResolver(int maxThreads,int bufferSize) {
				consumerThreads = new ThreadPoolExecutor(maxThreads,maxThreads,
								60, TimeUnit.SECONDS,
								new LinkedBlockingQueue<Runnable>(),
								new ThreadFactoryBuilder().setNameFormat("readResolver-%d").setDaemon(true).build());

				disruptor = new Disruptor<ResolveEvent>(new ResolveEventFactory(),bufferSize,consumerThreads,
								ProducerType.MULTI,
								new YieldingWaitStrategy()); //we want low latency here, but it might cost too much in CPU
				disruptor.handleEventsWith(new ResolveEventHandler());
				ringBuffer = disruptor.getRingBuffer();
		}

		public void start(){
				disruptor.start();
		}

		public void shutdown(){
				stopped=true;
				disruptor.shutdown();
				consumerThreads.shutdownNow();
		}

		public @ThreadSafe ReadResolver getResolver(final HRegion region){
				return new RegionReadResolver(region);
		}

		private static class ResolveEvent{
				HRegion region;
				long txnId;
				long commitTimestamp;
				ByteSlice rowKey;
				boolean rolledBack;
		}

		private static class ResolveEventFactory implements EventFactory<ResolveEvent>{

				@Override
				public ResolveEvent newInstance() {
						return new ResolveEvent();
				}
		}

		private static class ResolveEventHandler implements EventHandler<ResolveEvent>{

				@Override
				public void onEvent(ResolveEvent event, long sequence, boolean endOfBatch) throws Exception {
						if(event.region.isClosing()||event.region.isClosed()) return; //bypass this row, since we can't write it anyway

						if(event.rolledBack){
								Delete delete = new Delete(event.rowKey.getByteCopy(),event.txnId);
								delete.deleteFamily(SpliceConstants.DEFAULT_FAMILY_BYTES); //delete all the columns for our family only
								delete.setWriteToWAL(false); //avoid writing to the WAL for performance
								try{
										event.region.delete(delete,false);
								}catch(IOException ioe){
										LOG.info("Exception encountered when attempting to resolve a row as rolled back",ioe);
								}
						}else{
								Put put = new Put(event.rowKey.getByteCopy());
								put.add(SIConstants.DEFAULT_FAMILY_BYTES,
												SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, event.txnId,Bytes.toBytes(event.commitTimestamp));
								put.setWriteToWAL(false);
								try{
										event.region.put(put,false);
								}catch(IOException ioe){
										LOG.info("Exception encountered when attempting to resolve a row as committed",ioe);
								}
						}
				}
		}

		private class RegionReadResolver implements ReadResolver {
				private final HRegion region;

				public RegionReadResolver(HRegion region) {
						this.region = region;
				}

				@Override
				public void resolveCommitted(ByteSlice rowKey, long txnId, long commitTimestamp) {
						if(stopped) return; //if we aren't running, do nothing
						long sequence = ringBuffer.next();
						try{
								ResolveEvent event = ringBuffer.get(sequence); //get the next entry
								event.region = region;
								event.txnId = txnId;
								event.commitTimestamp = commitTimestamp;
						}finally{
								ringBuffer.publish(sequence);
						}

				}

				@Override
				public void resolveRolledback(ByteSlice rowKey, long txnId) {
						if(stopped) return; //if we aren't running, do nothing
						long sequence = ringBuffer.next();
						try{
								ResolveEvent event = ringBuffer.get(sequence); //get the next entry
								event.region = region;
								event.txnId = txnId;
								event.rolledBack =true;
						}finally{
								ringBuffer.publish(sequence);
						}

				}
		}
}
