package com.splicemachine.si.impl.readresolve;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.ThreadSafe;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Read-Resolver which asynchronously submits regions for execution, discarding
 * @author Scott Fines
 * Date: 7/1/14
 */
@ThreadSafe
public class AsyncReadResolver  {
		private final RingBuffer<ResolveEvent> ringBuffer;
		private final Disruptor<ResolveEvent> disruptor;

		private final ThreadPoolExecutor consumerThreads;
		private volatile boolean stopped;

		public AsyncReadResolver(int maxThreads,int bufferSize) {
				consumerThreads = new ThreadPoolExecutor(maxThreads,maxThreads,
								60, TimeUnit.SECONDS,
								new LinkedBlockingQueue<Runnable>(),
								new ThreadFactoryBuilder().setNameFormat("readResolver-%d").setDaemon(true).build());

				int bSize =1;
				while(bSize<bufferSize)
						bSize<<=1;
				disruptor = new Disruptor<ResolveEvent>(new ResolveEventFactory(),bSize,consumerThreads,
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
						if(event.rolledBack)
								SynchronousReadResolver.INSTANCE.resolveRolledback(event.region,event.rowKey,event.txnId);
						else
								SynchronousReadResolver.INSTANCE.resolveCommitted(event.region,event.rowKey,event.txnId,event.commitTimestamp);
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
								setCommit(txnId, commitTimestamp, sequence);
						}finally{
								ringBuffer.publish(sequence);
						}

				}

				@Override
				public void resolveRolledback(ByteSlice rowKey, long txnId) {
						if(stopped) return; //if we aren't running, do nothing
						long sequence = ringBuffer.next();
						try{
								setRollback(txnId, sequence);
						}finally{
								ringBuffer.publish(sequence);
						}
				}

				private void setCommit(long txnId, long commitTimestamp, long sequence) {
						ResolveEvent event = ringBuffer.get(sequence); //get the next entry
						event.region = region;
						event.txnId = txnId;
						event.commitTimestamp = commitTimestamp;
				}

				private ResolveEvent setRollback(long txnId, long sequence) {
						ResolveEvent event = ringBuffer.get(sequence); //get the next entry
						event.region = region;
						event.txnId = txnId;
						event.rolledBack =true;
						return event;
				}
		}
}
