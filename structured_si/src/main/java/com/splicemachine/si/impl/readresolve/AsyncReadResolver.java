package com.splicemachine.si.impl.readresolve;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.ThreadSafe;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

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
    private static final Logger LOG = Logger.getLogger(AsyncReadResolver.class);
    private final RingBuffer<ResolveEvent> ringBuffer;
		private final Disruptor<ResolveEvent> disruptor;

		private final ThreadPoolExecutor consumerThreads;
		private volatile boolean stopped;
    private final TxnSupplier txnSupplier;

		public AsyncReadResolver(int maxThreads,int bufferSize,
                             TxnSupplier txnSupplier) {
        this.txnSupplier = txnSupplier;
				consumerThreads = new ThreadPoolExecutor(maxThreads,maxThreads,
								60, TimeUnit.SECONDS,
								new LinkedBlockingQueue<Runnable>(),
								new ThreadFactoryBuilder().setNameFormat("readResolver-%d").setDaemon(true).build());

				int bSize =1;
				while(bSize<bufferSize)
						bSize<<=1;
				disruptor = new Disruptor<ResolveEvent>(new ResolveEventFactory(),bSize,consumerThreads,
								ProducerType.MULTI,
								new BlockingWaitStrategy()); //we want low latency here, but it might cost too much in CPU
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
				ByteSlice rowKey = new ByteSlice();
		}

		private static class ResolveEventFactory implements EventFactory<ResolveEvent>{

				@Override
				public ResolveEvent newInstance() {
						return new ResolveEvent();
				}
		}

		private class ResolveEventHandler implements EventHandler<ResolveEvent>{

				@Override
				public void onEvent(ResolveEvent event, long sequence, boolean endOfBatch) throws Exception {
            SynchronousReadResolver.INSTANCE.resolve(event.region,event.rowKey,event.txnId,txnSupplier);
				}
		}

		private class RegionReadResolver implements ReadResolver {
				private final HRegion region;

				public RegionReadResolver(HRegion region) {
						this.region = region;
				}

        @Override
        public void resolve(ByteSlice rowKey, long txnId) {
            if(stopped) return; //we aren't running, so do nothing
            long sequence;
            try {
                sequence = ringBuffer.tryNext();
            } catch (InsufficientCapacityException e) {
                if(LOG.isTraceEnabled())
                    LOG.trace("Unable to submit for read resolution");
                return;
            }

            try{
                ResolveEvent event = ringBuffer.get(sequence);
                event.region = region;
                event.txnId = txnId;
                event.rowKey = rowKey;
            }finally{
                ringBuffer.publish(sequence);
            }
        }

		}
}
