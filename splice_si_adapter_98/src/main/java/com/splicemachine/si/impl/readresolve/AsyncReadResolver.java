package com.splicemachine.si.impl.readresolve;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.RollForward;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Read-Resolver which asynchronously submits regions for execution, discarding
 * any entries which exceed the size of the processing queue.
 *
 * This implementation uses an LMAX disruptor to asynchronously pass Read-resolve events
 * to a background thread, which in turn uses a SynchronousReadResolver to actually perform the resolution.
 *
 * @see com.splicemachine.si.impl.readresolve.SynchronousReadResolver
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
    private final RollForwardStatus status;

    public AsyncReadResolver(int maxThreads,int bufferSize,
                             TxnSupplier txnSupplier,RollForwardStatus status) {
        this.txnSupplier = txnSupplier;
        this.status = status;
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

		public @ThreadSafe ReadResolver getResolver(HRegion region,RollForward rollForward){
				return new RegionReadResolver(region,rollForward);
		}

		private static class ResolveEvent{
				HRegion region;
				long txnId;
				ByteSlice rowKey = new ByteSlice();
        RollForward rollForward;
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
            try{
                if(SynchronousReadResolver.INSTANCE.resolve(event.region,event.rowKey,event.txnId,txnSupplier,status,false)){
                    event.rollForward.recordResolved(event.rowKey,event.txnId);
                }
            }catch(Exception e){
                LOG.info("Error during read resolution",e);
                throw e;
            }
				}
		}

		private class RegionReadResolver implements ReadResolver {
				private final HRegion region;
        private final RollForward rollForward;

				public RegionReadResolver(HRegion region,RollForward rollForward) {
						this.region = region;
            this.rollForward = rollForward;
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
                event.rowKey.set(rowKey.getByteCopy());
                event.rollForward = rollForward;
            }finally{
                ringBuffer.publish(sequence);
            }
        }

		}
}
