package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.*;
import com.lmax.disruptor.TimeoutException;
import com.splicemachine.metrics.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.BaseHRegionUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * RegionScanner that performs "read-aheads" to reduce the overall latency of a query.
 *
 * In essence, this scanner uses a background thread to perform the actual IO of reading data off of disk, while
 * the primary thread ( the "reader" thread) will wait for the background thread to generate rows.
 *
 * Doing this efficiently involves safe publication between threads. For a low-impact implementation, this
 * class uses the LMAX Disruptor's ring buffer(https://github.com/LMAX-Exchange/disruptor) to publish and consume rows.
 *
 * @author Scott Fines
 * Date: 5/8/14
 */
public abstract class BaseReadAheadRegionScanner<Put extends OperationWithAttributes,Get extends OperationWithAttributes,Data> implements MeasuredRegionScanner<Data>{
		private static final Logger LOGGER = Logger.getLogger(BaseReadAheadRegionScanner.class);
		private static final ExecutorService readAheadService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("tableScan-lookAhead-%d").build());

		private final RingBuffer<List<Data>> ringBuffer;
		private final Sequence consumerSequence;
		private final SequenceBarrier consumerBarrier;

		private final HRegion region;
		private final RegionScanner delegate;
		private final MetricFactory metricFactory;
		/*
		 * Used to determine that an error was thrown. Informs the producer thread to stop processing
		 */
		private volatile boolean closed = false;
		private volatile long availableSequence =-1l;

		private volatile Future<IOStats> readAheadFuture;
		private volatile IOStats ioStats;
		private volatile boolean completed = true;
		private final SDataLib dataLib;

		public BaseReadAheadRegionScanner(HRegion region, 
				int bufferSize,RegionScanner delegate,
				MetricFactory metricFactory,
				SDataLib<Data, Put, Delete, Get, Scan> dataLib){
				this.dataLib = dataLib;
				this.region = region;
				this.delegate = delegate;
				this.metricFactory = metricFactory;
				/*
				 * Note: There are different wait strategies available, depending on what kind of guarantees we want. For
				 * low latency even at the cost of additional CPU, use the BusySpinWaitStrategy. SleepingWaitStrategy imposes
				 * the highest latency, but the lowest CPU cost. YeildingWaitStrategy is somewhere in the middle. For more
				 * information see https://github.com/LMAX-Exchange/disruptor/wiki/Getting-Started
				 */
				int s = 1;
				while(s<bufferSize){
						s<<=1;
				}
				this.ringBuffer = RingBuffer.createSingleProducer(new ListFactory(),s,new BusySpinWaitStrategy());
				this.consumerBarrier = ringBuffer.newBarrier();
				this.consumerSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
				ringBuffer.addGatingSequences(consumerSequence);
		}

		public void start(){
				if(readAheadFuture!=null) return;
				if(LOGGER.isDebugEnabled())
						LOGGER.debug("Starting Read Ahead scanner");
				readAheadFuture = readAheadService.submit(new Reader(delegate,metricFactory, region));
				completed=false;
		}

		@Override
		public Data next() throws IOException {
				throw new UnsupportedOperationException("use nextRaw(list) instead");
		}

		@Override
		public HRegionInfo getRegionInfo() {
				return region.getRegionInfo();
		}

		@Override
		public boolean isFilterDone() {
				throw new UnsupportedOperationException("Cannot ask if filter is done with ReadAheadRegionScanner");
		}

		@Override
		public boolean reseek(byte[] row) throws IOException {
				throw new UnsupportedOperationException("Cannot reseek with ReadAheadRegionScanner");
		}

		@Override
		public long getMvccReadPoint() {
				throw new UnsupportedOperationException("Cannot getMvccReadPoint with ReadAheadRegionScanner");
		}

		public boolean internalNextRaw(List<Data> result, int limit, String metric) throws IOException {
				if(completed||closed) return false;
				long nextSequence = consumerSequence.get()+1L;
				if(getFromBatch(result,nextSequence)) return true;

				if(completed) return false;

				/*
				 * We need to wait for a new batch of sequences
				 */
				try {
						if(LOGGER.isTraceEnabled())
								SpliceLogUtils.trace(LOGGER,"Batch empty at sequence %d, attempting to read new batch",nextSequence);
						availableSequence = consumerBarrier.waitFor(nextSequence);
						return getFromBatch(result, nextSequence);
				} catch (AlertException e) {
						throw new IOException(e);
				} catch (InterruptedException e) {
						throw new IOException(e);
				} catch (TimeoutException e) {
						throw new IOException(e);
				}
		}

		protected boolean getFromBatch(List<Data> result, long nextSequence) {
				if(nextSequence<=availableSequence){
						/*
						 * We are still within the same batch, can read one directly
						 */
						try{
								List<Data> kvs = ringBuffer.get(nextSequence);
								if(kvs.size()<=0) {
										completed = true;
										return false;
								}

								result.addAll(kvs);
								return true;
						}finally{
								consumerSequence.set(nextSequence);
						}
				}
				if(LOGGER.isTraceEnabled())
						SpliceLogUtils.trace(LOGGER,"no more data to read from batch,sequence at %d",nextSequence);
				return false;
		}

		@Override
		public void close() throws IOException {
				closed=true;
				//cancel the worker thread
				if(readAheadFuture!=null){
						readAheadFuture.cancel(true);

						if(metricFactory.isActive()){
								//we want the stats, so forcibly wait for the thread to report them.
								while(ioStats==null)
										LockSupport.parkNanos(1L);
						}
				}
		}

		@Override
		public TimeView getReadTime() {
				if(ioStats==null) return Metrics.noOpTimeView();
				return ioStats.getTime();
		}

		@Override public long getBytesOutput() { return getBytesVisited(); }
		@Override public long getRowsOutput() { return getRowsVisited(); }
		@Override public long getRowsFiltered() { return 0l; }

		@Override
		public long getBytesVisited() {
				if(ioStats==null) return 0l;
				return ioStats.getBytes();
		}

		@Override
		public long getRowsVisited() {
				if(ioStats==null) return 0l;
				return ioStats.getRows();
		}

		/*private helper methods*/
		/********************************************************************************************************************/

		private static final int LOCK_SIZE = (1<<6); //lock for region checking every 64 entries
		private class Reader implements Callable<IOStats> {
				private final RegionScanner delegate;
				private final MetricFactory metricFactory;
				private final HRegion region;
				private final EventTranslatorOneArg<List<Data>,List<Data>> translator;
				
				private Reader(RegionScanner delegate, MetricFactory metricFactory, HRegion region) {
						this.delegate = delegate;
						this.metricFactory = metricFactory;
						this.region = region;						
						this.translator = new EventTranslatorOneArg<List<Data>,List<Data>>() {
							@Override
							public void translateTo(List<Data> event, long sequence, List<Data> arg0) {
								event.clear();
								event.addAll(arg0);
							}
						};
				}

				@Override
				public IOStats call() throws Exception {
						Timer timer = metricFactory.newWallTimer();
						Counter bytesCounter = metricFactory.newCounter();
						int rowCount = 0;

						List<Data> readKvs = Lists.newArrayListWithExpectedSize(2);
						boolean shouldContinue = true;
						int c = 0;
						OUTER: do{
								if(closed) break; //check if we are closed
								region.startRegionOperation();
								MultiVersionConsistencyControl.setThreadReadPoint(delegate.getMvccReadPoint());
								timer.startTiming();
								try{
										if(LOGGER.isTraceEnabled())
												SpliceLogUtils.trace(LOGGER,"Fetching the next batch of rows, current sequence id ~ %d",ringBuffer.getCursor());
										for(int batch=0;batch<LOCK_SIZE && shouldContinue;batch++){
												readKvs.clear();
												shouldContinue = dataLib.regionScannerNextRaw(delegate, readKvs);
												if(readKvs.size()<=0) break OUTER;

												rowCount++;
												if(bytesCounter.isActive()){
														for(Data kv:readKvs){
																bytesCounter.add(dataLib.getLength(kv));
														}
												}

												ringBuffer.publishEvent(translator,readKvs);
												c++;
										}
								}finally{
										timer.tick(c);
										region.closeRegionOperation();
										c=0;
								}
						}while(!Thread.currentThread().isInterrupted() && shouldContinue);
						BaseHRegionUtil.updateReadRequests(region,rowCount);
						//make sure that the timer has stopped
						timer.stopTiming();

						if(LOGGER.isTraceEnabled())
								SpliceLogUtils.trace(LOGGER,
												"Finished reading data from delegate scanner. " +
																"Read %d rows and ended the sequence with %d",rowCount,ringBuffer.getCursor());
						ioStats = new BaseIOStats(timer.getTime(),bytesCounter.getTotal(),timer.getNumEvents());
						if(!closed){
								//need to notify the thread (if it is waiting) that there is no more data to be returned
								//do this by publishing an empty row
								readKvs.clear();
								ringBuffer.publishEvent(translator,readKvs);
						}
						return ioStats;
				}
		}

		private static class ListFactory<Data> implements EventFactory<List<Data>> {
				@Override public List<Data> newInstance() {
						return Lists.newArrayListWithExpectedSize(2);
				}
		}


}
