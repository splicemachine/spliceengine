package com.splicemachine.derby.metrics;

import com.carrotsearch.hppc.BitSet;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.storage.EntryEncoder;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 1/16/14
 */
public class AsyncTableWritingMetricsReporter implements RuntimeMetricsReporter{
		private static final Logger LOG = Logger.getLogger(AsyncTableWritingMetricsReporter.class);
		private final BlockingQueue<OperationRuntimeStats> statsQueue;
		private final ThreadPoolExecutor writerThreads;
		private final TableName destinationTable;
		private long flushTimeWindow;

		public AsyncTableWritingMetricsReporter(TableName destinationTable,int reporterThreads, long flushTimeWindowSeconds) {
				this.flushTimeWindow = flushTimeWindowSeconds;
				ThreadFactory factory = new ThreadFactoryBuilder()
								.setDaemon(true).setNameFormat("metrics-reporter-%d").build();
				this.writerThreads = new ThreadPoolExecutor(reporterThreads,
								reporterThreads,60l, TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>(),factory);
				this.destinationTable = destinationTable;
				this.statsQueue = new LinkedBlockingDeque<OperationRuntimeStats>();
		}

		@Override
		public void start(){
			for(int i=0;i<writerThreads.getMaximumPoolSize();i++){
					writerThreads.submit(new Worker());
			}
		}

		@Override
		public void shutdown() {
				writerThreads.shutdown();
				try {
						if(!writerThreads.awaitTermination(60l,TimeUnit.SECONDS)){
								writerThreads.shutdownNow();
						}
				} catch (InterruptedException e) {
						writerThreads.shutdownNow();
						//restore the interrupt
						Thread.currentThread().interrupt();
				}
		}

		@Override
		public void reportMetrics(OperationRuntimeStats stats) {
				statsQueue.offer(stats);
		}

		private class Worker implements Runnable{

				@Override
				public void run() {
						int numMetrics = OperationMetric.values().length;
						int numFields = numMetrics+3; //statementId + operationId + taskId
						BitSet setCols = new BitSet();
						setCols.set(0,numFields);
						BitSet scalarFields = new BitSet();
						scalarFields.set(3,numFields);
						BitSet floatFields = new BitSet();
						BitSet doubleFields = new BitSet();

						EntryEncoder entryEncoder = EntryEncoder.create(SpliceDriver.getKryoPool(),
										numFields, setCols, scalarFields, floatFields, doubleFields);
						//flush frequently
						CallBuffer<KVPair> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(destinationTable,null,20);
						HTableInterface hTable = SpliceAccessManager.getHTable(destinationTable);
						try{
							//build a put for the stats object
								MultiFieldEncoder fieldEncoder = entryEncoder.getEntryEncoder();
								MultiFieldEncoder rowEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),3);
								while(!Thread.currentThread().isInterrupted()){
										try {
												OperationRuntimeStats next = statsQueue.poll(flushTimeWindow, TimeUnit.SECONDS);
												if(next==null){
														//no data has come through for a while, so flush what you do have
														writeBuffer.flushBuffer();
														continue;
												}

												fieldEncoder.reset();
												next.encode(fieldEncoder);
												byte[] rowData = fieldEncoder.build();

												//encode the row key
												rowEncoder.reset();
												byte[] rowKey = rowEncoder.encodeNext(next.getStatementId())
																.encodeNext(next.getOperationId())
																.encodeNext(next.getTaskId()).build();

												writeBuffer.add(new KVPair(rowKey,rowData, KVPair.Type.INSERT));
										} catch (InterruptedException e) {
												//we've been cancelled, time to shut down
												Thread.currentThread().interrupt();
												return;
										} catch (Exception e) {
												LOG.info("Unexpected error encountered writing data to Statistics, some fields may be lost",e);
												//otherwise continue reading and writing
										}
								}
								try{
										writeBuffer.flushBuffer();
								} catch (Exception e) {
										LOG.info("Unexpected error encountered writing data to Statistics, some fields may be lost", e);
								}finally{
										try {
												writeBuffer.close();
										} catch (Exception e) {
												LOG.info("Unexpected error encountered closing write buffer",e);
										}
								}
						}  finally{
								Closeables.closeQuietly(hTable);
						}
				}
		}


}
