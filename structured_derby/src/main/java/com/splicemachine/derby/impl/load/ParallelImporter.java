package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.pipeline.api.CallBufferFactory;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.impl.MergingWriteStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
public class ParallelImporter implements Importer{
    private static final Logger LOG = Logger.getLogger(ParallelImporter.class);
    private final ExecutorService processingPool;
    private final BlockingQueue<String[]> processingQueue;
		private volatile boolean closed;
		private final List<Future<Pair<WriteStats,TimeView>>> futures;
		private volatile Exception error;
		private final MetricFactory metricFactory;
		private List<Pair<WriteStats,TimeView>> stats;

		private Timer processTimer;


		public ParallelImporter(ImportContext importContext,ExecRow template,TxnView txn,
														ImportErrorReporter errorReporter,KVPair.Type importType) {
        this(importContext,
												template,
												txn,
												SpliceConstants.maxImportProcessingThreads,
												SpliceConstants.maxImportReadBufferSize,
												SpliceDriver.driver().getTableWriter(),
								errorReporter,importType
        );
    }

		public ParallelImporter(ImportContext importContext,
														ExecRow template,
														int numProcessingThread,
														int maxImportReadBufferSize,
														TxnView txn,ImportErrorReporter errorReporter,KVPair.Type importType) {
				this(importContext,
								template,
								txn,
								numProcessingThread,
								maxImportReadBufferSize,
								SpliceDriver.driver().getTableWriter(),
								errorReporter, importType);
		}

    public ParallelImporter(ImportContext importCtx,
                            ExecRow template,
                            TxnView txn,
                            int numProcessingThreads,
                            int maxImportReadBufferSize,
                            CallBufferFactory<KVPair> factory,
                            final ImportErrorReporter errorReporter,KVPair.Type importType){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "ThreadingCallBuffer#init called");
				Path filePath = importCtx.getFilePath();
        ThreadFactory processingFactory = new ThreadFactoryBuilder()
                .setNameFormat("import-"+filePath.getName()+"-processor-%d")
                .build();
        	processingPool = Executors.newFixedThreadPool(numProcessingThreads, processingFactory);

				processingQueue = new BoundedConcurrentLinkedQueue<String[]>(maxImportReadBufferSize);

				if(importCtx.shouldRecordStats()){
						metricFactory = Metrics.samplingMetricFactory(SpliceConstants.sampleTimingSize);
				}else
						metricFactory = Metrics.noOpMetricFactory();

        futures = Lists.newArrayList();
        for(int i=0;i<numProcessingThreads;i++){
						SequentialImporter importer = new SequentialImporter(importCtx,template.getClone(),txn, factory,
                    errorReporter,importType) {
								@Override
								public boolean isFailed() {
										return ParallelImporter.this.isFailed();
								}
						};
            futures.add(processingPool.submit(new Processor(processingQueue,importer)));
        }
    }
    
    @Override
    public void process(String[] parsedRow) throws Exception {
				processBatch(parsedRow);
    }

		@Override
		public boolean processBatch(String[]... parsedRows) throws Exception {
				if(parsedRows==null) return false;

				if(processTimer==null)
						processTimer = metricFactory.newTimer();
				processTimer.startTiming();

				int count = 0;
				for(String[] row:parsedRows){
						if(row==null) break;
						boolean shouldContinue;
						do{
	            /*
	             * In the event of a processor failing, it will set the error
	             * field atomically, then stop processing entries off the queue. All
	             * other processor threads will then atomically see the error, and stop
	             * processing themselves. If this happens while we are in the process
	             * of offering entries to the processors, then we must be sure that we
	             * don't spin for forever. To prevent that, we look at the error field
	             * ourselves.
	             */
								if(error!=null)
										throw error;

								shouldContinue = !processingQueue.offer(row,200,TimeUnit.MILLISECONDS);
						}while(shouldContinue);
						count++;
				}
				if(count>0)
						processTimer.tick(count);
				else
						processTimer.stopTiming();
				return count == parsedRows.length;
		}

		@Override
    public void close() throws IOException {
				if(processTimer==null)
						processTimer = metricFactory.newTimer();

				processTimer.startTiming();
        closed = true;
        try{
						stats = Lists.newArrayListWithCapacity(futures.size());
            for(Future<Pair<WriteStats,TimeView>> future:futures){
                try {
                    stats.add(future.get());
                } catch (InterruptedException e) {
										LOG.warn("Interrupted attempting to complete writes",e);
										Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
										throw new IOException(e.getCause());
                }
            }
						if(isFailed())
								throw new IOException(error);
        }finally{
            processingPool.shutdownNow();
						processTimer.stopTiming();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

		public WriteStats getWriteStats(){
				if(stats==null) return WriteStats.NOOP_WRITE_STATS;

				MergingWriteStats view = new MergingWriteStats(metricFactory);
				for(Pair<WriteStats,TimeView> ioStats:stats){
					view.merge(ioStats.getFirst());
				}
				return view;
		}

		@Override
		public TimeView getTotalTime() {
				if(processTimer==null) return Metrics.noOpTimeView();
				return processTimer.getTime();
		}

		void markFailed(Exception e){ this.error = e; }
    boolean isFailed(){ return error!=null; }

    private class Processor implements Callable<Pair<WriteStats,TimeView>>{
        private final BlockingQueue<String[]> queue;
				private final SequentialImporter importer;

				private Processor( BlockingQueue<String[]> queue,SequentialImporter importer){
						this.importer = importer;
						this.queue = queue;

				}

        @Override
        public Pair<WriteStats,TimeView> call() throws Exception {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Processor#call");
            /*
             * We eat off the queue, process, and place them into the write destination until
             * we are out of recrods to process.
             *
             * But how do we determine if we are out of records? We can't just pull off the queue
             * until we don't get anything--we may not get anything because the write portion is faster
             * than the read portion, so our threads are running out of work to do. On the other hand,
             * we can't just take() forever, because we'll never shut down that way.
             *
             * So first we break it into two stages--before close() is called on the source, and after.
             * Before closed is called, we block until new records are retained. But we can't block forever,
             * lest we fail to stop if a closed is called without our ever getting a new record to process. Thus,
             * we take for only a little while before backing off and trying again.
             */
            try{
								String[][] elements = new String[16][];
								int mask = 16-1;
								int position = 0;
                while(!isClosed()&&!isFailed()){
                    String[] next = queue.poll(200,TimeUnit.MILLISECONDS);
                    if(next==null)continue; //try again
					elements[position] = next;
					position = (position+1) & mask;
					if(position==0){
						importer.processBatch(elements);
					}
				}
								//source is closed, poll until the queue is empty
								String[] next;
								while(!isFailed() && (next = queue.poll())!=null){
										elements[position] = next;
										position = (position+1) & mask;
										if(position==0){
												importer.processBatch(elements);
										}
								}
								if(position!=0){
										Arrays.fill(elements,position,elements.length,null);
										importer.processBatch(elements);
								}
						}
            catch(Exception e){
                markFailed(e);
                throw e;
            } finally{
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "Closing Call Buffer");
								importer.close();
            }
						return Pair.newPair(importer.getWriteStats(), importer.getTotalTime());
        }

    }
}
