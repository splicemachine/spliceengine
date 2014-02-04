package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.stats.*;
import com.splicemachine.stats.util.Folders;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
    private final ImportContext importContext;
    private final List<Future<IOStats>> futures;
    private volatile Exception error;
		private final MetricFactory metricFactory;
		private ArrayList<IOStats> stats;


		public ParallelImporter(ImportContext importContext,ExecRow template,String txnId) {
        this(importContext,
                template,
                txnId,
                SpliceConstants.maxImportProcessingThreads,
                SpliceConstants.maxImportReadBufferSize,
                SpliceDriver.driver().getTableWriter());
    }

		public ParallelImporter(ImportContext importContext,
														ExecRow template,
														int numProcessingThread,
														int maxImportReadBufferSize,
														String txnId) {
				this(importContext,
								template,
								txnId,
								numProcessingThread,
								maxImportReadBufferSize,
								SpliceDriver.driver().getTableWriter());
		}

    public ParallelImporter(ImportContext importCtx,
                            ExecRow template,
                            String txnId,
                            int numProcessingThreads,
                            int maxImportReadBufferSize,
                            CallBufferFactory<KVPair> factory){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "ThreadingCallBuffer#init called");
        this.importContext = importCtx;
        Path filePath = importContext.getFilePath();
        ThreadFactory processingFactory = new ThreadFactoryBuilder()
                .setNameFormat("import-"+filePath.getName()+"-processor-%d")
                .build();
        processingPool = Executors.newFixedThreadPool(numProcessingThreads, processingFactory);

        processingQueue = new BoundedConcurrentLinkedQueue<String[]>(maxImportReadBufferSize);

				if(importCtx.shouldRecordStats()){
						metricFactory = Metrics.basicMetricFactory();
				}else
						metricFactory = Metrics.noOpMetricFactory();

        String tableName = importContext.getTableName();
        futures = Lists.newArrayList();
				/*
				 * We need a write configuration that will not attempt to retry any writes if the task
				 * itself has failed.
				 */
				Writer.WriteConfiguration writeConfiguration = new ForwardingWriteConfiguration(factory.defaultWriteConfiguration()){
						@Override
						public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
								if(LOG.isTraceEnabled())
										SpliceLogUtils.trace(LOG,"Received a global write error: ",t);
								if(isFailed())
										return Writer.WriteResponse.IGNORE; //we're already dead, so stop writing
								else
										return super.globalError(t);
						}

						@Override
						public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
								if(isFailed())
										return Writer.WriteResponse.IGNORE; //we're already dead, so stop writing
								else
										return super.partialFailure(result, request);
						}
				};
        for(int i=0;i<numProcessingThreads;i++){
						RecordingCallBuffer<KVPair> writeDest = factory.writeBuffer(tableName.getBytes(),txnId,writeConfiguration);
            futures.add(processingPool.submit(new Processor(template.getClone(), processingQueue, writeDest)));
        }
    }
    
    @Override
    public void process(String[] parsedRow) throws Exception {
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
            shouldContinue = !processingQueue.offer(parsedRow,200,TimeUnit.MILLISECONDS);
        }while(shouldContinue);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        try{
						stats = Lists.newArrayListWithCapacity(futures.size());
            for(Future<IOStats> future:futures){
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
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

		public IOStats getStats(){
				if(stats==null) return Metrics.noOpIOStats();

				MultiTimeView timeView = new MultiTimeView(
								Folders.maxLongFolder(), Folders.sumFolder(), Folders.sumFolder(),
								Folders.minLongFolder(), Folders.maxLongFolder());
				MultiStatsView view = new MultiStatsView(timeView);
				for(IOStats ioStats:stats){
					view.merge(ioStats);
				}
				return view;
		}

    void markFailed(Exception e){ this.error = e; }
    boolean isFailed(){ return error!=null; }

    public PairEncoder newEntryEncoder(ExecRow row) {
        int[] pkCols = importContext.getPrimaryKeys();

				KeyEncoder encoder;
				if(pkCols!=null&& pkCols.length>0){
					encoder = new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(pkCols,null),NoOpPostfix.INSTANCE);
				}else
					encoder = new KeyEncoder(new SaltedPrefix(getRandomGenerator()),NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);

				DataHash rowHash = new EntryDataHash(IntArrays.count(row.nColumns()),null);

				return new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);
    }

		protected Snowflake.Generator getRandomGenerator(){
				return SpliceDriver.driver().getUUIDGenerator().newGenerator(100);
		}

    private class Processor implements Callable<IOStats>{
        private final BlockingQueue<String[]> queue;
        private final RecordingCallBuffer<KVPair> writeDestination;
        private final PairEncoder entryEncoder;

        private RowParser importProcessor;

				private Timer writeTimer;

        private Processor(ExecRow row,
                          BlockingQueue<String[]> queue,
                          RecordingCallBuffer<KVPair> writeDestination ){
            this.queue = queue;
            this.writeDestination = writeDestination;
            this.entryEncoder = newEntryEncoder(row);
            this.importProcessor = new RowParser(row, importContext);
						this.writeTimer = metricFactory.newTimer();
        }

        @Override
        public IOStats call() throws Exception {
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
                while(!isClosed()&&!isFailed()){
                    String[] elements = queue.poll(200,TimeUnit.MILLISECONDS);
                    if(elements==null)continue; //try again
                    doImportRow(elements);
								}
								//source is closed, poll until the queue is empty
								String[] next;
								while(!isFailed() && (next = queue.poll())!=null){
										doImportRow(next);
								}
						}
            catch(Exception e){
                markFailed(e);
                throw e;
            } finally{
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "Closing Call Buffer");
                writeDestination.close(); //ensure your writes happen
            }
						long bytesWritten = writeDestination.getTotalBytesAdded();
						return new BaseIOStats(writeTimer.getTime(),bytesWritten,writeTimer.getNumEvents());
        }

        protected void doImportRow(String[] line) throws Exception {
						writeTimer.startTiming();
						ExecRow newRow = importProcessor.process(line, importContext.getColumnInformation());

						writeDestination.add(entryEncoder.encode(newRow));
						writeTimer.tick(1);
        }
    }
}
