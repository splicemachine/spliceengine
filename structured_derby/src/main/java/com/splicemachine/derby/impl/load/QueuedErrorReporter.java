package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 3/7/14
 */
public class QueuedErrorReporter implements ImportErrorReporter {
		private final BlockingQueue<ErrorRow> queue;

		private final ExecutorService loggerThreads = Executors.newSingleThreadExecutor();
		/*
		 * The max time to wait on adding a row to the queue before failing and bombing. This
		 * is to prevent runaway loggers from forcing the entire import to halt, and should
		 * be a reasonably long time (a minute or so is reasonable).
		 */
		private final long maxWaitTimeMs;

		/*
		 * Used to indicate that the Logger failed, in which case the reporter must also
		 * fail all reported rows.
		 */
		private volatile boolean failed = false;

		/*
		 * Used to indicate that the importer is closed. Attempts to report an error
		 * after the close will return {@code false}
		 */
		private volatile boolean closed = false;

		public QueuedErrorReporter(int maxQueuedRows,
															 long maxWaitTimeMs,
															 RowErrorLogger rowLogger, PairDecoder pairDecoder) {
				this.maxWaitTimeMs = maxWaitTimeMs;
				this.queue = new ArrayBlockingQueue<ErrorRow>(maxQueuedRows);
				QueueWorker worker = new QueueWorker(rowLogger,pairDecoder);
				loggerThreads.submit(worker);
		}

		@Override
		public void close() throws IOException {
				closed = true;
				loggerThreads.shutdownNow();
				try {
						loggerThreads.awaitTermination(Long.MAX_VALUE,TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
						/*
						 * We were cancelled while waiting for the logger to shut down.
						 * That must mean that the importer was cancelled as well, so it doesn't matter
						 * if the reporter finishes.
						 */
						//ensure interrupt flag is set
						Thread.currentThread().interrupt();
				}
		}

		@Override
		public boolean reportError(KVPair kvPair, WriteResult result) {
				return !closed && offer(new KVPairErrorRow(kvPair, result));
		}


		@Override
		public boolean reportError(String row, WriteResult result) {
				return !closed && offer(new StringErrorRow(row, result));
		}


		/*private helper methods*/
		private boolean offer(ErrorRow errorRow) {
				for(int i=0;i<10;i++){
						if(failed) return false; // the logger failed, so we must fail the import
						try {
								if(queue.offer(errorRow,maxWaitTimeMs/10, TimeUnit.MILLISECONDS))
										return true;
						} catch (InterruptedException e) {
								/*
								 * Interruption indicates that the offering thread was interrupted while
								 * attempting to place elements on the queue. That can only happen if
								 * the Importer was interrupted, which indicates that the import was cancelled
								 * (or failed). Thus, there's no reason to try logging the row, just fail
								 */
								//ensure the interrupt flag is still set
								Thread.currentThread().interrupt();
								return false;
						}
				}
				/*
				 * We've tried for the max wait time (which is hopefully considerable), so something is probably
				 * wrong with the Logger. Time to bail
				 */
				return false;
		}
		private static final Logger WORKER_LOG  = Logger.getLogger(QueueWorker.class);


		private static abstract class ErrorRow{
				private final WriteResult result;

				protected ErrorRow(WriteResult result) {
						this.result = result;
				}

				final void log(QueueWorker worker) throws IOException{
					worker.logger.report(getRow(worker),result);
				}

				protected abstract String getRow(QueueWorker worker) throws IOException;
		}

		private static class KVPairErrorRow extends ErrorRow{
				private final KVPair kvPair;

				protected KVPairErrorRow(KVPair kvPair,WriteResult result) {
						super(result);
						this.kvPair = kvPair;
				}

				@Override
				protected String getRow(QueueWorker worker) throws IOException {
						try{
								ExecRow row = worker.pairDecoder.decode(kvPair);
								if(row==null) return null;
								return row.toString(); //TODO -sf- decide on a standard format for rows?
						}catch(StandardException se){
								throw Exceptions.getIOException(se);
						}
				}
		}

		private static class StringErrorRow extends ErrorRow{
				private final String row;

				protected StringErrorRow(String row,WriteResult result) {
						super(result);
						this.row = row;
				}

				@Override
				protected String getRow(QueueWorker worker) throws IOException {
						//TODO -sf- convert to ExecRow format?
						return row;
				}
		}

		private class QueueWorker implements Runnable{
				private final RowErrorLogger logger;
				private final PairDecoder pairDecoder;

				private QueueWorker(RowErrorLogger logger, PairDecoder pairDecoder) {
						this.logger = logger;
						this.pairDecoder = pairDecoder;
				}

				@Override
				public void run() {
						//read data off the queue in bulk, then feed it forward to the logger. More efficient
						while(!Thread.currentThread().isInterrupted() &&!closed){
								try {
										//get the next entry. Wait only for 1 second so that we can check the closed variable periodically
										ErrorRow poll = queue.poll(1, TimeUnit.SECONDS);
										if(poll==null) continue;
										try {
												poll.log(this);
										} catch (IOException e) {
												WORKER_LOG.error("Unexpected error logging bad row",e);
												/*
												 * We couldn't log the row. Bail on the entire import.
												 */
												failed = true;
												return;
										}
								} catch (InterruptedException e) {
										//we have been shutdown, so move on
										Thread.currentThread().interrupt();
										break;
								}
						}
				}
		}
}
