package com.splicemachine.derby.impl.load;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts the number of errors that were encountered. If it exceeds
 * some configured threshold, then it will fail all reports.
 *
 * @author Scott Fines
 * Date: 3/7/14
 */
public class ThresholdErrorReporter implements ImportErrorReporter {
		private final AtomicLong errors = new AtomicLong(0l);

		private final long maxErrorCount;
		private final ImportErrorReporter delegate;

		public ThresholdErrorReporter(long maxErrorCount, ImportErrorReporter delegate) {
				this.maxErrorCount = maxErrorCount;
				this.delegate = delegate;
		}

		@Override
		public boolean reportError(KVPair kvPair, WriteResult result) {
				/*
				 * This is written this way to tolerate multiple concurrent access.
				 *
				 * With this structure, up to maxErrorCount rows will be recorded, but the
				 * import will be failed WHEN the number reported equals maxErrorCount.
				 *
				 * Thus, for example, if the maxErrorCount is 10, then 10 row will be recorded,
				 * but the 10th logged error will fail the import.
				 */
				long errorCount = errors.incrementAndGet();
				if(errorCount> maxErrorCount) return false;
				delegate.reportError(kvPair,result);
				return errorCount< maxErrorCount;
		}

		@Override
		public boolean reportError(String row, WriteResult result) {
				/*
				 * This is written this way to tolerate multiple concurrent access.
				 *
				 * With this structure, up to maxErrorCount rows will be recorded, but the
				 * import will be failed WHEN the number reported equals maxErrorCount.
				 *
				 * Thus, for example, if the maxErrorCount is 10, then 10 row will be recorded,
				 * but the 10th logged error will fail the import.
				 */
				long errorCount = errors.incrementAndGet();
				if(errorCount> maxErrorCount) return false;
				delegate.reportError(row,result);
				return errorCount< maxErrorCount;
		}

		@Override public long errorsReported() { return errors.get(); }

		@Override public void close() throws IOException { delegate.close();		 }
}
