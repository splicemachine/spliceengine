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
				return errors.incrementAndGet() < maxErrorCount && delegate.reportError(kvPair, result);
		}

		@Override
		public boolean reportError(String row, WriteResult result) {
				return errors.incrementAndGet() < maxErrorCount && delegate.reportError(row, result);
		}

		@Override public long errorsReported() { return errors.get(); }

		@Override public void close() throws IOException { delegate.close();		 }
}
