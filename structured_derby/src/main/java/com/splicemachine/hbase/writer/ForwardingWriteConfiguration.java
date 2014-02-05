package com.splicemachine.hbase.writer;

import com.splicemachine.stats.MetricFactory;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 1/30/14
 */
public  class ForwardingWriteConfiguration implements Writer.WriteConfiguration{
		private final Writer.WriteConfiguration delegate;

		protected ForwardingWriteConfiguration(Writer.WriteConfiguration delegate) {
				this.delegate = delegate;
		}

		@Override
		public int getMaximumRetries() {
				return delegate.getMaximumRetries();
		}

		@Override
		public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
				return delegate.globalError(t);
		}

		@Override
		public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
				return delegate.partialFailure(result, request);
		}

		@Override
		public long getPause() {
				return delegate.getPause();
		}

		@Override
		public void writeComplete(long timeTakenMs, long numRecordsWritten) {
				delegate.writeComplete(timeTakenMs, numRecordsWritten);
		}

		@Override
		public MetricFactory getMetricFactory() {
				return delegate.getMetricFactory();
		}
}
