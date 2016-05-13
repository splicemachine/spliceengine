package com.splicemachine.pipeline.api;


import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;

/**
 * @author Scott Fines
 *         Date: 2/5/14
 */
public interface WriteStats {
		WriteStats NOOP_WRITE_STATS = new WriteStats() {

			@Override
			public long getWrittenCounter() {
				return 0;
			}

			@Override
			public long getRetryCounter() {
				return 0;
			}

			@Override
			public long getThrownErrorsRows() {
				return 0;
			}

			@Override
			public long getRetriedRows() {
				return 0;
			}

			@Override
			public long getPartialRows() {
				return 0;
			}

			@Override
			public long getPartialThrownErrorRows() {
				return 0;
			}

			@Override
			public long getPartialRetriedRows() {
				return 0;
			}

			@Override
			public long getPartialIgnoredRows() {
				return 0;
			}

			@Override
			public long getPartialWrite() {
				return 0;
			}

			@Override
			public long getIgnoredRows() {
				return 0;
			}

			@Override
			public long getCatchThrownRows() {
				return 0;
			}

			@Override
			public long getCatchRetriedRows() {
				return 0;
			}

			@Override
			public long getRegionTooBusy() {
				return 0;
			}
		};

	long getWrittenCounter();
	long getRetryCounter();
	long getThrownErrorsRows();
	long getRetriedRows();
	long getPartialRows();
	long getPartialThrownErrorRows();
	long getPartialRetriedRows();
	long getPartialIgnoredRows();
	long getPartialWrite();
	long getIgnoredRows();
	long getCatchThrownRows();
	long getCatchRetriedRows();
	long getRegionTooBusy();
}
