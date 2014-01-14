package com.splicemachine.job;

import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 11/20/13
 */
public class SimpleJobResults implements JobResults{
		private final JobStats stats;
		private final JobFuture originalFuture;

		public SimpleJobResults(JobStats stats, JobFuture originalFuture) {
				this.stats = stats;
				this.originalFuture = originalFuture;
		}

		@Override
		public JobStats getJobStats() {
				return stats;
		}

		@Override
		public void cleanup() throws StandardException {
				if(originalFuture!=null){
						try {
								originalFuture.cleanup();
						} catch (ExecutionException e) {
								throw Exceptions.parseException(e);
						}
				}
		}
}
