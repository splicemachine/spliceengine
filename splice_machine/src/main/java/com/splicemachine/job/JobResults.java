package com.splicemachine.job;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * @author Scott Fines
 * Date: 11/20/13
 */
public interface JobResults {

		JobStats getJobStats();

		void cleanup() throws StandardException;
}
