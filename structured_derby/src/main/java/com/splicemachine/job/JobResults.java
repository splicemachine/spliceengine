package com.splicemachine.job;

import org.apache.derby.iapi.error.StandardException;

/**
 * @author Scott Fines
 * Date: 11/20/13
 */
public interface JobResults {

		JobStats getJobStats();

		void cleanup() throws StandardException;
}
