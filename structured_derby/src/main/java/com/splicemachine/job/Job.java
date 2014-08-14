package com.splicemachine.job;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

/**
 * Represents a Job to be executed.
 *
 * @author Scott Fines
 * Created on: 4/3/13
 */
public interface Job {

    /**
     * @return a unique id for this job
     */
    String getJobId();

		/**
		 * @return the parent transaction for the job, or {@code null} if the job is not transactional.
		 */
		TxnView getTxn();

    <T extends Task> Pair<T,Pair<byte[],byte[]>> resubmitTask(T originalTask,byte[] taskStartKey,byte[] taskEndKey) throws IOException;
}
