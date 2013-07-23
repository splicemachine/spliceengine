package com.splicemachine.job;

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

    <T extends Task> Pair<T,Pair<byte[],byte[]>> resubmitTask(T originalTask,byte[] taskStartKey,byte[] taskEndKey) throws IOException;
}
