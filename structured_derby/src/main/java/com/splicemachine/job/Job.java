package com.splicemachine.job;

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
}
