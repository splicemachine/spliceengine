package com.splicemachine.olap;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public interface OlapJobRegistry{
    OlapJobStatus register(String uniqueJobName);

    OlapJobStatus getStatus(String jobId);

    void clear(String jobId);

    long tickTime();
}
