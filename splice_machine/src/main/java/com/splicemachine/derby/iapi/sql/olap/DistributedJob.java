package com.splicemachine.derby.iapi.sql.olap;

import com.splicemachine.concurrent.Clock;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Representation of a Job which can be executed by a distributed server.
 *
 * @author Scott Fines
 *         Date: 4/1/16
 */
public interface DistributedJob extends Serializable{

    Callable<Void> toCallable(OlapStatus jobStatus,
                              Clock clock,
                              long clientTimeoutCheckIntervalMs);

    String getUniqueName();

}
