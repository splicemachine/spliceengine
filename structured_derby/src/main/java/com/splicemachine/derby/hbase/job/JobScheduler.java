package com.splicemachine.derby.hbase.job;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public interface JobScheduler<J extends Job> {

    JobFuture submit(J job) throws ExecutionException;
}
