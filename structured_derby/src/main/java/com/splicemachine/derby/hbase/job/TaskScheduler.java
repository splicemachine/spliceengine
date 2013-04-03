package com.splicemachine.derby.hbase.job;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface TaskScheduler {

    TaskFuture submit(Task task) throws ExecutionException;


}
