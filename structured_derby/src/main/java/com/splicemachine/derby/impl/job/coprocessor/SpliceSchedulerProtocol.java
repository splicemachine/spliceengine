package com.splicemachine.derby.impl.job.coprocessor;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface SpliceSchedulerProtocol extends CoprocessorProtocol {

    public TaskFutureContext submit(RegionTask task) throws IOException;
}
