package com.splicemachine.derby.impl.hbase.job.coprocessor;

import com.splicemachine.derby.impl.hbase.job.OperationJob;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface SpliceSchedulerProtocol extends CoprocessorProtocol {

    public TaskFutureContext submit(OperationJob job) throws IOException;
}
