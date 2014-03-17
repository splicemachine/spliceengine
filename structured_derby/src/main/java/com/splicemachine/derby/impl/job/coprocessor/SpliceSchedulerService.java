package com.splicemachine.derby.impl.job.coprocessor;

import java.io.IOException;

import com.google.protobuf.Service;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface SpliceSchedulerService extends CoprocessorService, Service {

    public TaskFutureContext submit(byte[] taskStart,byte[] taskStop,RegionTask task) throws IOException;
}
