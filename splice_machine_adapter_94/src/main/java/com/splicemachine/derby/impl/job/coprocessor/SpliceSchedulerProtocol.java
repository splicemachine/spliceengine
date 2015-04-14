package com.splicemachine.derby.impl.job.coprocessor;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface SpliceSchedulerProtocol extends CoprocessorProtocol {

    /**
     *
     * @param taskStart
     * @param taskStop
     * @param taskData an encoded (using Kryo) region task to execute
     * @param allowSplits
     * @return an encoded list of TaskFutureContext (using Kryo)
     * @throws IOException
     */
    byte[] submit(byte[] taskStart,byte[] taskStop,byte[] taskData,boolean allowSplits) throws IOException;
}
