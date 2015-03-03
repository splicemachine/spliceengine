package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * Interface for SpliceOperations that need to sink rows from their children before computing result rows.
 */
public interface SinkingOperation extends SpliceOperation {

    /**
     * Get next ExecRow to sink to an intermediate table as prep for computing result rows
     */
    ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException;

    RecordingCallBuffer<KVPair> transformWriteBuffer(RecordingCallBuffer<KVPair> bufferToTransform) throws StandardException;

    byte[] getUniqueSequenceId();

}
