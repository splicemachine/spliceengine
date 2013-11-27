package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * User: pjt
 * Date: 6/18/13
 */

/**
 * Interface for SpliceOperations that need to sink rows from their children
 * before computing result rows.
 */
public interface SinkingOperation {

    /**
     * Get next ExecRow to sink to an intermediate table as prep for computing result rows
     */
    ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException;

    String getTransactionID();

		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException;

		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException;

		CallBuffer<KVPair> transformWriteBuffer(CallBuffer<KVPair> bufferToTransform) throws StandardException;

		void close() throws IOException,StandardException;

		byte[] getUniqueSequenceId();
}
