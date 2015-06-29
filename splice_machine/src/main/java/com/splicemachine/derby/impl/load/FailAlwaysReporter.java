package com.splicemachine.derby.impl.load;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.impl.WriteResult;

import java.io.IOException;

/**
 * ErrorReporter that will always fail the row the first time it is seen.
 * @author Scott Fines
 * Date: 3/7/14
 */
public class FailAlwaysReporter implements ImportErrorReporter{
		public static final ImportErrorReporter INSTANCE = new FailAlwaysReporter();
		private FailAlwaysReporter(){}

		@Override public boolean reportError(KVPair kvPair, WriteResult result, boolean cancel) { return false; }

		@Override public boolean reportError(String row, WriteResult result) { return false; }

		@Override public long errorsReported() { return 0; }

		@Override public void close() throws IOException {  }

        @Override public void setWriteBuffer(RecordingCallBuffer<KVPair> writeBuffer){};
}
