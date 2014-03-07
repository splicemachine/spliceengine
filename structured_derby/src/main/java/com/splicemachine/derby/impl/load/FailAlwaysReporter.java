package com.splicemachine.derby.impl.load;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;

import java.io.IOException;

/**
 * ErrorReporter that will always fail the row the first time it is seen.
 * @author Scott Fines
 * Date: 3/7/14
 */
public class FailAlwaysReporter implements ImportErrorReporter{
		public static final ImportErrorReporter INSTANCE = new FailAlwaysReporter();
		private FailAlwaysReporter(){}

		@Override public boolean reportError(KVPair kvPair, WriteResult result) { return false; }

		@Override public boolean reportError(String row, WriteResult result) { return false; }

		@Override public void close() throws IOException {  }
}
