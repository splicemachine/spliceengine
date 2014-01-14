package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class FixedDataHash implements DataHash {
		private final byte[] bytes;

		public FixedDataHash(byte[] bytes) {
				this.bytes = bytes;
		}

		@Override public void setRow(ExecRow rowToEncode) {  }
		@Override public KeyHashDecoder getDecoder() { return null; }

		@Override
		public byte[] encode() throws StandardException, IOException {
				return bytes;
		}
}
