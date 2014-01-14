package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * @author Scott Fines
 * Date: 11/18/13
 */
public class NoOpKeyHashDecoder implements KeyHashDecoder {
		public static final NoOpKeyHashDecoder INSTANCE = new NoOpKeyHashDecoder();

		private NoOpKeyHashDecoder() { }
		@Override public void set(byte[] bytes, int hashOffset, int length) { }
		@Override public void decode(ExecRow destination) throws StandardException { }
}
