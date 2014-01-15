package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class KeyDecoder {
		private final KeyHashDecoder hashDecoder;
		private final int prefixOffset;

		public KeyDecoder(KeyHashDecoder hashDecoder, int prefixOffset) {
				this.hashDecoder = hashDecoder;
				this.prefixOffset = prefixOffset;
		}

		public void decode(byte[] data, int offset, int length,ExecRow destination) throws StandardException {
				hashDecoder.set(data,offset+prefixOffset,length-prefixOffset);
				hashDecoder.decode(destination);
		}

		public int getPrefixOffset() {
				return prefixOffset;
		}

		@Override
		public String toString() {
			return String.format("KeyDecoder { hashDecoder=%s, prefixOffset=%d",hashDecoder,prefixOffset);
		}
		
		
}
