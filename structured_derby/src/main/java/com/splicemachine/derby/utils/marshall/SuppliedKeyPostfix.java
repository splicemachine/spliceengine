package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.utils.StandardSupplier;
import org.apache.derby.iapi.error.StandardException;

/**
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class SuppliedKeyPostfix implements KeyPostfix {
		private final StandardSupplier<byte[]> supplier;

		private byte[] bytesToEncode;
		public SuppliedKeyPostfix(StandardSupplier<byte[]> supplier) {
				this.supplier = supplier;
		}

		@Override
		public int getPostfixLength(byte[] hashBytes) throws StandardException {
				bytesToEncode = supplier.get();
				return bytesToEncode.length;
		}

		@Override
		public void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes) {
			System.arraycopy(bytesToEncode,0,keyBytes,postfixPosition,bytesToEncode.length);
		}
}
