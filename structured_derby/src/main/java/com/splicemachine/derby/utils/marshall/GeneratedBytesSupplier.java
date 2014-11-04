package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.utils.StandardSupplier;
import org.apache.derby.iapi.error.StandardException;

/**
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class GeneratedBytesSupplier implements StandardSupplier<byte[]>{
		private final byte[] baseBytes;
		private final int offset;
		private final StandardSupplier<byte[]> changingBytesSupplier;

		public GeneratedBytesSupplier(byte[] baseBytes, int offset, StandardSupplier<byte[]> changingBytesSupplier) {
				this.baseBytes = baseBytes;
				this.offset = offset;
				this.changingBytesSupplier = changingBytesSupplier;
		}

		@Override
		public byte[] get() throws StandardException {
				byte[] changedBytes = changingBytesSupplier.get();
				System.arraycopy(changedBytes,0,baseBytes,offset,changedBytes.length);
				return changedBytes;
		}
}
