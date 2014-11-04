package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.utils.StandardSupplier;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * DataHash where the bytes are provided by an external
 * Supplier with no reference to the actual ExecRow.
 *
 * @author Scott Fines
 * Date: 11/18/13
 */
public class SuppliedDataHash implements DataHash<ExecRow>{
		private final StandardSupplier<byte[]> supplier;

		public SuppliedDataHash(StandardSupplier<byte[]> supplier) {
				this.supplier = supplier;
		}

		@Override public void setRow(ExecRow rowToEncode) { }

		@Override
		public byte[] encode() throws StandardException, IOException {
				return supplier.get();
		}

		@Override
		public KeyHashDecoder getDecoder() {
				//override to provide custom behavior
				return NoOpKeyHashDecoder.INSTANCE;
		}

		@Override
		public void close() throws IOException {

		}
}
