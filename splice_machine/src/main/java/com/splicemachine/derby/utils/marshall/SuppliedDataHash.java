/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

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
