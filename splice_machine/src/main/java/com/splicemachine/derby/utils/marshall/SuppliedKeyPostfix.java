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

import java.io.IOException;

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

		@Override public void close() throws IOException {  }
}
