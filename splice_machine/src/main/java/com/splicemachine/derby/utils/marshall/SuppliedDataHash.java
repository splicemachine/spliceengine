/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
