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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * Represents a DataHash which does not encode or decode anything.
 *
 * @author Scott Fines
 * Date: 11/15/13
 */
public class NoOpDataHash implements DataHash,KeyHashDecoder{
		public static final DataHash INSTANCE = new NoOpDataHash();

		private NoOpDataHash(){}

		@SuppressWarnings("unchecked")
		public static <T> DataHash<T> instance(){
				return (DataHash<T>)INSTANCE;
		}
		@Override
		public void setRow(Object rowToEncode) {
				//no-op
		}

		@Override
		public byte[] encode() throws StandardException {
				return new byte[0];
		}

		@Override
		public KeyHashDecoder getDecoder() {
				return this;
		}

		@Override
		public void set(byte[] bytes, int hashOffset,int length) {
				//no-op
		}

		@Override
		public void decode(ExecRow destination) throws StandardException {
				//no-op
		}

		@Override
		public void close() throws IOException {
				//no-op
		}
}
