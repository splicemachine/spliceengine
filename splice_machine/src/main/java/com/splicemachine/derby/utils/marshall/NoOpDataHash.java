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
