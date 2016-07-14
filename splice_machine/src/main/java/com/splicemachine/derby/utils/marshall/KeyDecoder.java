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

import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class KeyDecoder implements Closeable{
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


		private static KeyDecoder NO_OP_DECODER = new KeyDecoder(NoOpDataHash.INSTANCE.getDecoder(),0);

		public static KeyDecoder noOpDecoder() {
				return NO_OP_DECODER;
		}

		public static KeyDecoder bareDecoder(int[] keyColumnsMap, DescriptorSerializer[] serializers) {
				return new KeyDecoder(BareKeyHash.decoder(keyColumnsMap,null,serializers),0);
		}

		@Override
		public void close() throws IOException {
				hashDecoder.close();
		}
}
