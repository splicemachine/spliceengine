/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
