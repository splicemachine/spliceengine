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


import com.splicemachine.uuid.UUIDGenerator;

import java.io.IOException;

/**
 * A Prefix which "Salts" the hash with an 8-byte Snowflake-generated
 * UUID.
 *
 * @author Scott Fines
 * Date: 11/15/13
 */
public class SaltedPrefix implements HashPrefix {
		private UUIDGenerator generator;

		public SaltedPrefix(UUIDGenerator generator) {
				this.generator = generator;
		}

		@Override public int getPrefixLength() { return generator.encodedLength(); }

		@Override
		public void encode(byte[] bytes, int offset, byte[] hashBytes) {
				//encode the UUID directly into the bytes
				generator.next(bytes,offset);
		}

		@Override
		public void close() throws IOException {

		}
}
