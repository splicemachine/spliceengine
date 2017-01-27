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

package com.splicemachine.uuid;

import com.splicemachine.primitives.Bytes;

import java.util.UUID;

/**
 * Uses Java standard library classes to generate 128-bit UUIDs
 * @author Scott Fines
 * Date: 2/26/14
 */
public class BasicUUIDGenerator implements UUIDGenerator {
		@Override
		public byte[] nextBytes() {
				UUID next = UUID.randomUUID();
				byte[] data = new byte[16];
				Bytes.toBytes(next.getMostSignificantBits(), data, 0);
				Bytes.toBytes(next.getLeastSignificantBits(), data, 8);
				return data;
		}

		@Override
		public int encodedLength() {
				return 16;
		}

		@Override
		public void next(byte[] data, int offset) {
				UUID next = UUID.randomUUID();
				Bytes.toBytes(next.getMostSignificantBits(), data, offset);
				Bytes.toBytes(next.getLeastSignificantBits(), data, offset + 8);
		}
}
