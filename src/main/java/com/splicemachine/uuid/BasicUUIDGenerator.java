package com.splicemachine.uuid;

import com.splicemachine.utils.Bytes;
import com.splicemachine.uuid.UUIDGenerator;

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
