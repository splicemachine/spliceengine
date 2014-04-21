package com.splicemachine.derby.utils.marshall;

import com.splicemachine.utils.UUIDGenerator;

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
