package com.splicemachine.derby.utils.marshall;

import com.splicemachine.utils.Snowflake;

/**
 * A Prefix which "Salts" the hash with an 8-byte Snowflake-generated
 * UUID.
 *
 * @author Scott Fines
 * Date: 11/15/13
 */
public class SaltedPrefix implements HashPrefix {
		private Snowflake.Generator generator;

		public SaltedPrefix(Snowflake.Generator generator) {
				this.generator = generator;
		}

		@Override
		public int getPrefixLength() {
				return 8; //the salt is always an 8-byte generated UUID
		}

		@Override
		public void encode(byte[] bytes, int offset, byte[] hashBytes) {
				//encode the UUID directly into the bytes
				long uid = generator.next();
				for(int i=7;i>0;i--){
						bytes[offset+i] = (byte)uid;
						uid >>>=8;
				}
				bytes[offset] = (byte)uid;
		}
}
