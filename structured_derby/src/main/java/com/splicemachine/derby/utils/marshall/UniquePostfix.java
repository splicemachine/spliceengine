package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.iapi.error.StandardException;

/**
 * Postfix which uses UUIDs to ensure that the postfix is unique.
 *
 * @author Scott Fines
 * Date: 11/18/13
 */
public class UniquePostfix implements KeyPostfix{
		private final byte[] baseBytes;
		private final Snowflake.Generator generator;

		public UniquePostfix(byte[] baseBytes) {
				this(baseBytes,SpliceDriver.driver().getUUIDGenerator().newGenerator(100));
		}

		public UniquePostfix(byte[] baseBytes, Snowflake.Generator generator) {
				this.generator = generator;
				this.baseBytes = baseBytes;
		}

		@Override
		public int getPostfixLength(byte[] hashBytes) throws StandardException {
				return baseBytes.length + Snowflake.UUID_BYTE_SIZE;
		}

		@Override
		public void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes) {
				byte[] uuidBytes = generator.nextBytes();
				System.arraycopy(uuidBytes,0,keyBytes,postfixPosition,uuidBytes.length);
				System.arraycopy(baseBytes,0,keyBytes,postfixPosition+uuidBytes.length,baseBytes.length);
		}
}
