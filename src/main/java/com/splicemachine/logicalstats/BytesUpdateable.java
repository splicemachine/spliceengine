package com.splicemachine.logicalstats;

import java.nio.ByteBuffer;

/**
 * Represents a Datastructure which can be updated incrementally
 * from a fixed set of bytes.
 *
 * @author Scott Fines
 * Date: 3/26/14
 */
public interface BytesUpdateable extends Updateable<byte[]>{

		/**
		 * Update the structure using the bytes contained in {@code bytes}, beginning
		 * at {@code offset} and using {@code length} bytes.
		 *
		 * @param bytes the bytes to use
		 * @param offset the offset to read from
		 * @param length the number of bytes to update with
		 */
		void update(byte[] bytes, int offset, int length);

		void update(byte[] bytes, int offset,int length, long count);

		/**
		 * Update the structure using all remaining bytes in the buffer.
		 *
		 * @param bytes the buffer to use
		 */
		void update(ByteBuffer bytes);

		void update(ByteBuffer bytes, long count);
}
