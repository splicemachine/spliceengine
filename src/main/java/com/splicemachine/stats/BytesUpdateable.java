package com.splicemachine.stats;

import java.nio.ByteBuffer;

/**
 * Represents a Data structure which can be updated incrementally
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

		/**
		 * Update the structure using the bytes contained in {@code bytes}, beginning
		 * at {@code offset} and using {@code length} bytes.
		 *
		 * @param bytes the bytes to use
		 * @param offset the offset to read from
		 * @param length the number of bytes to update with
		 * @param count the number of times it occurred in the data stream
		 */
		void update(byte[] bytes, int offset,int length, long count);

		/**
		 * Update the structure using all remaining bytes in the buffer.
		 *
		 * @param bytes the buffer to use
		 */
		void update(ByteBuffer bytes);

		/**
		 * Update the structure using all remaining bytes in the buffer, with
		 * {@code count} number of occurrences.
		 *
		 * @param bytes the buffer to use
		 * @param count the number of time that byte sequence occurs in the data stream
		 */
		void update(ByteBuffer bytes, long count);
}
