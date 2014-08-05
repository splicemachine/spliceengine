package com.splicemachine.utils;

/**
 * Abstraction for generating UUIDs.
 *
 * @author Scott Fines
 * Date: 2/26/14
 */
public interface UUIDGenerator {

		byte[] nextBytes();

		int encodedLength();

		void next(byte[] data, int offset);
}
