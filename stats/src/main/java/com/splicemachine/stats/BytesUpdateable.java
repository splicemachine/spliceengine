/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.stats;

import java.nio.ByteBuffer;

/**
 * Represents a Data structure which can be updated incrementally
 * from a fixed set of bytes.
 *
 * @author Scott Fines
 * Date: 3/26/14
 */
public interface BytesUpdateable extends Updateable<ByteBuffer>{

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
