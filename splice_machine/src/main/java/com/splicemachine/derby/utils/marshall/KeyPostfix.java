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

package com.splicemachine.derby.utils.marshall;

import com.splicemachine.db.iapi.error.StandardException;

import java.io.Closeable;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public interface KeyPostfix extends Closeable {

		/**
		 * @param hash the hash for the row to encode.
		 *
		 * @return the length of the postfix, in bytes
		 */
		public int getPostfixLength(byte[] hashBytes) throws StandardException;

		/**
		 * Encode the postfix into the specified byte[], starting at {@code postfixPosition}.
		 *
		 * @param keyBytes the bytes to encode the postfix into (properly sized according
		 *                 to the specified lengths)
		 * @param postfixPosition the position to begin the encoding.
		 */
		public void encodeInto(byte[] keyBytes, int postfixPosition,byte[] hashBytes);
}
