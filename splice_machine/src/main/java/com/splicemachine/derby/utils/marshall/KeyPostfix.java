/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
		int getPostfixLength(byte[] hashBytes) throws StandardException;

		/**
		 * Encode the postfix into the specified byte[], starting at {@code postfixPosition}.
		 *
		 * @param keyBytes the bytes to encode the postfix into (properly sized according
		 *                 to the specified lengths)
		 * @param postfixPosition the position to begin the encoding.
		 */
		void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes);
}
