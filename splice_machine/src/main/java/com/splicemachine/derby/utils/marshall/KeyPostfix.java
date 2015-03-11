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
