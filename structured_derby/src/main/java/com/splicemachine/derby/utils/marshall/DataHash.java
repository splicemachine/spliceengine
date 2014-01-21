package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * Represents the "Hash" of the row key.
 *
 * This is usually the place where actual values are
 * @author Scott Fines
 * Date: 11/15/13
 */
public interface DataHash<T> {

		void setRow(T rowToEncode);

		/**
		 * @return the byte encoding for the hash;
		 */
		byte[] encode() throws StandardException, IOException;

		KeyHashDecoder getDecoder();
}
