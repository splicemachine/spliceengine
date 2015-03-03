package com.splicemachine.derby.utils.marshall;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.Closeable;
import java.io.IOException;

/**
 * Represents the "Hash" of the row key.
 *
 * This is usually the place where actual values are
 * @author Scott Fines
 * Date: 11/15/13
 */
public interface DataHash<T> extends Closeable {

		void setRow(T rowToEncode);

		/**
		 * @return the byte encoding for the hash;
		 */
		byte[] encode() throws StandardException, IOException;

		KeyHashDecoder getDecoder();
}
