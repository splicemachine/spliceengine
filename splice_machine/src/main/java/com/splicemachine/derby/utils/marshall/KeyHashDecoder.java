package com.splicemachine.derby.utils.marshall;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.Closeable;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public interface KeyHashDecoder extends Closeable {
		void set(byte[] bytes, int hashOffset, int length);
		void decode(ExecRow destination) throws StandardException;
}
