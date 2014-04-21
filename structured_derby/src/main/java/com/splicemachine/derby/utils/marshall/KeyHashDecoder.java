package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.Closeable;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public interface KeyHashDecoder extends Closeable {

		void set(byte[] bytes, int hashOffset, int length);

		void decode(ExecRow destination) throws StandardException;
}
