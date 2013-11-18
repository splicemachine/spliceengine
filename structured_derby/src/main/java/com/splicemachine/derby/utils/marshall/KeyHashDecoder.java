package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public interface KeyHashDecoder {

		void set(byte[] bytes, int hashOffset, int length);

		void decode(ExecRow destination) throws StandardException;
}
