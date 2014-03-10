package com.splicemachine.derby.impl.load;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.utils.ThreadSafe;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.Closeable;

/**
 * Report irrecoverable errors (such as UniqueConstraint or Write-Write conflicts)
 * that were encountered during import. Useful for things like logging a BAD records
 * file, etc.
 *
 * @author Scott Fines
 * Date: 3/7/14
 */
@ThreadSafe
public interface ImportErrorReporter extends Closeable{

		/**
		 * Report an erroneous row. If the reporter decides that
		 * this report would exceed some implementation-specific threshold (e.g.
		 * too many errors in a single file, etc.) then it may return {@code false},
		 * which will tell the importer that it must fail.
		 *
		 * @param kvPair the row to be reported
		 * @param result the nature of the error
		 * @return true if the import can proceed, or {@code false} if
		 * the import should stop.
		 */
		public boolean reportError(KVPair kvPair,WriteResult result);

		/**
		 * Report an erroneous row. If the reporter decides that
		 * this report would exceed some implementation-specific threshold (e.g.
		 * too many errors in a single file, etc.) then it may return {@code false},
		 * which will tell the importer that it must fail.
		 *
		 * @param row the row to be reported
		 * @param result the nature of the error
		 * @return true if the import can proceed, or {@code false} if
		 * the import should stop.
		 */
		public boolean reportError(String row,WriteResult result);

		long errorsReported();

}
