package com.splicemachine.derby.impl.load;

/**
 * @author Scott Fines
 *         Date: 3/7/14
 */

import com.splicemachine.hbase.writer.WriteResult;

import java.io.Closeable;
import java.io.IOException;

/**
 * Mechanism for reporting a row (using the ExecRow interface) as erroneous.
 * In general, an ImportErrorReporter would convert a KVPair back into an ExecRow,
 * and then report it to this logger.
 *
 * Unlike ImportErrorReporters, RowLoggers are <em>not</em> required to be
 * thread-safe. It is assumed that the ImportErrorReporter will properly synchronize the logger.
 *
 * Typical implementations would be to write the row to a file.
 */
public interface RowErrorLogger extends Closeable {

		/**
		 * Log the row and error
		 * @param row the row to log
		 * @param result the error to log
		 * @throws IOException if something goes wrong during the report
		 */
		public void report(String row, WriteResult result) throws IOException;
}

