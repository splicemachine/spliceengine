package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * @author Scott Fines
 *         Date: 3/20/14
 */
public class SpliceStartingException extends DoNotRetryIOException {

		public SpliceStartingException() { }
		public SpliceStartingException(String message) { super(message); }
		public SpliceStartingException(String message, Throwable cause) { super(message, cause); }
		public SpliceStartingException(Throwable cause) { super(cause); }
}
