package com.splicemachine.si.api;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Indicates that an attempt was made to perform a write
 * with a read-only transaction
 *
 * @author Scott Fines
 * Date: 6/24/14
 */
public class ReadOnlyModificationException extends DoNotRetryIOException{
		public ReadOnlyModificationException() {
		}

		public ReadOnlyModificationException(String message) {
				super(message);
		}

		public ReadOnlyModificationException(String message, Throwable cause) {
				super(message, cause);
		}
}

