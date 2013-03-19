package com.splicemachine.error;

import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

public class SpliceStandardDoNotRetryIOException extends DoNotRetryIOException {
	private static final long serialVersionUID = -3028529963138495858L;
	protected StandardException standardException;
	
	public SpliceStandardDoNotRetryIOException(StandardException standardException) {
		super();
		this.standardException = standardException;
	}
	public SpliceStandardDoNotRetryIOException(String message, StandardException standardException) {
		super(message);
		this.standardException = standardException;
	}

	public StandardException getStandardException() {
		return standardException;
	}
	public void setStandardException(StandardException standardException) {
		this.standardException = standardException;
	}
}
