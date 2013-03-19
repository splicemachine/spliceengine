package com.splicemachine.error;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;

public class SpliceStandardIOException extends IOException {
	private static final long serialVersionUID = 53138821460465941L;
	protected StandardException standardException;

	public SpliceStandardIOException (StandardException standardException) {
		this.standardException = standardException;
	}
	
	public SpliceStandardIOException(String message, StandardException standardException) {
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
