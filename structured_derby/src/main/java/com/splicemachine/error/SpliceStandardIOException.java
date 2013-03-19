package com.splicemachine.error;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
/**
 * Standard Exception wrapped into an IOException to handle failures that 
 * HBase should retry.  We wrap it to propagate the 
 * i18N supported Derby generated message. 
 * 
 * @author John Leach
 *
 */
public class SpliceStandardIOException extends IOException {
	private static final long serialVersionUID = 53138821460465941L;
	protected StandardException standardException;
	/**
	 * Instance method setting the standard exception on the ioexception
	 * 
	 * @param standardException
	 */
	public SpliceStandardIOException (StandardException standardException) {
		this.standardException = standardException;
	}
	/**
	 * Instance method setting the message on the IOException and the derby i18n Standard Exception.
	 * 
	 * @param message
	 * @param standardException
	 */
	public SpliceStandardIOException(String message, StandardException standardException) {
		super(message);
		this.standardException = standardException;
	}
	/**
	 * 
	 * Retrieve the 18n exception from Derby.
	 * 
	 * @return StandardException
	 */
	public StandardException getStandardException() {
		return standardException;
	}
	/**
	 * 
	 * @param standardException
	 */
	public void setStandardException(StandardException standardException) {
		this.standardException = standardException;
	}
	
}
