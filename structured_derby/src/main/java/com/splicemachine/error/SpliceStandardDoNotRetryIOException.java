package com.splicemachine.error;

import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
/**
 * Standard Exception wrapped into a DoNotRetryIOException to handle failures that 
 * HBase should immediately abort versus retrying.  We wrap it to propagate the 
 * i18N supported Derby generated message. 
 * 
 * @author John Leach
 *
 */
public class SpliceStandardDoNotRetryIOException extends DoNotRetryIOException {
	private static final long serialVersionUID = -3028529963138495858L;
	protected StandardException standardException;
	/**
	 * Instance method to set the standard exception
	 * 
	 * @param standardException
	 */
	public SpliceStandardDoNotRetryIOException(StandardException standardException) {
		super();
		this.standardException = standardException;
	}
	/**
	 * Instance method to place message on DoNotRetryIOException and to set the standard exception
	 * 
	 * @param message
	 * @param standardException
	 */
	public SpliceStandardDoNotRetryIOException(String message, StandardException standardException) {
		super(message);
		this.standardException = standardException;
	}
	/**
	 * Retrieve the derby i18n message.
	 * 
	 * @return StandardException
	 */
	public StandardException getStandardException() {
		return standardException;
	}
	/**
	 * Set the Derby i18n Standard Exception
	 * @param standardException
	 */
	public void setStandardException(StandardException standardException) {
		this.standardException = standardException;
	}
}
