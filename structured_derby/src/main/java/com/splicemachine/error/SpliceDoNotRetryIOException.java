package com.splicemachine.error;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceDoNotRetryIOException extends DoNotRetryIOException { 
	private static final long serialVersionUID = -733330421228198468L;
	private static final Logger LOG = Logger.getLogger(SpliceDoNotRetryIOException.class);
    protected String passedMessage;

	public SpliceDoNotRetryIOException() {
		super();
		SpliceLogUtils.trace(LOG, "instance");
	}

	public SpliceDoNotRetryIOException(String message, Throwable cause) {
		super(message, cause);
		this.passedMessage = message;
		SpliceLogUtils.trace(LOG, "instance with passedMessage %s and messsage %s and throwable %s",passedMessage,message,cause);
	}

	public SpliceDoNotRetryIOException(String message) {
		super(message);
		this.passedMessage = message;
		SpliceLogUtils.trace(LOG, "instance with passedMessage %s and messsage %s",passedMessage,message);
	}

	@Override
	public String getMessage() {
		LOG.trace("getMessage");
		return passedMessage;
	}
}
