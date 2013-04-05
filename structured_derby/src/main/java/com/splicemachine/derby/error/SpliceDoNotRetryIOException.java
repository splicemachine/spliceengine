package com.splicemachine.derby.error;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceDoNotRetryIOException extends DoNotRetryIOException { 
	private static final long serialVersionUID = -733330421228198468L;
	private static final Logger LOG = Logger.getLogger(SpliceDoNotRetryIOException.class);

	public SpliceDoNotRetryIOException() {
		super();
		SpliceLogUtils.trace(LOG, "instance");
	}

	public SpliceDoNotRetryIOException(String message, Throwable cause) {
		super(message, cause);
		SpliceLogUtils.trace(LOG, "instance with messsage %s and throwable %s",message,cause);
	}

	public SpliceDoNotRetryIOException(String message) {
		super(message);
		SpliceLogUtils.trace(LOG, "instance with messsage %s",message);
	}

	@Override
	public String getMessage() {
		LOG.trace("getMessage");
		return super.getMessage();
	}
}
