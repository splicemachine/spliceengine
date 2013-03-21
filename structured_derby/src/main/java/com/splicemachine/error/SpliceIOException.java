package com.splicemachine.error;

import java.io.IOException;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceIOException extends IOException {
	private static final long serialVersionUID = 2756073849787812684L;
	private static final Logger LOG = Logger.getLogger(SpliceIOException.class);
    protected String passedMessage;

	public SpliceIOException() {
		super();
		SpliceLogUtils.trace(LOG, "instance");
	}

	public SpliceIOException(String message, Throwable cause) {
		super(message, cause);
		this.passedMessage = message;
		SpliceLogUtils.trace(LOG, "instance with passedMessage %s and messsage %s and throwable %s",passedMessage,message,cause);
	}

	public SpliceIOException(String message) {
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
