package com.splicemachine.error;

import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;
import com.google.gson.Gson;

public class SpliceStandardLogUtils {
	protected static Gson gson = new Gson();

	public static SpliceDoNotRetryIOException generateSpliceDoNotRetryIOException(Logger logger, String message, Throwable throwable) {
		logger.error(message, throwable);
		if (throwable instanceof StandardException)
			return new SpliceDoNotRetryIOException(generateSpliceStandardExceptionString((StandardException) throwable));
		return new SpliceDoNotRetryIOException(gson.toJson(StandardException.unexpectedUserException(throwable)));
	}

	public static SpliceIOException generateSpliceIOException(Logger logger, String message, Throwable throwable) {
		logger.error(message, throwable);
		if (throwable instanceof StandardException)
			return new SpliceIOException(generateSpliceStandardExceptionString((StandardException) throwable));
		return new SpliceIOException(gson.toJson(StandardException.unexpectedUserException(throwable)));
	}

	public static StandardException logAndReturnStandardException(Logger logger, String message, Exception exception) {
		logger.error(message, exception);
		if (exception instanceof SpliceDoNotRetryIOException || exception instanceof SpliceIOException)
			return (gson.fromJson(exception.getMessage(), SpliceStandardException.class)).generateStandardException();
		return StandardException.unexpectedUserException(exception);
	}		
	
	public static String generateSpliceStandardExceptionString(StandardException standardException) {
		return gson.toJson(new SpliceStandardException(standardException));
	}
	
}