package com.splicemachine.si.impl.timestamp;

import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class TimestampUtil {

	// This extra logging layer no longer does anything beyond delegation
	// to SpliceLogUtils, but leave here for now anyway.

	//
	// Trace
	//
	
	public static void doClientTrace(Logger logger, String message) {
		SpliceLogUtils.trace(logger, message);
	}
	
	public static void doClientTrace(Logger logger, String message, Object... args) {
		SpliceLogUtils.trace(logger, message, args);
	}
	
	public static void doServerTrace(Logger logger, String message) {
		SpliceLogUtils.trace(logger, message);
	}
	
	public static void doServerTrace(Logger logger, String message, Object... args) {
		SpliceLogUtils.trace(logger, message, args);
	}
	
	//
	// Info
	//
	
	public static void doClientInfo(Logger logger, String message) {
		SpliceLogUtils.info(logger, message);
	}
	
	public static void doClientInfo(Logger logger, String message, Object... args) {
		SpliceLogUtils.info(logger, message, args);
	}
	
	public static void doServerInfo(Logger logger, String message) {
		SpliceLogUtils.info(logger, message);
	}
	
	public static void doServerInfo(Logger logger, String message, Object... args) {
		SpliceLogUtils.info(logger, message, args);
	}
	
	//
	// Debug
	//
	
	public static void doClientDebug(Logger logger, String message) {
		SpliceLogUtils.debug(logger, message);
	}

	public static void doClientDebug(Logger logger, String message, Object... args) {
		SpliceLogUtils.debug(logger, message, args);
	}

	public static void doServerDebug(Logger logger, String message) {
		SpliceLogUtils.debug(logger, message);
	}
	
	public static void doServerDebug(Logger logger, String message, Object... args) {
		SpliceLogUtils.debug(logger, message, args);
	}
	
	//
	// Error
	//
	
	public static void doClientError(Logger logger, String message) {
		SpliceLogUtils.error(logger, message);		
	}
	
	public static void doClientError(Logger logger, String message, Throwable t) {
		SpliceLogUtils.error(logger, message, t);		
	}
	
	public static void doClientError(Logger logger, String message, Throwable t, Object... args) {
		SpliceLogUtils.error(logger, message, t, args);		
	}

	public static void doClientErrorThrow(Logger logger, String message, Throwable t, Object... args) throws TimestampIOException {
        if (message == null) message = "";
        TimestampIOException t1 = t != null ? new TimestampIOException(message, t) : new TimestampIOException(message);
        SpliceLogUtils.logAndThrow(logger, String.format(message, args), t1);
	}
	
	public static void doServerError(Logger logger, String message) {
		SpliceLogUtils.error(logger, message);		
	}
	
	public static void doServerError(Logger logger, String message, Throwable t) {
		SpliceLogUtils.error(logger, message, t);		
	}
	
	public static void doServerError(Logger logger, String message, Object... args) {
		SpliceLogUtils.error(logger, message, args);		
	}
	
}
