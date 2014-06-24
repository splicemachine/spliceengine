package com.splicemachine.si.impl.timestamp;

import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class TimestampUtil {

	//
	// Trace
	//
	
	public static void doClientTrace(Logger logger, String message) {
		SpliceLogUtils.trace(logger, message);
	}
	
	public static void doClientTrace(Logger logger, String message, Object obj) {
		SpliceLogUtils.trace(logger, message + " : " + obj);
	}
	
	public static void doServerTrace(Logger logger, String message) {
		SpliceLogUtils.trace(logger, message);
	}
	
	public static void doServerTrace(Logger logger, String message, Object obj) {
		SpliceLogUtils.trace(logger, message + " : " + obj);
	}
	
	//
	// Info
	//
	
	public static void doClientInfo(Logger logger, String message) {
		SpliceLogUtils.info(logger, message);
	}
	
	public static void doClientInfo(Logger logger, String message, Object obj) {
		SpliceLogUtils.info(logger, message + " : " + obj);
	}
	
	public static void doServerInfo(Logger logger, String message) {
		SpliceLogUtils.info(logger, message);
	}
	
	public static void doServerInfo(Logger logger, String message, Object obj) {
		SpliceLogUtils.info(logger, message + " : " + obj);
	}
	
	//
	// Debug
	//
	
	public static void doClientDebug(Logger logger, String message) {
		doDebug(logger, message);
	}
	
	public static void doClientDebug(Logger logger, String message, Object obj) {
		doDebug(logger, message + " : " + obj);
	}
	
	public static void doServerDebug(Logger logger, String message) {
		doDebug(logger, message);
	}
	
	public static void doServerDebug(Logger logger, String message, Object obj) {
		doDebug(logger, message + " : " + obj);
	}
	
	public static void doDebug(Logger logger, String message) {
		SpliceLogUtils.debug(logger, message);
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
	
	public static void doClientError(Logger logger, String message, Throwable t, Object obj) {
		SpliceLogUtils.error(logger, message + " : " + obj, t);		
	}
	
	public static void doClientErrorThrow(Logger logger, String message, Throwable t, Object obj) {
		if (message == null) message = "";
		if (obj == null) obj = "";
		StringBuffer sb = new StringBuffer();
		String fullMsg = sb.append(message).append(" : ").append(obj).toString();
		if (t != null) {
			SpliceLogUtils.logAndThrowRuntime(logger, fullMsg, t);
		} else {
			SpliceLogUtils.error(logger, fullMsg);
			throw new RuntimeException(fullMsg);
		}
	}
	
	public static void doServerError(Logger logger, String message) {
		SpliceLogUtils.error(logger, message);		
	}
	
	public static void doServerError(Logger logger, String message, Throwable t) {
		SpliceLogUtils.error(logger, message, t);		
	}
	
	public static void doServerError(Logger logger, String message, Throwable t, Object obj) {
		SpliceLogUtils.error(logger, message + " : " + obj, t);		
	}
	
}
