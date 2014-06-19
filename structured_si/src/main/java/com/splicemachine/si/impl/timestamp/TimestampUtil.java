package com.splicemachine.si.impl.timestamp;

import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class TimestampUtil {

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
	
	public static void doClientError(Logger logger, String message, Object obj) {
		SpliceLogUtils.error(logger, message + " : " + obj);		
	}
	
	public static void doClientError(Logger logger, String message, Object obj, Throwable t) {
		SpliceLogUtils.error(logger, message + " : " + obj);		
	}
	
	public static void doServerError(Logger logger, String message) {
		SpliceLogUtils.error(logger, message);		
	}
	
	public static void doServerError(Logger logger, String message, Object obj) {
		SpliceLogUtils.error(logger, message + " : " + obj);		
	}
	
}
