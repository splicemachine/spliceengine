package com.splicemachine.utils;

import org.apache.log4j.Logger;

public class SpliceLogUtils {
	
	private SpliceLogUtils(){}
	
	public static void trace(Logger logger,String message){
		if(logger.isTraceEnabled())logger.trace(message);
	}
	
	public static void trace(Logger logger, String messageFormat,Object...args){
		if(logger.isTraceEnabled())logger.trace(String.format(messageFormat,args));
	}
	public static void debug(Logger logger, String messageFormat,Object...args){
		if(logger.isDebugEnabled())logger.debug(String.format(messageFormat,args));
	}

	public static void info(Logger logger, String messageFormat,Object...args){
		if(logger.isInfoEnabled())logger.info(String.format(messageFormat,args));
	}
	
	
	public static void debug(Logger logger, String message){
		if(logger.isDebugEnabled())logger.debug(message);
	}

	public static void info(Logger logger, String message){
		if(logger.isInfoEnabled())logger.info(message);
	}
	
	
	public static void error(Logger logger, Throwable error){
		logger.error(error);
	}
	
	public static void error(Logger logger, String message, Throwable error){
		logger.error(message,error);
	}

    public static void error(Logger logger, String message, Object...args){
        logger.error(String.format(message,args));
    }
	
	public static <T extends Throwable> void logAndThrow(Logger logger, String message,T t) throws T{
		logger.error(message,t);
		throw t;
	}
	
	public static <T extends Throwable> void logAndThrow(Logger logger, T t) throws T{
		logger.error(t);
		throw t;
	}
	
	public static void logAndThrowRuntime(Logger logger, String message,Throwable t){
		logger.error(message,t);
		throw new RuntimeException(t);
	}
	
	public static void logAndThrowRuntime(Logger logger, Throwable t){
		logger.error(t.getMessage(),t);
		throw new RuntimeException(t);
	}

	public static void warn(Logger log, String messagePattern,Object...args) {
		log.warn(String.format(messagePattern,args));
	}

    public static void warn(Logger log, String message,Throwable error) {
        log.warn(message,error);
    }
}
