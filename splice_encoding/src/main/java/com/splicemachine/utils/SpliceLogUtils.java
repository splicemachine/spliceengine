/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.utils;

import java.util.Arrays;

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

    public static void debug(Logger logger, String message,Throwable t){
        if(logger.isDebugEnabled())logger.debug(message,t);
    }

    public static void debug(Logger logger, String message,Throwable t,Object... args){
        if(logger.isDebugEnabled())logger.debug(String.format(message,args),t);
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
    
    public static String getStackTrace() {
    	return Arrays.toString(Thread.currentThread().getStackTrace()).replaceAll(", ", "\n\t");
    }
}
