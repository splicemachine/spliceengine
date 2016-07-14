/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
