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
 *
 */

package com.splicemachine.db.client.cluster;


import sun.util.logging.LoggingProxy;
import sun.util.logging.LoggingSupport;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * @author Scott Fines
 *         Date: 10/17/16
 */
public class ThreadFormatter extends Formatter{
    // format string for printing the log record
    private static final String format = getFormat();


    private final Date dat = new Date();

    @Override
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public synchronized String format(LogRecord record){
        dat.setTime(record.getMillis());
        String source;
        if (record.getSourceClassName() != null) {
            source = record.getSourceClassName();
            if (record.getSourceMethodName() != null) {
                source += " " + record.getSourceMethodName();
            }
        } else {
            source = record.getLoggerName();
        }
        String threadName = getThreadName(record.getThreadID());
        String message = formatMessage(record);
        String throwable = "";
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println();
            record.getThrown().printStackTrace(pw);
            pw.close();
            throwable = sw.toString();
        }
        return String.format(format,
                dat,
                source,
                record.getLoggerName(),
                record.getLevel().getLocalizedName(),
                message,
                throwable,
                threadName);
    }

    @SuppressWarnings("UnusedParameters")
    private String getThreadName(int threadID){
        return Thread.currentThread().getName();
    }



    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static String getFormat(){
        String format =AccessController.doPrivileged(new PrivilegedAction<String>(){
            @Override
            public String run(){
                String property=System.getProperty("java.util.logging.SimpleFormatter.format");
                if(property!=null) return property;
                return System.getProperty("com.splicemachine.db.client.cluster.ThreadFormatter.format");
            }
        });

        if(format!=null) return format;

        LoggingProxy proxy = (LoggingProxy)AccessController.doPrivileged(new PrivilegedAction() {
            public LoggingProxy run() {
                try {
                    Class proxyClass = Class.forName("java.util.logging.LoggingProxyImpl", true,null);
                    Field instanceField = proxyClass.getDeclaredField("INSTANCE");
                    instanceField.setAccessible(true);
                    return (LoggingProxy)instanceField.get(null);
                } catch (ClassNotFoundException var3) {
                    return null;
                } catch (NoSuchFieldException | IllegalAccessException var4) {
                    throw new AssertionError(var4);
                }
            }
        });

        format = proxy.getProperty("java.util.logging.SimpleFormatter.format");
        if(format==null)
            format = proxy.getProperty("com.splicemachine.db.client.cluster.ThreadFormatter.format");

        if(format!=null){
            try{
                String.format(format,new Date(),"","","","","","");
            }catch(IllegalArgumentException ie){
                format = LoggingSupport.getSimpleFormat(); //get the default format
            }
        }else format = LoggingSupport.getSimpleFormat();// get the default format

        return format;
    }
}
