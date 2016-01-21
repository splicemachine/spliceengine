package com.splicemachine.storage;

import java.util.Collection;

/**
 * Wrapper interface for Logging Management for an individual server.
 *
 * @author Scott Fines
 *         Date: 1/21/16
 */
public interface ServerLogging{

    String getLoggerLevel(String loggerName);

    void setLoggerLevel(String loggerName, String logLevel);

    Collection<String> getAllLoggerNames();
}
