package com.splicemachine.management;

import com.splicemachine.access.api.DatabaseVersion;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/17/16
 */
public interface DatabaseAdministrator{

    void setLoggerLevel(String loggerName, String logLevel) throws SQLException;

    List<String> getLoggerLevel(String loggerName) throws SQLException;

    Set<String> getLoggers() throws SQLException;

    Map<String,DatabaseVersion> getClusterDatabaseVersions() throws SQLException;

    void setWritePoolMaxThreadCount(int maxThreadCount) throws SQLException;

    Map<String,Integer> getWritePoolMaxThreadCount() throws SQLException;

    Map<String,String> getGlobalDatabaseProperty(String key) throws SQLException;

    void setGlobalDatabaseProperty(String key, String value) throws SQLException;

    void emptyGlobalStatementCache() throws SQLException;
}
