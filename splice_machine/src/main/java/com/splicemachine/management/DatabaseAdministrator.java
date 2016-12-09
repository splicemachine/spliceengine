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

package com.splicemachine.management;

import com.splicemachine.access.api.DatabaseVersion;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/17/16
 */
public interface DatabaseAdministrator{

    Map<String, Collection<Integer>> getJDBCHostPortInfo() throws SQLException;

    void setLoggerLevel(String loggerName, String logLevel) throws SQLException;

    List<String> getLoggerLevel(String loggerName) throws SQLException;

    Set<String> getLoggers() throws SQLException;

    Map<String,DatabaseVersion> getClusterDatabaseVersions() throws SQLException;

    Map<String,Map<String,String>> getDatabaseVersionInfo() throws SQLException;

    void setWritePoolMaxThreadCount(int maxThreadCount) throws SQLException;

    Map<String,Integer> getWritePoolMaxThreadCount() throws SQLException;

    Map<String,String> getGlobalDatabaseProperty(String key) throws SQLException;

    void setGlobalDatabaseProperty(String key, String value) throws SQLException;

    void emptyGlobalStatementCache() throws SQLException;
}
