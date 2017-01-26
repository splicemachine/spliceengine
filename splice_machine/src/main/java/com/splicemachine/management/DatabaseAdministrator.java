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

    Map<String,Map<String,String>> getDatabaseVersionInfo() throws SQLException;

    void setWritePoolMaxThreadCount(int maxThreadCount) throws SQLException;

    Map<String,Integer> getWritePoolMaxThreadCount() throws SQLException;

    Map<String,String> getGlobalDatabaseProperty(String key) throws SQLException;

    void setGlobalDatabaseProperty(String key, String value) throws SQLException;

    void emptyGlobalStatementCache() throws SQLException;
}
