/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import splice.com.google.common.collect.Sets;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.derby.utils.DatabasePropertyManagementImpl;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.utils.logging.LogManager;
import com.splicemachine.utils.logging.Logging;

import java.sql.SQLException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 2/17/16
 */
public class DirectDatabaseAdministrator implements DatabaseAdministrator{
    private final Logging logging = new LogManager();

    @Override
    public void setLoggerLevel(String loggerName,String logLevel) throws SQLException{
        logging.setLoggerLevel(loggerName,logLevel);
    }

    @Override
    public List<String> getLoggerLevel(String loggerName) throws SQLException{
        return Collections.singletonList(logging.getLoggerLevel(loggerName));
    }

    @Override
    public Set<String> getLoggers() throws SQLException{
        return Sets.newHashSet(logging.getLoggerNames());
    }

    @Override
    public Map<String, DatabaseVersion> getClusterDatabaseVersions() throws SQLException{
        return Collections.singletonMap("mem",EngineDriver.driver().getVersion());
    }

    @Override
    public Map<String,Map<String,String>> getDatabaseVersionInfo() throws SQLException{
        DatabaseVersion databaseVersion = EngineDriver.driver().getVersion();
        Map<String,String> attrs = new HashMap<>();
        attrs.put("release", databaseVersion.getRelease());
        attrs.put("implementationVersion", databaseVersion.getImplementationVersion());
        attrs.put("buildTime", databaseVersion.getBuildTime());
        attrs.put("url", databaseVersion.getURL());
        return Collections.singletonMap("mem",attrs);
    }

    @Override
    public void setWritePoolMaxThreadCount(int maxThreadCount) throws SQLException{
        PipelineDriver.driver().writeCoordinator().setMaxAsyncThreads(maxThreadCount);
    }

    @Override
    public Map<String, Integer> getWritePoolMaxThreadCount() throws SQLException{
        return Collections.singletonMap("mem",PipelineDriver.driver().writeCoordinator().getMaxAsyncThreads());
    }

    @Override
    public String getDatabaseProperty(String key) throws SQLException{
        return DatabasePropertyManagementImpl.instance().getDatabaseProperty(key);
    }

    @Override
    public void setGlobalDatabaseProperty(String key,String value) throws SQLException{
        DatabasePropertyManagementImpl.instance().setDatabaseProperty(key,value);
    }

    @Override
    public void emptyGlobalStatementCache() throws SQLException{
        //TODO -sf- no-op for now --eventually may need to implement
    }
}
