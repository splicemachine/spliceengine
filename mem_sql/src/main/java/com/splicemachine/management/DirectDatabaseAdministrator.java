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

import com.splicemachine.si.api.txn.TxnRegistry;
import com.splicemachine.si.api.txn.TxnRegistryWatcher;
import com.splicemachine.si.impl.driver.SIDriver;
import org.spark_project.guava.collect.Sets;
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

    @Override public void close() throws Exception{ } //nothing to close

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
    public Map<String, String> getGlobalDatabaseProperty(String key) throws SQLException{
        return Collections.singletonMap("mem",DatabasePropertyManagementImpl.instance().getDatabaseProperty(key));
    }

    @Override
    public void setGlobalDatabaseProperty(String key,String value) throws SQLException{
        DatabasePropertyManagementImpl.instance().setDatabaseProperty(key,value);
    }

    @Override
    public void emptyGlobalStatementCache() throws SQLException{
        //TODO -sf- no-op for now --eventually may need to implement
    }

    @Override
    public TxnRegistry.TxnRegistryView getGlobalTransactionRegistry() throws SQLException{
        return SIDriver.driver().getTxnRegistry().asView();
    }

    @Override
    public TxnRegistryWatcher getGlobalTransactionRegistryWatcher() throws SQLException{
        return SIDriver.driver().getTxnRegistry().watcher();
    }
}
