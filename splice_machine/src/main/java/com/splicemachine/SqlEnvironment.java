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

package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.Manager;
import com.splicemachine.uuid.Snowflake;

import java.sql.Connection;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public interface SqlEnvironment{
    Snowflake getUUIDGenerator();

    Connection getInternalConnection();

    DatabaseVersion getVersion();

    SConfiguration getConfiguration();

    Manager getManager();

    PartitionLoadWatcher getLoadWatcher();

    DataSetProcessorFactory getProcessorFactory();

    PropertyManager getPropertyManager();

    void initialize(SConfiguration config,Snowflake snowflake,Connection internalConnection,DatabaseVersion spliceVersion);

    SqlExceptionFactory exceptionFactory();

    DatabaseAdministrator databaseAdministrator();

    OlapClient getOlapClient();

    void refreshEnterpriseFeatures();
}
