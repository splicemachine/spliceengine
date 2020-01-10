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

package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServiceDiscovery;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.derby.iapi.sql.execute.OperationManager;
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

    OperationManager getOperationManager();

    ServiceDiscovery serviceDiscovery();
}
