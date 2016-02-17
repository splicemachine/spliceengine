package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.management.DatabaseAdministrator;
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

    BackupManager getBackupManager();

    PartitionLoadWatcher getLoadWatcher();

    DataSetProcessorFactory getProcessorFactory();

    PropertyManager getPropertyManager();

    void initialize(SConfiguration config,Snowflake snowflake,Connection internalConnection,DatabaseVersion spliceVersion);

    SqlExceptionFactory exceptionFactory();

    DatabaseAdministrator databaseAdministrator();
}
