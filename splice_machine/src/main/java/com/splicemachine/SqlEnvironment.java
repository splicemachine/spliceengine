package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.impl.store.access.FileResourceFactory;
import com.splicemachine.derby.utils.Sequencer;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.tools.version.SpliceMachineVersion;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.UUIDGenerator;

import java.sql.Connection;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public interface SqlEnvironment{
    Snowflake getUUIDGenerator();

    Connection getInternalConnection();

    SpliceMachineVersion getVersion();

    SConfiguration getConfiguration();

    Sequencer getConglomerateSequencer();

    FileResourceFactory getFileResourceFactory();

    StorageFactory getStorageFactory();

    BackupManager getBackupManager();

    PartitionLoadWatcher getLoadWatcher();

    DataSetProcessorFactory getProcessorFactory();

    PropertyManager getPropertyManager();

    void initialize(Snowflake snowflake,Connection internalConnection,SpliceMachineVersion spliceVersion);
}
