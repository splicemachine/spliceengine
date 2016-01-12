package com.splicemachine.derby.lifecycle;

import com.splicemachine.SqlEnvironment;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.impl.store.access.FileResourceFactory;
import com.splicemachine.derby.utils.Sequencer;
import com.splicemachine.tools.version.SpliceMachineVersion;
import com.splicemachine.uuid.Snowflake;

import java.sql.Connection;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public abstract class EngineSqlEnvironment implements SqlEnvironment{

    private SConfiguration config;
    private Snowflake snowflake;
    private Connection connection;
    private SpliceMachineVersion version;

    public EngineSqlEnvironment(){
    }

    @Override
    public void initialize(SConfiguration config,Snowflake snowflake,Connection internalConnection,SpliceMachineVersion spliceVersion){
        this.snowflake = snowflake;
        this.connection = internalConnection;
        this.version = spliceVersion;
        this.config = config;
    }

    @Override
    public Snowflake getUUIDGenerator(){
        return snowflake;
    }

    @Override
    public Connection getInternalConnection(){
        return connection;
    }

    @Override
    public SpliceMachineVersion getVersion(){
        return version;
    }

    @Override
    public SConfiguration getConfiguration(){
        return config;
    }
}
