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

    private final SConfiguration config;
    private final Snowflake snowflake;
    private final Connection connection;
    private final SpliceMachineVersion version;

    public EngineSqlEnvironment(Snowflake snowflake,
                                Connection connection,
                                SpliceMachineVersion version,
                                SConfiguration config){
        this.snowflake=snowflake;
        this.connection=connection;
        this.version=version;
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
