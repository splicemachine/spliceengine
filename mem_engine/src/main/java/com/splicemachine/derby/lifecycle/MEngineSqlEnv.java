package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.impl.sql.ControlOnlyDataSetProcessorFactory;
import com.splicemachine.derby.impl.sql.DirectPartitionLoadWatcher;
import com.splicemachine.derby.impl.sql.DirectPropertyManager;
import com.splicemachine.derby.impl.sql.NoOpBackupManager;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.uuid.Snowflake;

import java.sql.Connection;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MEngineSqlEnv extends EngineSqlEnvironment{

    private PropertyManager propertyManager;
    private PartitionLoadWatcher partitionLoadWatcher;
    private DataSetProcessorFactory dspFactory;

    @Override
    public void initialize(SConfiguration config,
                           Snowflake snowflake,
                           Connection internalConnection,
                           DatabaseVersion spliceVersion){
        super.initialize(config,snowflake,internalConnection,spliceVersion);
        this.propertyManager = new DirectPropertyManager();
        this.partitionLoadWatcher = new DirectPartitionLoadWatcher();
        this.dspFactory = new ControlOnlyDataSetProcessorFactory();
    }

    @Override
    public BackupManager getBackupManager(){
        return NoOpBackupManager.getInstance();
    }

    @Override
    public PartitionLoadWatcher getLoadWatcher(){
        return partitionLoadWatcher;
    }

    @Override
    public DataSetProcessorFactory getProcessorFactory(){
        return dspFactory;
    }

    @Override
    public PropertyManager getPropertyManager(){
        return propertyManager;
    }
}
