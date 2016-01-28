package com.splicemachine.derby.lifecycle;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.backup.RestoreItem;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.PropertyManagerService;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.impl.sql.HSqlExceptionFactory;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.control.CostChoosingDataSetProcessorFactory;
import com.splicemachine.derby.stream.spark.SparkDataSetProcessor;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;

import java.io.IOException;
import java.sql.Connection;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HEngineSqlEnv extends EngineSqlEnvironment{

    private PropertyManager propertyManager;
    private PartitionLoadWatcher loadWatcher;
    private DataSetProcessorFactory processorFactory;
    private SqlExceptionFactory exceptionFactory;

    @Override
    public void initialize(SConfiguration config,
                           Snowflake snowflake,
                           Connection internalConnection,
                           DatabaseVersion spliceVersion){
        super.initialize(config,snowflake,internalConnection,spliceVersion);
        this.propertyManager =PropertyManagerService.loadPropertyManager();
        this.loadWatcher = HBaseRegionLoads.INSTANCE;
        SIDriver driver =SIDriver.driver();
        ControlDataSetProcessor cdsp = new ControlDataSetProcessor(driver.getTxnSupplier(),
                driver.getIgnoreTxnSupplier(),driver.getTransactor(),
                driver.getOperationFactory());
        this.processorFactory = new CostChoosingDataSetProcessorFactory(new SparkDataSetProcessor(), cdsp, config);
        this.exceptionFactory = new HSqlExceptionFactory(SIDriver.driver().getExceptionFactory());
    }

    @Override
    public SqlExceptionFactory exceptionFactory(){
        return exceptionFactory;
    }

    @Override
    public BackupManager getBackupManager(){
        return new BackupManager(){
            @Override
            public void fullBackup(String backupDirectory) throws IOException{
                throw new UnsupportedOperationException("IMPLEMENT");
            }

            @Override
            public void incrementalBackup(String directory) throws IOException{
                throw new UnsupportedOperationException("IMPLEMENT");
            }

            @Override
            public String getRunningBackup(){
                throw new UnsupportedOperationException("IMPLEMENT");
            }

            @Override
            public void restoreDatabase(String directory,long backupId) throws IOException{
                throw new UnsupportedOperationException("IMPLEMENT");
            }

            @Override
            public void removeBackup(long backupId) throws IOException{
                throw new UnsupportedOperationException("IMPLEMENT");
            }

            @Override
            public Iterator<RestoreItem> listRestoreItems() throws IOException{
                throw new UnsupportedOperationException("IMPLEMENT");
            }
        };
    }

    @Override
    public PartitionLoadWatcher getLoadWatcher(){
        return loadWatcher;
    }

    @Override
    public DataSetProcessorFactory getProcessorFactory(){
        return processorFactory;
    }

    @Override
    public PropertyManager getPropertyManager(){
        return propertyManager;
    }
}
