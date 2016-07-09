package com.splicemachine;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.impl.sql.execute.sequence.SequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.Manager;
import com.splicemachine.tools.CachedResourcePool;
import com.splicemachine.tools.ResourcePool;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.UUIDGenerator;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class EngineDriver{
    private static EngineDriver INSTANCE;

    private final Connection internalConnection;
    private final Snowflake uuidGen;
    private final ResourcePool<SpliceSequence, SequenceKey> sequencePool;
    private final DatabaseVersion version;
    private final SConfiguration config;
    private final PartitionLoadWatcher loadWatcher;
    private final DataSetProcessorFactory processorFactory;
    private final PropertyManager propertyManager;
    private final SqlExceptionFactory exceptionFactory;
    private final DatabaseAdministrator dbAdmin;
    private final OlapClient olapClient;
    private final SqlEnvironment environment;

    public static void loadDriver(SqlEnvironment environment){
        INSTANCE=new EngineDriver(environment);
    }

    public static EngineDriver driver(){
        return INSTANCE;
    }

    public EngineDriver(SqlEnvironment environment){
        this.environment = environment;
        this.uuidGen=environment.getUUIDGenerator();
        this.internalConnection=environment.getInternalConnection();
        this.version=environment.getVersion();
        this.config=environment.getConfiguration();
        this.loadWatcher = environment.getLoadWatcher();
        this.processorFactory = environment.getProcessorFactory();
        this.olapClient = environment.getOlapClient();
        this.propertyManager = environment.getPropertyManager();
        this.exceptionFactory = environment.exceptionFactory();
        this.dbAdmin = environment.databaseAdministrator();
        this.sequencePool=CachedResourcePool.Builder.<SpliceSequence, SequenceKey>newBuilder()
                .expireAfterAccess(1,TimeUnit.MINUTES)
                .generator(new ResourcePool.Generator<SpliceSequence, SequenceKey>(){
                    @Override
                    public SpliceSequence makeNew(SequenceKey refKey) throws Exception{
                        return refKey.makeNew();
                    }

                    @Override
                    public void close(SpliceSequence entity) throws Exception{
                        entity.close();
                    }
                }).build();

    }

    public DatabaseAdministrator dbAdministrator(){
        return dbAdmin;
    }

    public UUIDGenerator newUUIDGenerator(int blockSize){
        return uuidGen.newGenerator(blockSize);
    }

    public Connection getInternalConnection(){
        return internalConnection;
    }

    public ResourcePool<SpliceSequence, SequenceKey> sequencePool(){
        return sequencePool;
    }

    public DatabaseVersion getVersion(){
        return version;
    }

    public SConfiguration getConfiguration(){
        return config;
    }

    public PropertyManager propertyManager(){
        return propertyManager;
    }

    public DataSetProcessorFactory processorFactory(){
        return processorFactory;
    }

    public PartitionLoadWatcher partitionLoadWatcher(){
        return loadWatcher;
    }

    public BackupManager backupManager(){
        return environment.getBackupManager();
    }

    public Manager manager(){
        return environment.getManager();
    }

    public SqlExceptionFactory getExceptionFactory(){
        return exceptionFactory;
    }

    public OlapClient getOlapClient() {
        return olapClient;
    }
}
