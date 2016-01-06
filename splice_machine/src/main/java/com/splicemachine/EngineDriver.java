package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.impl.sql.execute.sequence.SequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.FileResourceFactory;
import com.splicemachine.derby.utils.Sequencer;
import com.splicemachine.tools.CachedResourcePool;
import com.splicemachine.tools.ResourcePool;
import com.splicemachine.tools.version.SpliceMachineVersion;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.UUIDGenerator;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class EngineDriver{
    private static EngineDriver INSTANCE;

    private final Connection internalConnection;
    private final Snowflake uuidGen;
    private final ResourcePool<SpliceSequence, SequenceKey> sequencePool;
    private final SpliceMachineVersion version;
    private final SConfiguration config;
    private final Sequencer conglomerateSequencer;
    private final StorageFactory storageFactory;
    private final FileResourceFactory fileResourceFactory;
    private final BackupManager backupManager;
    private final PartitionLoadWatcher loadWatcher;
    private final DataSetProcessorFactory processorFactory;
    private final PropertyManager propertyManager;

    public static void loadDriver(SqlEnvironment environment){
        INSTANCE=new EngineDriver(environment);
    }

    public static EngineDriver driver(){
        return INSTANCE;
    }

    public EngineDriver(SqlEnvironment environment){
        this.uuidGen=environment.getUUIDGenerator();
        this.internalConnection=environment.getInternalConnection();
        this.version=environment.getVersion();
        this.config=environment.getConfiguration();
        this.conglomerateSequencer = environment.getConglomerateSequencer();
        this.fileResourceFactory = environment.getFileResourceFactory();
        this.storageFactory = environment.getStorageFactory();
        this.backupManager = environment.getBackupManager();
        this.loadWatcher = environment.getLoadWatcher();
        this.processorFactory = environment.getProcessorFactory();
        this.propertyManager = environment.getPropertyManager();
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

    public UUIDGenerator newUUIDGenerator(int blockSize){
        return uuidGen.newGenerator(blockSize);
    }

    public Connection getInternalConnection(){
        return internalConnection;
    }

    public ResourcePool<SpliceSequence, SequenceKey> sequencePool(){
        return sequencePool;
    }

    public SpliceMachineVersion getVersion(){
        return version;
    }

    public SConfiguration getConfiguration(){
        return config;
    }

    public StorageFactory getStorageFactory(){
        return storageFactory;
    }

    public FileResourceFactory fileResourceFactory(){
        return fileResourceFactory;
    }

    public Sequencer getConglomerateSequencer(){
        return conglomerateSequencer;
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
        return backupManager;
    }
}
