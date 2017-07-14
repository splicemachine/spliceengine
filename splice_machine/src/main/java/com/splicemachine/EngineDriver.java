/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.PermissionsDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.catalog.ManagedCacheMBean;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.impl.sql.execute.sequence.SequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.utils.BaseAdminProcedures;
import com.splicemachine.hbase.JMXThreadPool;
import com.splicemachine.hbase.ManagedThreadPool;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.Manager;
import com.splicemachine.tools.CachedResourcePool;
import com.splicemachine.tools.ResourcePool;
import com.splicemachine.utils.Pair;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.UUIDGenerator;
import org.spark_project.guava.collect.Lists;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class EngineDriver extends BaseAdminProcedures{
    private static volatile EngineDriver INSTANCE;

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
    private final ExecutorService threadPool;

    public static void loadDriver(SqlEnvironment environment){
        INSTANCE=new EngineDriver(environment);
    }

    public static void shutdownDriver() {
        if (INSTANCE != null)
            INSTANCE.threadPool.shutdownNow();
        INSTANCE = null;
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

        /* Create a general purpose thread pool */
        final AtomicLong count = new AtomicLong(0);
        this.threadPool = new ManagedThreadPool(new ThreadPoolExecutor(0, config.getThreadPoolMaxSize(),
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                (runnable) -> {
                    Thread t = new Thread(runnable, "SpliceThreadPool-" + count.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()));
        ((ThreadPoolExecutor)threadPool).allowCoreThreadTimeOut(false);
        ((ThreadPoolExecutor)threadPool).prestartAllCoreThreads();
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

    public Manager manager(){
        return environment.getManager();
    }

    public void refreshEnterpriseFeatures(){
        environment.refreshEnterpriseFeatures();
    }

    public SqlExceptionFactory getExceptionFactory(){
        return exceptionFactory;
    }

    public OlapClient getOlapClient() {
        return olapClient;
    }

    public ExecutorService getExecutorService() {
        return threadPool;
    }


    private static final ResultColumnDescriptor[] EXEC_SERVICE_COLUMNS= {
            new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("CurrentPoolSize",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("CurrentlyAvailableThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("CurrentlyExecutingThreads",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("LargestPoolSize",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("MaximumPoolSize",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("PendingTasks",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("ThreadKeepAliveTime",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("TotalCompletedTasks",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("TotalRejectedTasks", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("TotalScheduledTasks",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
    };
    private static final ResultColumnDescriptor[] MANAGED_CACHE_COLUMNS= {
            new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("Size",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("MissCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("MissRate",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("HitCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("HitRate",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
    };

    public static void SYSCS_GET_EXEC_SERVICE_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new BaseAdminProcedures.JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<JMXThreadPool> executorService = JMXUtils.getExecutorService(connections);
                ExecRow template = buildExecRow(EXEC_SERVICE_COLUMNS);
                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(executorService.size());
                int i=0;
                for (JMXThreadPool ex : executorService) {
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    try{
                        dvds[0].setValue(connections.get(i).getFirst());
                        dvds[1].setValue(ex.getCurrentPoolSize());
                        dvds[2].setValue(ex.getCurrentlyAvailableThreads());
                        dvds[3].setValue(ex.getCurrentlyExecutingThreads());
                        dvds[4].setValue(ex.getLargestPoolSize());
                        dvds[5].setValue(ex.getMaximumPoolSize());
                        dvds[6].setValue(ex.getPendingTasks());
                        dvds[7].setValue(ex.getThreadKeepAliveTime());
                        dvds[8].setValue(ex.getTotalCompletedTasks());
                        dvds[9].setValue(ex.getTotalRejectedTasks());
                        dvds[10].setValue(ex.getTotalScheduledTasks());
                    }catch(StandardException se){
                        throw PublicAPI.wrapStandardException(se);
                    }
                    rows.add(template.getClone());
                    i++;
                }

                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, EXEC_SERVICE_COLUMNS,lastActivation);
                try {
                    resultsToWrap.openCore();
                } catch (StandardException e) {
                    throw PublicAPI.wrapStandardException(e);
                }
                EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);
                resultSet[0] = ers;
            }
        });
    }

    protected static ExecRow buildExecRow(ResultColumnDescriptor[] columns) throws SQLException {
        ExecRow template = new ValueRow(columns.length);
        try {
            DataValueDescriptor[] rowArray = new DataValueDescriptor[columns.length];
            for(int i=0;i<columns.length;i++){
                rowArray[i] = columns[i].getType().getNull();
            }
            template.setRowArray(rowArray);
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
        return template;
    }

    //For cache
    public static void SYSCS_GET_CACHE_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new BaseAdminProcedures.JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                String [] managedCaches = new String [] { "oidTdCache", "nameTdCache", "spsNameCache", "sequenceGeneratorCache", "sequenceGeneratorCache", "permissionsCache",
                "partitionStatisticsCache", "storedPreparedStatementCache", "statementCache", "schemaCache", "aliasDescriptorCache", "roleCache"};

                List<ManagedCacheMBean> executorService = JMXUtils.getManagedCache(connections, managedCaches);
                ExecRow template = buildExecRow(MANAGED_CACHE_COLUMNS);
                List<ExecRow> rows = Lists.newArrayListWithExpectedSize(executorService.size());
                int i=0;
                for (ManagedCacheMBean ex : executorService) {
                    template.resetRowArray();
                    DataValueDescriptor[] dvds = template.getRowArray();
                    try{
                        dvds[0].setValue(connections.get(i).getFirst());
                        dvds[1].setValue(ex.getSize());
                        dvds[2].setValue(ex.getMissCount());
                        dvds[3].setValue(ex.getMissRate());
                        dvds[4].setValue(ex.getHitCount());
                        dvds[5].setValue(ex.getHitRate());
                    }catch(StandardException se){
                        throw PublicAPI.wrapStandardException(se);
                    }
                    rows.add(template.getClone());
                    i++;
                }

                EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
                Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, MANAGED_CACHE_COLUMNS,lastActivation);
                try {
                    resultsToWrap.openCore();
                } catch (StandardException e) {
                    throw PublicAPI.wrapStandardException(e);
                }
                EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);
                resultSet[0] = ers;
            }
        });
    }
}
