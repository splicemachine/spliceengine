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

import java.sql.Connection;
import java.util.concurrent.*;

import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServiceDiscovery;
import com.splicemachine.db.iapi.services.authorization.AuthorizationFactory;
import com.splicemachine.db.iapi.services.authorization.AuthorizationFactoryService;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.OperationManager;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.impl.sql.execute.sequence.SequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.Manager;
import com.splicemachine.db.impl.sql.pyprocedure.PyInterpreterPool;
import com.splicemachine.tools.CachedResourcePool;
import com.splicemachine.tools.ResourcePool;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.UUIDGenerator;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class EngineDriver{
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
    private final OperationManager operationManager;
    private final SqlEnvironment environment;
    private final ServiceDiscovery serviceDiscovery;

    public static void loadDriver(SqlEnvironment environment){
        INSTANCE=new EngineDriver(environment);
    }

    public static void shutdownDriver() {
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
        this.operationManager = environment.getOperationManager();
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
        this.serviceDiscovery = environment.serviceDiscovery();
        // Initiate PyInterpreterPool which is a singleton
        PyInterpreterPool.getInstance();
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

    public OperationManager getOperationManager() { return operationManager; }

    public ServiceDiscovery getServiceDiscovery() {
        return serviceDiscovery;
    }

}
