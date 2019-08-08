/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.lifecycle;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.sql.Connection;
import java.util.Properties;

import com.splicemachine.system.SimpleSparkVersion;
import com.splicemachine.system.SparkVersion;
import org.apache.log4j.Logger;

import com.splicemachine.EngineDriver;
import com.splicemachine.SqlEnvironment;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.ddl.DDLEnvironmentLoader;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.lifecycle.DatabaseLifecycleService;
import com.splicemachine.pipeline.ContextFactoryDriverService;
import com.splicemachine.pipeline.DerbyContextFactoryLoader;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.ReferenceCountingFactoryDriver;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.tools.version.ManifestReader;
import com.splicemachine.utils.logging.LogManager;
import com.splicemachine.utils.logging.Logging;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.UUIDService;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class EngineLifecycleService implements DatabaseLifecycleService{
    public static final ThreadLocal<Boolean> isCreate = new ThreadLocal<>();
    public static final ThreadLocal<Boolean> toUpgrade = new ThreadLocal<>();

    private static final Logger LOG=Logger.getLogger(EngineLifecycleService.class);
    private final DistributedDerbyStartup startup;
    private final SConfiguration configuration;
    private final Properties dbProperties = new Properties();
    private final boolean isMaster;

    private Snowflake snowflake;
    private Connection internalConnection;
    private DatabaseVersion spliceVersion;
    private SparkVersion sparkVersion;
    private ManifestReader manifestReader;
    private Logging logging;
    private SpliceDatabase db;

    public EngineLifecycleService(DistributedDerbyStartup startup,SConfiguration configuration,boolean isMaster){
        this.startup=startup;
        this.configuration=configuration;
        this.isMaster = isMaster;
        dbProperties.put(EmbedConnection.INTERNAL_CONNECTION,"true");
    }

    @Override
    public void start() throws Exception{
        loadManifest();

        startup.distributedStart();

        // Initialize the table pool so the UUID generator below can access the SPLICE_SEQUENCES table in HBase.
        new SpliceAccessManager();
        // Since SPLICE_SEQUENCES table is set up, initialize the UUID generator, so the new Derby connection below
        // can execute an upgrade process if requested and create and store new system objects in the data dictionary tables.

        snowflake = SIDriver.driver().getSnowflakeFactory().getSnowFlake();
        UUIDService.setSnowflake(snowflake);
        // Create an embedded connection to Derby.  This essentially boots up Derby by creating an internal connection to it.
        // External connections to Derby are created later when the Derby network server is started.
        EmbedConnectionMaker maker = new EmbedConnectionMaker();
        toUpgrade.set(isMaster);
        if(startup.connectAsFirstTime()){
            isCreate.set(Boolean.TRUE);
            internalConnection=maker.createFirstNew(configuration,dbProperties);
        }else{
            isCreate.set(Boolean.FALSE);
            internalConnection=maker.createNew(dbProperties);
        }
        startup.markBootFinished();
        isCreate.remove();

        ContextFactoryDriver cfDriver = new ReferenceCountingFactoryDriver(){
            @Override
            protected ContextFactoryLoader newDelegate(long conglomerateId){
                SIDriver siDriver=SIDriver.driver();
                return new DerbyContextFactoryLoader(conglomerateId,siDriver.getOperationStatusLib(),
                        PipelineDriver.driver().exceptionFactory(),siDriver.readController(),
                        siDriver.getOperationFactory());
            }
        };
        ContextFactoryDriverService.setDriver(cfDriver); //set the Context service for the pipeline

        //initialize the engine driver
        SqlEnvironment ese = SqlEnvironmentLoader.loadEnvironment(configuration,snowflake,internalConnection,spliceVersion);
        EngineDriver.loadDriver(ese);
        SIDriver.driver().engineStarted();

        //initialize the DDLDriver
        DDLDriver.loadDriver(DDLEnvironmentLoader.loadEnvironment(configuration,EngineDriver.driver().getExceptionFactory()));
        db = (SpliceDatabase)((EmbedConnection)internalConnection).getLanguageConnection().getDatabase();
        db.registerDDL();

        logging = new LogManager();
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws Exception{
        manifestReader.registerJMX(mbs);
        try{
            ObjectName on=new ObjectName("com.splicemachine.utils.logging:type=LogManager");
            ObjectName execServ = new ObjectName("com.splicemachine.derby.lifecycle:type=ExecutorService");
            mbs.registerMBean(logging,on);
            mbs.registerMBean(SIDriver.driver().getRejectingExecutorService(),execServ);

            ObjectName rollforward = new ObjectName("com.splicemachine.si.api.rollforward:type=RollForward");
            mbs.registerMBean(SIDriver.driver().getRollForward(),rollforward);
            db.getDataDictionary().getDataDictionaryCache().registerJMX(mbs);


        }catch(InstanceAlreadyExistsException ignored){
            /*
             * For most purposes, this should never happen. However, it's possible to happen
             * when you are booting a regionserver and master in the same JVM (e.g. for testing purposes); Since
             * we can only really have one version of the software on a single node at one time, we just ignore
             * this exception and don't worry about it too much.
             */
        }
    }

    @Override
    public void shutdown() throws Exception{
        
        EngineDriver.shutdownDriver();

        try{
            SIDriver driver = SIDriver.driver();
            if(driver!=null)
                driver.shutdownDriver();
        }catch(Exception e){
            LOG.error("Unexpected error during shutdown",e);
        }
    }

    private void loadManifest(){
        manifestReader = new ManifestReader();
        spliceVersion = manifestReader.createVersion();
        sparkVersion = manifestReader.createSparkVersion();
        /* Make version information available to code in the derby codebase. */
        System.setProperty(Property.SPLICE_RELEASE, spliceVersion.getRelease());
        System.setProperty(Property.SPLICE_VERSION_HASH, spliceVersion.getImplementationVersion());
        System.setProperty(Property.SPLICE_BUILD_TIME, spliceVersion.getBuildTime());
        System.setProperty(Property.SPLICE_URL, spliceVersion.getURL());

        // If we can't parse the compile-time spark version string, use 2.2.0
        // as the default, the lowest expected version of spark.
        if (sparkVersion.isUnknown())
            sparkVersion = new SimpleSparkVersion("2.2.0");

        System.setProperty(Property.SPLICE_SPARK_COMPILE_VERSION, sparkVersion.getVersionString());
        String spliceSparkVersionString = System.getProperty(Property.SPLICE_SPARK_VERSION);
        SparkVersion spliceSparkVersion = new SimpleSparkVersion(spliceSparkVersionString);

        // If splice.spark.version is not set, or is not a proper version string, use the
        // splice compile-time spark version as the active spark version.
        if (spliceSparkVersion.isUnknown())
            System.setProperty(Property.SPLICE_SPARK_VERSION, sparkVersion.getVersionString());
    }
}
