package com.splicemachine.derby.lifecycle;

import com.splicemachine.EngineDriver;
import com.splicemachine.SQLConfiguration;
import com.splicemachine.SqlEnvironment;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.ddl.DDLEnvironmentLoader;
import com.splicemachine.derby.impl.db.AuthenticationConfiguration;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.derby.impl.stats.StatsConfiguration;
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
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.SnowflakeLoader;
import com.splicemachine.uuid.UUIDService;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class EngineLifecycleService implements DatabaseLifecycleService{
    public static ThreadLocal<Boolean> isCreate = new ThreadLocal<>();

    private static final Logger LOG=Logger.getLogger(EngineLifecycleService.class);
    private final DistributedDerbyStartup startup;
    private final SConfiguration configuration;
    private final Properties dbProperties = new Properties();

    private Snowflake snowflake;
    private SnowflakeLoader snowflakeLoader;
    private Connection internalConnection;
    private DatabaseVersion spliceVersion;
    private ManifestReader manifestReader;

    public EngineLifecycleService(DistributedDerbyStartup startup,SConfiguration configuration){
        this.startup=startup;
        this.configuration=configuration;

        dbProperties.put(EmbedConnection.INTERNAL_CONNECTION,"true");
    }

    @Override
    public void start() throws Exception{
        System.setProperty("com.splicemachine.enableLegacyAsserts",Boolean.TRUE.toString());
        loadManifest();

        startup.distributedStart();

        // Initialize the table pool so the UUID generator below can access the SPLICE_SEQUENCES table in HBase.
        new SpliceAccessManager();
        // Since SPLICE_SEQUENCES table is set up, initialize the UUID generator, so the new Derby connection below
        // can execute an upgrade process if requested and create and store new system objects in the data dictionary tables.
        loadUUIDGenerator(configuration.getInt(SQLConfiguration.PARTITIONSERVER_PORT));

        configuration.addDefaults(AuthenticationConfiguration.defaults); //add auth defaults
        configuration.addDefaults(StatsConfiguration.defaults);
        // Create an embedded connection to Derby.  This essentially boots up Derby by creating an internal connection to it.
        // External connections to Derby are created later when the Derby network server is started.
        EmbedConnectionMaker maker = new EmbedConnectionMaker();
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
                        PipelineDriver.driver().exceptionFactory(),siDriver.readController());
            }
        };
        ContextFactoryDriverService.setDriver(cfDriver); //set the Context service for the pipeline

        //initialize the engine driver
        SqlEnvironment ese = SqlEnvironmentLoader.loadEnvironment(configuration,snowflake,internalConnection,spliceVersion);
        EngineDriver.loadDriver(ese);

        //initialize the DDLDriver
        DDLDriver.loadDriver(DDLEnvironmentLoader.loadEnvironment(configuration,EngineDriver.driver().getExceptionFactory()));
        SpliceDatabase db = (SpliceDatabase)((EmbedConnection)internalConnection).getLanguageConnection().getDatabase();
        db.registerDDL();
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws Exception{
        manifestReader.registerJMX(mbs);
    }

    @Override
    public void shutdown() throws Exception{
        try{
            if(snowflakeLoader!=null)
                snowflakeLoader.unload();
        }catch(Exception e){
            LOG.error("Unexpected error during shutdown",e);
        }
        try{
            if(internalConnection!=null)
                internalConnection.close();
        }catch(Exception e){
            LOG.error("Unexpected error during shutdown",e);
        }

        try{
            SIDriver driver = SIDriver.driver();
            if(driver!=null)
                driver.getTimestampSource().shutdown();
        }catch(Exception e){
            LOG.error("Unexpected error during shutdown",e);
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void loadUUIDGenerator(int port) throws IOException{
        SIDriver driver = SIDriver.driver();
        assert driver!=null: "Must boot SI before the database!";
        snowflakeLoader= new SnowflakeLoader(driver.getTableFactory(),driver.getOperationFactory(),driver.filterFactory());

        snowflake = snowflakeLoader.load(port);
        /*
         * In order to make snowflake available for derby classes during the boot sequence, we have to set snowflake
         * first.
         */
        UUIDService.setSnowflake(snowflake);
    }

    private void loadManifest(){
        manifestReader = new ManifestReader();
        spliceVersion = manifestReader.createVersion();
        /* Make version information available to code in the derby codebase. */
        System.setProperty(Property.SPLICE_RELEASE, spliceVersion.getRelease());
        System.setProperty(Property.SPLICE_VERSION_HASH, spliceVersion.getImplementationVersion());
        System.setProperty(Property.SPLICE_BUILD_TIME, spliceVersion.getBuildTime());
        System.setProperty(Property.SPLICE_URL, spliceVersion.getURL());
    }
}
