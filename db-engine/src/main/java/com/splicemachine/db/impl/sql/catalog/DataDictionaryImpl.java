/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.BaseTypeIdImpl;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.*;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.locks.LockFactory;
import com.splicemachine.db.iapi.services.locks.ShExLockable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionFactory;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.*;
import com.splicemachine.db.iapi.store.access.*;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.impl.jdbc.LOBStoredProcedure;
import com.splicemachine.db.impl.services.locks.Timeout;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.compile.SetNode;
import com.splicemachine.db.impl.sql.compile.TableName;
import com.splicemachine.db.impl.sql.execute.JarUtil;
import com.splicemachine.db.impl.sql.execute.TriggerEventDML;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Function;
import splice.com.google.common.base.Optional;
import splice.com.google.common.collect.FluentIterable;
import splice.com.google.common.collect.ImmutableListMultimap;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.collect.Multimaps;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard database implementation of the data dictionary that stores the information in the system catlogs.
 *
 * See SpliceDataDictionary in spliceengine repo.
 */
public abstract class DataDictionaryImpl extends BaseDataDictionary{
    private static final Logger LOG = Logger.getLogger(DataDictionaryImpl.class);

    /**
     * Runtime definition of the functions from SYSFUN_FUNCTIONS.
     * Populated dynamically as functions are called.
     */
    protected final AliasDescriptor[] sysfunDescriptors=new AliasDescriptor[SYSFUN_FUNCTIONS.length];

    // the structure that holds all the core table info
    protected TabInfoImpl[] coreInfo;

    /*
    ** SchemaDescriptors for system and app schemas.  Both
    ** are canonical.  We cache them for fast lookup.
    */

    protected SchemaDescriptor systemSchemaDesc;
    protected SchemaDescriptor sysIBMSchemaDesc;
    protected SchemaDescriptor sysIBMADMSchemaDesc;
    protected SchemaDescriptor declaredGlobalTemporaryTablesSchemaDesc;
    protected SchemaDescriptor systemUtilSchemaDesc;
    protected SchemaDescriptor sysFunSchemaDesc;
    protected SchemaDescriptor sysViewSchemaDesc;

    /**
     * Dictionary version of the on-disk database
     */
    protected DD_Version dictionaryVersion;
    /**
     * Dictionary version of the currently running engine
     */
    protected DD_Version softwareVersion;
    protected String authorizationDatabaseOwner;
    protected boolean usesSqlAuthorization;

    /**
     * When enabled, will update the data dictionary with all system stored procedures defined in the Derby jar.
     * If a system stored procedure already exists in the data dictionary, it will be dropped and then created again.
     * <p/>
     * Defaults to false (off).
     */
    private static boolean updateSystemProcs=
            PropertyUtil.getSystemBoolean(Property.LANG_UPDATE_SYSTEM_PROCS,Property.LANG_UPDATE_SYSTEM_PROCS_DEFAULT);

    /*
    ** This property and value are written into the database properties
    ** when the database is created, and are used to determine whether
    ** the system catalogs need to be upgraded.
    */

    // the structure that holds all the noncore info
    protected TabInfoImpl[] noncoreInfo;

    // no other system tables have id's in the configuration.

    public DataDescriptorGenerator dataDescriptorGenerator;
    protected DataValueFactory dvf;
    AccessFactory af;
    //DataDictionaryContext                ddc;

    protected ExecutionFactory exFactory;
    protected UUIDFactory uuidFactory;
    Properties startupParameters;
    int engineType;

    /* Information about whether or not we are at boot time */
    protected boolean booting;
    protected TransactionController bootingTC;
    protected DependencyManager dmgr;

    protected DataDictionaryCache dataDictionaryCache;

    /*
    ** Lockable object for synchronizing transition from caching to non-caching
    */
    ShExLockable cacheCoordinator;
    public LockFactory lockFactory;

    private Map<String, Map<String, String>> sequenceIDs;

    /**
     * True if the database is read only and requires
     * some form of upgrade, that makes the stored prepared
     * statements invalid.
     * With this case the engine is running at a different
     * version to the underlying stored database. This
     * can happen in 5.1 if the database is read only
     * and a different point release (later than 5.1.25?)
     * to the one that created it, has been booted. (Beetle 5170).
     * <p/>
     * <p/>
     * In 5.2 and newer this will be the case if the engine
     * booting the database is newer than the engine
     * that created it.
     */
    private boolean readOnlyUpgrade;


    //systemSQLNameNumber is the number used as the last digit during the previous call to getSystemSQLName.
    //If it is 9 for a given calendarForLastSystemSQLName, we will restart the counter to 0
    //and increment the calendarForLastSystemSQLName by 10ms.
    private int systemSQLNameNumber;
    private GregorianCalendar calendarForLastSystemSQLName=new GregorianCalendar();
    private long timeForLastSystemSQLName;


    /**
     * Collation Type for SYSTEM schemas. In Derby 10.3, this will always
     * be UCS_BASIC
     */
    private int collationTypeOfSystemSchemas;

    /**
     * Collation Type for user schemas. In Derby 10.3, this is either
     * UCS_BASIC or TERRITORY_BASED. The exact value is decided by what has
     * user asked for through JDBC url optional attribute COLLATION. If that
     * atrribute is set to UCS_BASIC, the collation type for user schemas
     * will be UCS_BASIC. If that attribute is set to TERRITORY_BASED, the
     * collation type for user schemas will be TERRITORY_BASED. If the user
     * has not provide COLLATION attribute value in the JDBC url at database
     * create time, then collation type of user schemas will default to
     * UCS_BASIC. Pre-10.3 databases after upgrade to Derby 10.3 will also
     * use UCS_BASIC for collation type of user schemas.
     */
    private int collationTypeOfUserSchemas;

    /*
    ** Constructor
    */

    public DataDictionaryImpl(){

    }


    /**
     * This is the data dictionary implementation for
     * the standard database engine.
     *
     * @return true if this service requested is for a database engine.
     */
    @Override
    public boolean canSupport(Properties startParams){
        return Monitor.isDesiredType(startParams,EngineType.STANDALONE_DB);
    }

    /**
     * Start-up method for this instance of the data dictionary.
     *
     * @param startParams The start-up parameters
     * @throws StandardException Thrown if the module fails to start
     */
    @Override
    public void boot(boolean create,Properties startParams) throws StandardException{
        softwareVersion=new DD_Version(this,DataDictionary.DD_VERSION_DERBY_10_9);
        startupParameters=startParams;
        uuidFactory=Monitor.getMonitor().getUUIDFactory();
        engineType=Monitor.getEngineType(startParams);
        //Set the collation type of system schemas before we start loading 
        //built-in schemas's SchemaDescriptor(s). This is because 
        //SchemaDescriptor will look to DataDictionary to get the correct 
        //collation type for themselves. We can't load SD for SESSION schema
        //just yet because we do not know the collation type for user schemas
        //yet. We will know the right collation for user schema little later
        //in this boot method.
        collationTypeOfSystemSchemas=StringDataValue.COLLATION_TYPE_UCS_BASIC;
        getBuiltinSystemSchemas();

        // REMIND: actually, we're supposed to get the DataValueFactory
        // out of the connection context...this is a bit of a shortcut.
        // We get the DataValueFactory early in order to help bootstrap the system catalogs.
        LanguageConnectionFactory langConnFactory=(LanguageConnectionFactory)Monitor.bootServiceModule(
                create,this,LanguageConnectionFactory.MODULE,startParams);

        dvf=langConnFactory.getDataValueFactory();
        exFactory=(ExecutionFactory)Monitor.bootServiceModule(
                create,this,
                ExecutionFactory.MODULE,
                startParams);

        // initailze the arrays of core and noncore tables
        initializeCatalogInfo();

        // indicate that we are in the process of booting
        booting=true;

        // set only if child class hasn't overriden this already
        if(dataDescriptorGenerator==null){
            dataDescriptorGenerator=new DataDescriptorGenerator(this);
        }

        if(!create){
            // SYSTABLES
            coreInfo[SYSTABLES_CORE_NUM].setHeapConglomerate(
                    getBootParameter(startParams,CFG_SYSTABLES_ID,true));

            coreInfo[SYSTABLES_CORE_NUM].setIndexConglomerate(SYSTABLESRowFactory.SYSTABLES_INDEX1_ID,
                    getBootParameter(startParams,CFG_SYSTABLES_INDEX1_ID,true));


            coreInfo[SYSTABLES_CORE_NUM].setIndexConglomerate(
                    SYSTABLESRowFactory.SYSTABLES_INDEX2_ID,
                    getBootParameter(startParams,CFG_SYSTABLES_INDEX2_ID,true));

            // SYSCOLUMNS

            coreInfo[SYSCOLUMNS_CORE_NUM].setHeapConglomerate(
                    getBootParameter(startParams,CFG_SYSCOLUMNS_ID,true));


            coreInfo[SYSCOLUMNS_CORE_NUM].setIndexConglomerate(
                    SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,
                    getBootParameter(startParams,CFG_SYSCOLUMNS_INDEX1_ID,true));
            // 2nd syscolumns index added in Xena, hence may not be there
            coreInfo[SYSCOLUMNS_CORE_NUM].setIndexConglomerate(
                    SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX2_ID,
                    getBootParameter(startParams,CFG_SYSCOLUMNS_INDEX2_ID,false));

            // SYSCONGLOMERATES

            coreInfo[SYSCONGLOMERATES_CORE_NUM].setHeapConglomerate(
                    getBootParameter(startParams,CFG_SYSCONGLOMERATES_ID,true));


            coreInfo[SYSCONGLOMERATES_CORE_NUM].setIndexConglomerate(
                    SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX1_ID,
                    getBootParameter(startParams,CFG_SYSCONGLOMERATES_INDEX1_ID,true));


            coreInfo[SYSCONGLOMERATES_CORE_NUM].setIndexConglomerate(
                    SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX2_ID,
                    getBootParameter(startParams,CFG_SYSCONGLOMERATES_INDEX2_ID,true));

            coreInfo[SYSCONGLOMERATES_CORE_NUM].setIndexConglomerate(
                    SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX3_ID,
                    getBootParameter(startParams,CFG_SYSCONGLOMERATES_INDEX3_ID,true));


            // SYSSCHEMAS
            coreInfo[SYSSCHEMAS_CORE_NUM].setHeapConglomerate(
                    getBootParameter(startParams,CFG_SYSSCHEMAS_ID,true));


            coreInfo[SYSSCHEMAS_CORE_NUM].setIndexConglomerate(
                    SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID,
                    getBootParameter(startParams,CFG_SYSSCHEMAS_INDEX1_ID,true));

            coreInfo[SYSSCHEMAS_CORE_NUM].setIndexConglomerate(
                    SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX2_ID,
                    getBootParameter(startParams,CFG_SYSSCHEMAS_INDEX2_ID,true));

        }

        dataDictionaryCache = new DataDictionaryCache(startParams,this);

        sequenceIDs=new HashMap<>();

        /* Get the object to coordinate cache transitions */
        cacheCoordinator=new ShExLockable();

        /* Get AccessFactory in order to transaction stuff */
        af=(AccessFactory)Monitor.findServiceModule(this,AccessFactory.MODULE);

        /* Get the lock factory */
        lockFactory=af.getLockFactory();

        /*
         * now we need to setup a context stack for the database creation work.
         * We assume the System boot process has created a context
         * manager already, but not that contexts we need are there.
         */
        ContextService csf=ContextService.getFactory();

        ContextManager cm=csf.getCurrentContextManager();
        if(SanityManager.DEBUG)
            SanityManager.ASSERT((cm!=null),"Failed to get current ContextManager");

        // RESOLVE other non-StandardException errors.
        bootingTC=null;
        try{
            // Get a transaction controller. This has the side effect of
            // creating a transaction context if there isn't one already.
            bootingTC=af.getTransaction(cm);

            /*
                We need an execution context so that we can generate rows
                REMIND: maybe only for create case?
             */
            exFactory.newExecutionContext(cm);

            DataDescriptorGenerator ddg=getDataDescriptorGenerator();

            //We should set the user schema collation type here now because
            //later on, we are going to create user schema SPLICE. By the time any
            //user schema gets created, we should have the correct collation
            //type set for such schemas to use. For this reason, don't remove
            //the following if else statement and don't move it later in this 
            //method.
            String userDefinedCollation;
            if(create){
                //Get the collation attribute from the JDBC url. It can only 
                //have one of 2 possible values - UCS_BASIC or TERRITORY_BASED
                //This attribute can only be specified at database create time. 
                //The attribute value has already been verified in DVF.boot and
                //hence we can be assured that the attribute value if provided
                //is either UCS_BASIC or TERRITORY_BASED. If none provided, 
                //then we will take it to be the default which is UCS_BASIC.
                userDefinedCollation=startParams.getProperty(
                        Attribute.COLLATION,Property.UCS_BASIC_COLLATION);
                bootingTC.setProperty(Property.COLLATION,userDefinedCollation,true);
            }else{
                userDefinedCollation=startParams.getProperty(
                        Property.COLLATION,Property.UCS_BASIC_COLLATION);
            }

            //Initialize the collation type of user schemas by looking at
            //collation property/attribute.
            collationTypeOfUserSchemas=DataTypeDescriptor.getCollationType(userDefinedCollation);
            if(SanityManager.DEBUG)
                SanityManager.ASSERT((collationTypeOfUserSchemas!=-1),"Invalid collation type: "+userDefinedCollation);

            //Now is also a good time to create schema descriptor for global
            //temporary tables. Since this is a user schema, it should use the
            //collation type associated with user schemas. Since we just 
            //finished setting up the collation type of user schema, it is 
            //safe to create user SchemaDescriptor(s) now.
            declaredGlobalTemporaryTablesSchemaDesc=
                    newDeclaredGlobalTemporaryTablesSchemaDesc(
                            SchemaDescriptor.STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME);

            boolean nativeAuthenticationEnabled=PropertyUtil.nativeAuthenticationEnabled(startParams);

            if(create){
                String userName=IdUtil.getUserNameFromURLProps(startParams);
                authorizationDatabaseOwner=IdUtil.getUserAuthorizationId(userName);
                HashSet newlyCreatedRoutines=new HashSet();

                // create any required tables.
                createDictionaryTables(startParams,bootingTC,ddg);

                //create metadata sps statement required for network server
                createSystemSps(bootingTC);

                // As part of Splice Derby fork, we abstracted out the
                // create_SYSIBM_procedures and create_SYSCS_procedures
                // methods into a generalized SystemProcedureGenerator.
                //
                // create procedures for network server metadata
                // create_SYSIBM_procedures(bootingTC, newlyCreatedRoutines );
                //
                // create the SYSCS_UTIL system procedures
                // create_SYSCS_procedures(bootingTC, newlyCreatedRoutines );

                // create stored procedures
                SystemProcedureGenerator procedureGenerator=getSystemProcedures();
                procedureGenerator.createProcedures(bootingTC,newlyCreatedRoutines);

                // create system aggregates
                SystemAggregateGenerator aggregateGenerator=getSystemAggregateGenerator();
                aggregateGenerator.createAggregates(bootingTC);

                // now grant execute permission on some of these routines
                grantPublicAccessToSystemRoutines(newlyCreatedRoutines,bootingTC,authorizationDatabaseOwner);
                // log the current dictionary version
                dictionaryVersion=softwareVersion;
                
                /* Set properties for current and create time
                 * DataDictionary versions.
                 */
                bootingTC.setProperty(DataDictionary.CORE_DATA_DICTIONARY_VERSION,
                        dictionaryVersion,true);

                bootingTC.setProperty(DataDictionary.CREATE_DATA_DICTIONARY_VERSION,
                        dictionaryVersion,true);

                //
                // If SqlAuthorization is set as a system property during database
                // creation, set it as a database property also, so that it gets persisted.
                //
                // We also turn on SqlAuthorization if NATIVE authentication has been specified.
                //
                if(PropertyUtil.getSystemBoolean(Property.SQL_AUTHORIZATION_PROPERTY)){
                    bootingTC.setProperty(Property.SQL_AUTHORIZATION_PROPERTY,"true",true);
                }
                if(PropertyUtil.getSystemBoolean(Property.SQL_AUTHORIZATION_PROPERTY) || nativeAuthenticationEnabled){
                    usesSqlAuthorization=true;
                }

                // Set default hash algorithm used to protect passwords stored
                // in the database for BUILTIN and NATIVE authentication.
                bootingTC.setProperty(
                        Property.AUTHENTICATION_BUILTIN_ALGORITHM,
                        findDefaultBuiltinAlgorithm(),
                        false);
            }else{
                // Get the ids for non-core tables
                loadDictionaryTables(bootingTC,startParams);

                String sqlAuth=PropertyUtil.getDatabaseProperty(bootingTC,
                        Property.SQL_AUTHORIZATION_PROPERTY);

                // Feature compatibility check.
                if(Boolean.valueOf(startParams.getProperty(Attribute.SOFT_UPGRADE_NO_FEATURE_CHECK))){
                    // Do not perform check if this boot is the first
                    // phase (soft upgrade boot) of a hard upgrade,
                    // which happens in two phases beginning with
                    // DERBY-2264. In this case, we need to always be
                    // able to boot to authenticate, notwithstanding
                    // any feature properties set
                    // (e.g. db.database.sqlAuthorization) which
                    // may not yet be supported until that hard
                    // upgrade has happened, normally causing an error
                    // below.
                    //
                    // Feature sqlAuthorization is a special case:
                    // Since database ownership checking only happens
                    // when sqlAuthorization is true, we can't afford
                    // to *not* use it for upgrades from 10.2 or
                    // later, lest we lose the database owner check.
                    // For upgrades from 10.1 and earlier there is no
                    // database owner check at a hard upgrade.
                    if(dictionaryVersion.majorVersionNumber>=DataDictionary.DD_VERSION_DERBY_10_2){
                        usesSqlAuthorization=Boolean.valueOf(sqlAuth) || nativeAuthenticationEnabled;
                    }
                }else{
                    if(Boolean.valueOf(sqlAuth) || nativeAuthenticationEnabled){
                        usesSqlAuthorization=true;
                    }
                }
            }

            assert authorizationDatabaseOwner!=null:"Failed to get Database Owner authorization";

            // Update (or create) the system stored procedures if requested.
            updateSystemProcedures(bootingTC);

            // read the system configuration setting about whether access restriction is enabled
            // and determine if sysvw.sysschemasview should be updated
            setMetadataAccessRestrictionEnabled();
            updateSystemSchemasView(bootingTC);

            /* Commit & destroy the create database */
            bootingTC.commit();
            assert cm!=null;
            cm.getContext(ExecutionContext.CONTEXT_ID).popMe(); // done with ctx
        }finally{

            if(bootingTC!=null){
                bootingTC.destroy();  // gets rid of the transaction context
                bootingTC=null;
            }
        }

        setDependencyManager();
        booting=false;
    }

    /**
     * Factory method for generating System Procedure information.
     * <p/>
     * Replace this method with a subimplementation to change which
     * SystemProcedures are installed at boot time.
     *
     * @return a generator for SystemProcedures.
     */
    protected abstract SystemProcedureGenerator getSystemProcedures();

    protected abstract SystemAggregateGenerator getSystemAggregateGenerator();

    /**
     * Find the default message digest algorithm to use for BUILTIN
     * authentication on this database.
     *
     * @return the name of the algorithm to use as the default
     */
    private String findDefaultBuiltinAlgorithm(){
        try{
            // First check for the preferred default, and return it if present
            MessageDigest.getInstance(Property.AUTHENTICATION_BUILTIN_ALGORITHM_DEFAULT);
            return Property.AUTHENTICATION_BUILTIN_ALGORITHM_DEFAULT;
        }catch(NoSuchAlgorithmException nsae){
            // Couldn't find the preferred algorithm, so use the fallback
            return Property.AUTHENTICATION_BUILTIN_ALGORITHM_FALLBACK;
        }
    }

    /**
     * sets the dependencymanager associated with this dd. subclasses can
     * override this to install their own funky dependency manager.
     */
    protected abstract void setDependencyManager();

    /**
     * returns the dependencymanager associated with this datadictionary.
     *
     * @see DataDictionary#getDependencyManager
     */
    @Override
    public DependencyManager getDependencyManager(){
        return dmgr;
    }

    /**
     * Stop this module.  In this case, nothing needs to be done.
     */
    @Override
    public void stop(){
    }

    @Override
    public void startWriting(LanguageConnectionContext lcc) throws StandardException{
       throw StandardException.plainWrapException(new RuntimeException("Not Implemented"));
    }

    /**
     * Get a DataDescriptorGenerator, through which we can create
     * objects to be stored in the DataDictionary.
     *
     * @return A DataDescriptorGenerator
     */
    @Override
    public DataDescriptorGenerator getDataDescriptorGenerator(){
        return dataDescriptorGenerator;
    }

    /**
     * Get authorizationID of Database Owner
     *
     * @return authorizationID
     */
    @Override
    public String getAuthorizationDatabaseOwner(){
        return authorizationDatabaseOwner;
    }

    @Override
    public boolean usesSqlAuthorization(){
        return usesSqlAuthorization;
    }

    @Override
    public int getCollationTypeOfSystemSchemas(){
        return collationTypeOfSystemSchemas;
    }

    @Override
    public int getCollationTypeOfUserSchemas(){
        return collationTypeOfUserSchemas;
    }

    @Override
    public DataValueFactory getDataValueFactory(){
        return dvf;
    }

    @Override
    public ExecutionFactory getExecutionFactory(){
        return exFactory;
    }

    /**
     * Set up the builtin schema descriptors for system schemas.
     */
    private void getBuiltinSystemSchemas(){
        if(systemSchemaDesc!=null)
            return;

        systemSchemaDesc=newSystemSchemaDesc(SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME,SchemaDescriptor.SYSTEM_SCHEMA_UUID);
        sysIBMSchemaDesc=newSystemSchemaDesc(SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME,SchemaDescriptor.SYSIBM_SCHEMA_UUID);
        sysIBMADMSchemaDesc=newSystemSchemaDesc(SchemaDescriptor.IBM_SYSTEM_ADM_SCHEMA_NAME,SchemaDescriptor.SYSIBMADM_SCHEMA_UUID);
        systemUtilSchemaDesc=newSystemSchemaDesc(SchemaDescriptor.STD_SYSTEM_UTIL_SCHEMA_NAME,SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID);
        sysFunSchemaDesc=newSystemSchemaDesc(SchemaDescriptor.IBM_SYSTEM_FUN_SCHEMA_NAME,SchemaDescriptor.SYSFUN_SCHEMA_UUID);
        sysViewSchemaDesc=newSystemSchemaDesc(SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME,SchemaDescriptor.SYSVW_SCHEMA_UUID);

    }

    public List<SchemaDescriptor> getAllSchemas(TransactionController tc) throws StandardException{
        List<SchemaDescriptor> lists =new ArrayList<>();
        TabInfoImpl ti=coreInfo[SYSSCHEMAS_CORE_NUM];
        SYSSCHEMASRowFactory rf=(SYSSCHEMASRowFactory)ti.getCatalogRowFactory();
        // don't hold open across commit
        // for update
        // all fields as objects
        // start position -
        // startSearchOperation - none
        //
        // stop position -through last row
        // stopSearchOperation - none
        try (ScanController sc = tc.openScan(
                ti.getHeapConglomerate(),
                false,   // don't hold open across commit
                0,       // for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,      // all fields as objects
                null, // start position -
                0,                            // startSearchOperation - none
                null,                //
                null, // stop position -through last row
                0)) {
            ExecRow outRow = rf.makeEmptyRow();
            SchemaDescriptor schemaDescr;
            while (sc.fetchNext(outRow.getRowArray())) {
                schemaDescr = (SchemaDescriptor) rf.buildDescriptor(outRow, null, this);
                lists.add(schemaDescr);
            }
        }
        return lists;
    }


    // returns null if database is at rev level 10.5 or earlier
    @Override
    public PasswordHasher makePasswordHasher(Dictionary props) throws StandardException{
        // Support for configurable hash algorithm was added in Derby 10.6, so
        // we don't want to store a hash using the new scheme if the database
        // is running in soft upgrade and may be used with an older version
        // later.
        boolean supportConfigurableHash=true;

        // Support for key stretching was added in Derby 10.9, so don't use it
        // if the database may still be used with an older version.
        boolean supportKeyStretching=true;

        if(!supportConfigurableHash){
            return null;
        }else{
            String algorithm=(String)PropertyUtil.getPropertyFromSet(props,Property.AUTHENTICATION_BUILTIN_ALGORITHM);

            if(algorithm==null){
                return null;
            }

            byte[] salt=null;
            int iterations=1;

            if(!algorithm.isEmpty()){

                if(supportKeyStretching){
                    salt=generateRandomSalt(props);
                    iterations=getIntProperty(
                            props,
                            Property.AUTHENTICATION_BUILTIN_ITERATIONS,
                            Property.AUTHENTICATION_BUILTIN_ITERATIONS_DEFAULT,
                            1,Integer.MAX_VALUE);
                }
            }

            return new PasswordHasher(algorithm,salt,iterations);
        }
    }

    /**
     * Generate an array of random bytes to use as salt when hashing
     * credentials.
     *
     * @param props database properties that possibly specify the desired
     *              length of the salt
     * @return random bytes
     */
    private byte[] generateRandomSalt(Dictionary props){
        int saltLength=getIntProperty(props,
                Property.AUTHENTICATION_BUILTIN_SALT_LENGTH,
                Property.AUTHENTICATION_BUILTIN_SALT_LENGTH_DEFAULT,
                0,Integer.MAX_VALUE);

        SecureRandom random=new SecureRandom();
        byte[] salt=new byte[saltLength];
        random.nextBytes(salt);

        return salt;
    }

    /**
     * Get the value of an integer property.
     *
     * @param props        database properties
     * @param key          the key of the property
     * @param defaultValue which value to return if the property is not set,
     *                     or if the property value is not in the valid range
     * @param minValue     lowest property value to accept
     * @param maxValue     highest property value to accept
     * @return the value of the property
     */
    private int getIntProperty(Dictionary props,String key,int defaultValue,int minValue,int maxValue){

        String sVal=(String)PropertyUtil.getPropertyFromSet(props,key);

        if(sVal!=null){
            try{
                int i=Integer.parseInt(sVal);
                if(i>=minValue && i<=maxValue){
                    return i;
                }
            }catch(NumberFormatException nfe){
                // By convention, Derby ignores property values that cannot be
                // parsed. Use the default value instead.
            }
        }

        return defaultValue;
    }


    /**
     * Get the descriptor for the system schema. Schema descriptors include
     * authorization ids and schema ids.
     * <p/>
     * SQL92 allows a schema to specify a default character set - we will
     * not support this.
     *
     * @return The descriptor for the schema.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SchemaDescriptor getSystemSchemaDescriptor() throws StandardException {
        return systemSchemaDesc;
    }


    /**
     * Get the descriptor for the SYSCS_UTIL system schema.
     * Schema descriptors include authorization ids and schema ids.
     * <p/>
     * SQL92 allows a schema to specify a default character set - we will
     * not support this.
     *
     * @return The descriptor for the schema.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SchemaDescriptor getSystemUtilSchemaDescriptor() throws StandardException{
        return (systemUtilSchemaDesc);
    }

    /**
     * Get the descriptor for the SYSIBM schema. Schema descriptors include
     * authorization ids and schema ids.
     * <p/>
     * SQL92 allows a schema to specify a default character set - we will
     * not support this.
     *
     * @return The descriptor for the schema.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SchemaDescriptor getSysIBMSchemaDescriptor() throws StandardException{
        return sysIBMSchemaDesc;
    }

    /**
     * Get the descriptor for the SYSFUN schema. Schema descriptors include
     * authorization ids and schema ids.
     * <p/>
     * SQL92 allows a schema to specify a default character set - we will
     * not support this.
     *
     * @return The descriptor for the schema.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SchemaDescriptor getSysFunSchemaDescriptor() throws StandardException{
        return sysFunSchemaDesc;
    }

    /**
     * Get the descriptor for the declared global temporary table schema which
     * is always named "SESSION".
     *
     * @return The descriptor for the schema.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SchemaDescriptor getDeclaredGlobalTemporaryTablesSchemaDescriptor() throws StandardException{
        return declaredGlobalTemporaryTablesSchemaDesc;
    }

    /**
     * Determine whether a string is the name of the system schema.
     *
     * @param name the name of the schema to check
     * @return true or false
     * @throws StandardException Thrown on failure
     */
    @Override
    public boolean isSystemSchemaName(String name) throws StandardException{
        boolean ret_val=false;

        for(int i=systemSchemaNames.length-1;i>=0;){
            if((ret_val=systemSchemaNames[i--].equals(name)))
                break;
        }

        return (ret_val);
    }

    /**
     * Get the descriptor for the named schema.
     * Schema descriptors include authorization ids and schema ids.
     * SQL92 allows a schema to specify a default character set - we will
     * not support this.  Will check default schema for a match
     * before scanning a system table.
     *
     * @param schemaName The name of the schema we're interested in. Must not be null.
     * @param tc         TransactionController
     * @param raiseError whether an exception should be thrown if the schema does not exist.
     * @return The descriptor for the schema. Can be null (not found) if raiseError is false.
     * @throws StandardException Thrown on error
     */
    @Override
    public SchemaDescriptor getSchemaDescriptor(String schemaName,
                                                TransactionController tc,
                                                boolean raiseError) throws StandardException{
        /*
        ** Check for SPLICE and SYS schemas before going any
        ** further.
        */

        if(tc==null){
            tc=getTransactionCompile();
        }

        if(getSystemSchemaDescriptor().getSchemaName().equals(schemaName)){
            return getSystemSchemaDescriptor();
        }else if(getSysIBMSchemaDescriptor().getSchemaName().equals(schemaName)){
            // oh you are really asking SYSIBM, if this db is soft upgraded 
            // from pre 52, I may have 2 versions for you, one on disk 
            // (user SYSIBM), one imaginary (builtin). The
            // one on disk (real one, if it exists), should always be used.
            if(dictionaryVersion.checkVersion(DataDictionary.DD_VERSION_CS_5_2,null)){
                return getSysIBMSchemaDescriptor();
            }
        }

        /*
        ** Manual lookup
        */

        SchemaDescriptor sd = dataDictionaryCache.schemaCacheFind(schemaName);
        if (sd!=null)
            return sd;

        sd=locateSchemaRow(schemaName,tc);

        if (sd!=null)
            dataDictionaryCache.schemaCacheAdd(schemaName,sd);
        //if no schema found and schema name is SESSION, then create an 
        //in-memory schema descriptor
        if(sd==null && getDeclaredGlobalTemporaryTablesSchemaDescriptor().getSchemaName().equals(schemaName)){
            return getDeclaredGlobalTemporaryTablesSchemaDescriptor();
        }

        if(sd==null && raiseError){
            throw StandardException.newException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST,schemaName);
        }else{
            return sd;
        }
    }

    /**
     * Get the target schema by searching for a matching row
     * in SYSSCHEMAS by schemaId.  Read only scan.
     *
     * @param schemaId       The id of the schema we're interested in.
     *                       If non-null, overrides schemaName
     * @param isolationLevel Use this explicit isolation level. Only
     *                       ISOLATION_REPEATABLE_READ (normal usage) or
     *                       ISOLATION_READ_UNCOMMITTED (corner cases)
     *                       supported for now.
     * @param tc             TransactionController.  If null, one
     *                       is gotten off of the language connection context.
     * @return The row for the schema
     * @throws StandardException Thrown on error
     */
    private SchemaDescriptor locateSchemaRow(UUID schemaId,
                                             int isolationLevel,
                                             TransactionController tc) throws StandardException{
        return locateSchemaRowBody(schemaId,isolationLevel,tc);
    }


    private SchemaDescriptor locateSchemaRowBody(UUID schemaId,
                                                 int isolationLevel,
                                                 TransactionController tc) throws StandardException{
        DataValueDescriptor UUIDStringOrderable;
        TabInfoImpl ti=coreInfo[SYSSCHEMAS_CORE_NUM];

        /* Use UUIDStringOrderable in both start and stop positions for scan */
        UUIDStringOrderable=getIDValueAsCHAR(schemaId);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,UUIDStringOrderable);

        return (SchemaDescriptor)
                getDescriptorViaIndex(
                        SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX2_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false,
                        isolationLevel,
                        tc);
    }

    /**
     * Get the target schema by searching for a matching row
     * in SYSSCHEMAS by schema name.  Read only scan.
     *
     * @param schemaName The name of the schema we're interested in.
     *                   If schemaId is null, used to qual.
     * @param tc         TransactionController.  If null, one
     *                   is gotten off of the language connection context.
     * @return The row for the schema
     * @throws StandardException Thrown on error
     */
    @Override
    public SchemaDescriptor locateSchemaRow(String schemaName,TransactionController tc) throws StandardException{
        DataValueDescriptor schemaNameOrderable;
        TabInfoImpl ti=coreInfo[SYSSCHEMAS_CORE_NUM];

        schemaNameOrderable=new SQLVarchar(schemaName);

        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,schemaNameOrderable);

        return (SchemaDescriptor)
                getDescriptorViaIndex(
                        SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false,
                        TransactionController.ISOLATION_REPEATABLE_READ,
                        tc);
    }

    /**
     * Get the SchemaDescriptor for the given schema identifier.
     *
     * @param schemaId The id of the schema we're interested in.
     * @param tc       The transaction controller to us when scanning
     *                 SYSSCHEMAS
     * @return The descriptor for the schema, null if no such schema exists.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SchemaDescriptor getSchemaDescriptor(UUID schemaId,TransactionController tc) throws StandardException{
        SchemaDescriptor sd = null;
        if (schemaId != null)
            sd = dataDictionaryCache.oidSchemaCacheFind(schemaId);
        if (sd != null)
            return sd;
        sd = getSchemaDescriptorBody(schemaId,TransactionController.ISOLATION_REPEATABLE_READ,tc);
        if (schemaId != null && sd != null)
            dataDictionaryCache.oidSchemaCacheAdd(schemaId, sd);
        return sd;
    }

    /**
     * Get the SchemaDescriptor for the given schema identifier.
     *
     * @param schemaId       the uuid of the schema we want a descriptor for
     * @param isolationLevel use this explicit isolation level. Only
     *                       ISOLATION_REPEATABLE_READ (normal usage) or
     *                       ISOLATION_READ_UNCOMMITTED (corner cases)
     *                       supported for now.
     * @param tc             transaction controller
     * @throws StandardException thrown on error
     */
    @Override
    public SchemaDescriptor getSchemaDescriptor(UUID schemaId,
                                                int isolationLevel,
                                                TransactionController tc) throws StandardException{
        return getSchemaDescriptorBody(schemaId,isolationLevel,tc);
    }


    private SchemaDescriptor getSchemaDescriptorBody(UUID schemaId,
                                                     int isolationLevel,
                                                     TransactionController tc) throws StandardException{
        SchemaDescriptor sd;

        if(tc==null){
            tc=getTransactionCompile();
        }

        /*
        ** Check for SPLICE and SYS schemas before going any
        ** further.
        */
        if(schemaId!=null){
            if(getSystemSchemaDescriptor().getUUID().equals(schemaId)){
                return getSystemSchemaDescriptor();
            }else if(getSysIBMSchemaDescriptor().getUUID().equals(schemaId)){
                return getSysIBMSchemaDescriptor();
            }
        }

        /*
        ** If we aren't booting, lets see if we already
        ** have the descriptor.  If we are in the middle
        ** of booting we cannot get the LanguageConnectionContext.
        */
        if(!booting){

            LanguageConnectionContext lcc=getLCC();

            if(lcc!=null){
                sd=lcc.getDefaultSchema();

                if((sd!=null) && ((schemaId==null) || schemaId.equals(sd.getUUID()))){
                    return sd;
                }
            }
        }

        return locateSchemaRow(schemaId,isolationLevel,tc);
    }


    /**
     * Return true of there exists a schema whose authorizationId equals
     * authid, i.e.  SYS.SYSSCHEMAS contains a row whose column
     * (AUTHORIZATIONID) equals authid.
     *
     * @param authid authorizationId
     * @param tc     TransactionController
     * @return true iff there is a matching schema
     * @throws StandardException
     */
    @Override
    public boolean existsSchemaOwnedBy(String authid,TransactionController tc) throws StandardException{
        TabInfoImpl ti=coreInfo[SYSSCHEMAS_CORE_NUM];
        SYSSCHEMASRowFactory rf=(SYSSCHEMASRowFactory)ti.getCatalogRowFactory();
        ConglomerateController heapCC=tc.openConglomerate(
                ti.getHeapConglomerate(),false,0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

        DataValueDescriptor authIdOrderable=new SQLVarchar(authid);
        ScanQualifier[][] scanQualifier=exFactory.getScanQualifier(1);

        scanQualifier[0][0].setQualifier(
                SYSSCHEMASRowFactory.SYSSCHEMAS_SCHEMAAID-1,    /* to zero-based */
                authIdOrderable,
                Orderable.ORDER_OP_EQUALS,
                false,
                false,
                false);


        boolean result=false;

        try(ScanController sc=tc.openScan(ti.getHeapConglomerate(),
                false,
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,
                null,
                0,
                scanQualifier,
                null,
                0)){
            ExecRow outRow=rf.makeEmptyRow();

            if(sc.fetchNext(outRow.getRowArray())){
                result=true;
            }
        }finally{
            if(heapCC!=null){
                heapCC.close();
            }
        }

        return result;
    }

    @Override
    public void addColumnStatistics(ExecRow row,
                              TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSCOLUMNSTATS_CATALOG_NUM);
        int insertRetCode = ti.insertRow(row,tc);
        if (insertRetCode != TabInfoImpl.ROWNOTDUPLICATE)
            throw StandardException.newException(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,
                    "SYSCOLUMNSTATISTICS_INDEX1", SYSCOLUMNSTATISTICSRowFactory.TABLENAME_STRING);
    }

    @Override
    public void addTableStatistics(ExecRow row,
                                    TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSTABLESTATS_CATALOG_NUM);
        int insertRetCode = ti.insertRow(row,tc);
        if (insertRetCode != TabInfoImpl.ROWNOTDUPLICATE)
            throw StandardException.newException(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,
                    "SYSTABLESTATS_INDEX1", SYSTABLESTATISTICSRowFactory.TABLENAME_STRING);
    }


    @Override
    public void addDescriptor(TupleDescriptor td,
                              TupleDescriptor parent,
                              int catalogNumber,
                              boolean duplicatesAllowed,
                              TransactionController tc,
                              boolean buildVersion2Descriptor) throws StandardException{
        TabInfoImpl ti=(catalogNumber<NUM_CORE)?coreInfo[catalogNumber]:getNonCoreTI(catalogNumber);

        ExecRow row=ti.getCatalogRowFactory().makeRow(td,parent);

        if (buildVersion2Descriptor && catalogNumber == DataDictionary.SYSTABLES_CATALOG_NUM)
        {
            row.setColumn(SYSTABLESRowFactory.SYSTABLES_VERSION,new SQLVarchar("2.0"));
        }
        int insertRetCode=ti.insertRow(row,tc);

        if(!duplicatesAllowed){
            if(insertRetCode!=TabInfoImpl.ROWNOTDUPLICATE)
                throw duplicateDescriptorException(td,parent);
        }
    }

    protected StandardException duplicateDescriptorException(TupleDescriptor tuple,TupleDescriptor parent){
        if(parent!=null)
            return StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                    tuple.getDescriptorType(),
                    tuple.getDescriptorName(),
                    parent.getDescriptorType(),
                    parent.getDescriptorName());

        else return StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
                tuple.getDescriptorType(),
                tuple.getDescriptorName());
    }

    @Override
    public void addDescriptorArray(TupleDescriptor[] td,
                                   TupleDescriptor parent,
                                   int catalogNumber,
                                   boolean allowDuplicates,
                                   TransactionController tc) throws StandardException{
        TabInfoImpl ti=(catalogNumber<NUM_CORE)?coreInfo[catalogNumber]:getNonCoreTI(catalogNumber);
        CatalogRowFactory crf=ti.getCatalogRowFactory();

        ExecRow[] rl=new ExecRow[td.length];

        for(int index=0;index<td.length;index++){
            ExecRow row=crf.makeRow(td[index],parent);
            rl[index]=row;
        }

        int insertRetCode=ti.insertRowList(rl,tc);
        if(!allowDuplicates && insertRetCode!=TabInfoImpl.ROWNOTDUPLICATE){
            throw duplicateDescriptorException(td[insertRetCode],parent);
        }
    }

    @Override
    public void dropRoleGrant(String roleName,
                              String grantee,
                              String grantor,
                              TransactionController tc) throws StandardException{
        DataValueDescriptor roleNameOrderable;
        DataValueDescriptor granteeOrderable;
        DataValueDescriptor grantorOrderable;

        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);

        roleNameOrderable=new SQLVarchar(roleName);
        granteeOrderable=new SQLVarchar(grantee);
        grantorOrderable=new SQLVarchar(grantor);

        ExecIndexRow keyRow;

        /* Set up the start/stop position for the scan */
        keyRow=exFactory.getIndexableRow(3);
        keyRow.setColumn(1,roleNameOrderable);
        keyRow.setColumn(2,granteeOrderable);
        keyRow.setColumn(3,grantorOrderable);

        ti.deleteRow(tc,keyRow,SYSROLESRowFactory.SYSROLES_INDEX_ID_EE_OR_IDX);
    }


    @Override
    public void deletePartitionStatistics(long conglomerate,
                                          TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSTABLESTATS_CATALOG_NUM);
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1, new SQLLongint(conglomerate));
        ti.deleteRow(tc,keyRow, SYSTABLESTATISTICSRowFactory.SYSTABLESTATISTICS_INDEX3_ID);
        deleteColumnStatistics(conglomerate,tc);
    }

    @Override
    public void deleteColumnStatistics(long conglomerate,
                                      TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSCOLUMNSTATS_CATALOG_NUM);
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,new SQLLongint(conglomerate));
        ti.deleteRow(tc,keyRow,SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_INDEX3_ID);
    }

    @Override
    public void deleteColumnStatisticsByColumnId(long conglomerate,
                                                 int columnId,
                                                 TransactionController tc) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSCOLUMNSTATS_CATALOG_NUM);
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,new SQLLongint(conglomerate));

        // only drop the rows which have the given columnId
        TupleFilter filter=new ColumnStatsFilter(columnId);

        ti.deleteRows(tc,
                keyRow,                // start row
                ScanController.GE,
                null,                //qualifier
                filter,                // filter on base row
                keyRow,                // stop row
                ScanController.GT,
                SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_INDEX3_ID);
    }

    @Override
    public List<PartitionStatisticsDescriptor> getPartitionStatistics(long conglomerate, TransactionController tc) throws StandardException {
        List<PartitionStatisticsDescriptor> partitionStatisticsDescriptors = dataDictionaryCache.partitionStatisticsCacheFind(conglomerate);
        if (partitionStatisticsDescriptors!=null)
            return partitionStatisticsDescriptors;
        partitionStatisticsDescriptors = new ArrayList<>();
        TabInfoImpl ti=getNonCoreTI(SYSTABLESTATS_CATALOG_NUM);
        DataValueDescriptor startStop =new SQLLongint(conglomerate);
        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1, startStop);
        getDescriptorViaIndex(
                SYSTABLESTATISTICSRowFactory.SYSTABLESTATISTICS_INDEX3_ID,
                keyRow,
                null,
                ti,
                null,
                partitionStatisticsDescriptors,
                false);
        List<ColumnStatisticsDescriptor> columnStats = getColumnStatistics(conglomerate, tc);
        ImmutableListMultimap columnStatsMap = Multimaps.index(columnStats, new Function<ColumnStatisticsDescriptor, String>() {
            @Override
            public String apply(@Nullable ColumnStatisticsDescriptor input) {
                assert input != null;
                return input.getPartitionId();
            }
        });
        for (PartitionStatisticsDescriptor desc: partitionStatisticsDescriptors) {
            desc.setColumnStatsDescriptors(columnStatsMap.get(desc.getPartitionId()));
        }
        dataDictionaryCache.partitionStatisticsCacheAdd(conglomerate,partitionStatisticsDescriptors);
        return partitionStatisticsDescriptors;
    }

    @Override
    public List<ColumnStatisticsDescriptor> getColumnStatistics(long conglomerate, TransactionController tc) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSCOLUMNSTATS_CATALOG_NUM);
        DataValueDescriptor startStop =new SQLLongint(conglomerate);
        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1, startStop);
        List<ColumnStatisticsDescriptor> columnStatisticsDescriptors = new ArrayList();
        getDescriptorViaIndex(
                SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_INDEX1_ID,
                keyRow,
                null,
                ti,
                null,
                columnStatisticsDescriptors,
                false);
        return columnStatisticsDescriptors;
    }

    /**
     * Drop the descriptor for a schema, given the schema's name
     *
     * @param schemaName The name of the schema to drop
     * @param tc         TransactionController for the transaction
     * @throws StandardException Thrown on error
     */
    @Override
    public void dropSchemaDescriptor(String schemaName,TransactionController tc) throws StandardException{
        ExecIndexRow keyRow1;
        DataValueDescriptor schemaNameOrderable;
        TabInfoImpl ti=coreInfo[SYSSCHEMAS_CORE_NUM];

        if(SanityManager.DEBUG){
            SchemaDescriptor sd=getSchemaDescriptor(schemaName,getTransactionCompile(),true);
            if(!isSchemaEmpty(sd)){
                SanityManager.THROWASSERT("Attempt to drop schema "+schemaName+" that is not empty");
            }
        }

        /* Use schemaNameOrderable in both start
         * and stop position for index 1 scan.
         */
        schemaNameOrderable=new SQLVarchar(schemaName);

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(1);
        keyRow1.setColumn(1,schemaNameOrderable);

        ti.deleteRow(tc,keyRow1,SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID);
    }

    /**
     * Get the descriptor for the named table within the given schema.
     * If the schema parameter is NULL, it looks for the table in the
     * current (default) schema. Table descriptors include object ids,
     * object types (table, view, etc.)
     *
     * @param tableName The name of the table to get the descriptor for
     * @param schema    The descriptor for the schema the table lives in.
     *                  If null, use the system schema.
     * @return The descriptor for the table, null if table does not
     * exist.
     * @throws StandardException Thrown on failure
     */
    @Override
    public TableDescriptor getTableDescriptor(String tableName,SchemaDescriptor schema,TransactionController tc) throws StandardException{
        TableDescriptor retval=null;

        /*
        ** If we didn't get a schema descriptor, we had better
        ** have a system table.
        */
        if(SanityManager.DEBUG){
            if((schema==null) && !tableName.startsWith("SYS")){
                SanityManager.THROWASSERT("null schema for non system table "+tableName);
            }
        }

        SchemaDescriptor sd=(schema==null)?getSystemSchemaDescriptor():schema;

        UUID schemaUUID=sd.getUUID();

        if(SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME.equals(
                sd.getSchemaName())){
            TableDescriptor td=new TableDescriptor(this,tableName,sd,TableDescriptor.VTI_TYPE,TableDescriptor.DEFAULT_LOCK_GRANULARITY,-1,
                    null,null,null,null,null,null, false,false, null);

            // ensure a vti class exists
            if(getVTIClass(td,false)!=null)
                return td;

            // otherwise just standard search
        }

        TableKey tableKey=new TableKey(schemaUUID,tableName);

        retval = dataDictionaryCache.nameTdCacheFind(tableKey);
        if (retval != null) {
                retval.setReferencedColumnMap(null);
                if (retval.getSchemaDescriptor() == null) {
                    retval.setSchemaDesctiptor(sd);
                }
            return retval;
        }
        if (schemaUUID == null)
            throw StandardException.newException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST, sd.getSchemaName());
        retval = getTableDescriptorIndex1Scan(tableName,schemaUUID.toString());
        if (retval!=null) {
            dataDictionaryCache.nameTdCacheAdd(tableKey, retval);
        }
        return retval;
    }

    /**
     * Scan systables_index1 (tablename, schemaid) for a match.
     *
     * @return TableDescriptor    The matching descriptor, if any.
     * @throws StandardException Thrown on failure
     */
    private TableDescriptor getTableDescriptorIndex1Scan(String tableName,String schemaUUID) throws StandardException{
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor tableNameOrderable;
        TableDescriptor td;
        TabInfoImpl ti=coreInfo[SYSTABLES_CORE_NUM];

        /* Use tableNameOrderable and schemaIdOrderable in both start
         * and stop position for scan.
         */
        tableNameOrderable=new SQLVarchar(tableName);
        schemaIDOrderable=new SQLChar(schemaUUID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,tableNameOrderable);
        keyRow.setColumn(2,schemaIDOrderable);

        td=(TableDescriptor)getDescriptorViaIndex(
                SYSTABLESRowFactory.SYSTABLES_INDEX1_ID,
                keyRow,
                null,
                ti,
                null,
                null,
                false);

        return finishTableDescriptor(td);
    }

    /**
     * This method can get called from the DataDictionary cache.
     *
     * @param tableId The UUID of the table
     * @return The descriptor for the table, null if the table does
     * not exist.
     * @throws StandardException Thrown on failure
     */
    public TableDescriptor getUncachedTableDescriptor(UUID tableId) throws StandardException{
        return getTableDescriptorIndex2Scan(tableId.toString());
    }

    /**
     * Get the descriptor for the table with the given UUID.
     * <p/>
     * NOTE: I'm assuming that the object store will define an UUID for
     * persistent objects. I'm also assuming that UUIDs are unique across
     * schemas, and that the object store will be able to do efficient
     * lookups across schemas (i.e. that no schema descriptor parameter
     * is needed).
     *
     * @param tableID The UUID of the table to get the descriptor for
     * @return The descriptor for the table, null if the table does
     * not exist.
     * @throws StandardException Thrown on failure
     */
    @Override
    public TableDescriptor getTableDescriptor(UUID tableID) throws StandardException{
        TableDescriptor td = dataDictionaryCache.oidTdCacheFind(tableID);
        if (td != null)
            return td;
        td = getTableDescriptorIndex2Scan(tableID.toString());
        if (td != null)
            dataDictionaryCache.oidTdCacheAdd(tableID,td);
        return td;
    }

    /**
     * Scan systables_index2 (tableid) for a match.
     *
     * @return TableDescriptor    The matching descriptor, if any.
     * @throws StandardException Thrown on failure
     */
    private TableDescriptor getTableDescriptorIndex2Scan(String tableUUID) throws StandardException{
        DataValueDescriptor tableIDOrderable;
        TableDescriptor td;
        TabInfoImpl ti=coreInfo[SYSTABLES_CORE_NUM];

        /* Use tableIDOrderable in both start and stop position for scan.
         */
        tableIDOrderable=new SQLChar(tableUUID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,tableIDOrderable);

        td=(TableDescriptor)
                getDescriptorViaIndex(
                        SYSTABLESRowFactory.SYSTABLES_INDEX2_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false);

        return finishTableDescriptor(td);
    }

    protected void markSystemTablesAsVersion1(TableDescriptor td) {
        if(td.getSchemaName() != null && td.getSchemaName().equals(SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME)) {
            td.setVersion(SYSTABLESRowFactory.ORIGINAL_TABLE_VERSION);
        }
    }

    /**
     * Finish filling in the TableDescriptor.
     * (Build the various lists that hang off the TD.)
     *
     * @param td The TableDescriptor.
     * @return The completed TableDescriptor.
     * @throws StandardException Thrown on failure
     */
    private TableDescriptor finishTableDescriptor(TableDescriptor td) throws StandardException{

        if(td!=null){
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized(td){
                getColumnDescriptorsScan(td);
                getConglomerateDescriptorsScan(td);
                markSystemTablesAsVersion1(td);
                td.getHeapConglomerateId(); // populate heapConglomerateId
             }
        }

        return td;
    }

    /**
     * Indicate whether there is anything in the
     * particular schema.  Checks for tables in the
     * the schema, on the assumption that there cannot
     * be any other objects in a schema w/o a table.
     *
     * @param sd descriptor
     * @return true/false
     * @throws StandardException on error
     */
    @Override
    public boolean isSchemaEmpty(SchemaDescriptor sd) throws StandardException{
        DataValueDescriptor schemaIdOrderable;
        TransactionController tc=getTransactionCompile();

        schemaIdOrderable=getIDValueAsCHAR(sd.getUUID());

        if(isSchemaReferenced(tc,coreInfo[SYSTABLES_CORE_NUM],
                SYSTABLESRowFactory.SYSTABLES_INDEX1_ID,
                SYSTABLESRowFactory.SYSTABLES_INDEX1_SCHEMAID,
                schemaIdOrderable)){
            return false;
        }

        if(isSchemaReferenced(tc,getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM),
                SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX2_ID,
                2,
                schemaIdOrderable)){
            return false;
        }

        if(isSchemaReferenced(tc,getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM),
                SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX2_ID,
                2,
                schemaIdOrderable)){
            return false;
        }

        if(isSchemaReferenced(tc,getNonCoreTI(SYSTRIGGERS_CATALOG_NUM),
                SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX2_ID,
                2,
                schemaIdOrderable)){
            return false;
        }

        // don't orphan routines or UDTs
        if(isSchemaReferenced(tc,getNonCoreTI(SYSALIASES_CATALOG_NUM),
                SYSALIASESRowFactory.SYSALIASES_INDEX1_ID,
                1,
                schemaIdOrderable)){
            return false;
        }

        // don't orphan Files
        if(isSchemaReferenced(tc,getNonCoreTI(SYSFILES_CATALOG_NUM),
                SYSFILESRowFactory.SYSFILES_INDEX1_ID,
                2,
                schemaIdOrderable)){
            return false;
        }


        // These catalogs were added in 10.6. Don't look for these catalogs if we
        // have soft-upgraded from an older release.
        if(dictionaryVersion.majorVersionNumber>=DataDictionary.DD_VERSION_DERBY_10_6){
            if(isSchemaReferenced(tc,getNonCoreTI(SYSSEQUENCES_CATALOG_NUM),
                    SYSSEQUENCESRowFactory.SYSSEQUENCES_INDEX2_ID,
                    1,
                    schemaIdOrderable)){
                return false;
            }
        }

        return true;
    }

    /**
     * Is the schema id referenced by the system table in question?
     * Currently assumes that the schema id is in an index.
     * NOTE: could be generalized a bit, and possibly used
     * elsewhere...
     *
     * @param tc                transaction controller
     * @param ti                table info for the system table
     * @param indexId           index id
     * @param indexCol          1 based index column
     * @param schemaIdOrderable the schemaid in a char orderable
     * @return true if there is a reference to this schema
     * @throws StandardException on error
     */
    protected boolean isSchemaReferenced(TransactionController tc,
                                         TabInfoImpl ti,
                                         int indexId,
                                         int indexCol,
                                         DataValueDescriptor schemaIdOrderable) throws StandardException{
        ConglomerateController heapCC=null;
        ScanController scanController=null;
        boolean foundRow;
        FormatableBitSet colToCheck=new FormatableBitSet(indexCol);

        assert indexId>=0:"Programmer error: code needs to be enhanced to support a table scan to find the index id";

        colToCheck.set(indexCol-1);

        ScanQualifier[][] qualifier=exFactory.getScanQualifier(1);
        qualifier[0][0].setQualifier
                (indexCol-1,
                        schemaIdOrderable,
                        Orderable.ORDER_OP_EQUALS,
                        false,
                        false,
                        false);

        try{
            heapCC=tc.openConglomerate(
                    ti.getHeapConglomerate(),false,0,
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

            scanController=tc.openScan(
                    ti.getIndexConglomerate(indexId),    // conglomerate to open
                    false,                                // don't hold open across commit
                    0,                                  // for read
                    TransactionController.MODE_RECORD,    // row locking
                    TransactionController.ISOLATION_REPEATABLE_READ,
                    colToCheck,                        // don't get any rows
                    null,                            // start position - first row
                    ScanController.GE,                // startSearchOperation
                    qualifier,                            // scanQualifier,
                    null,                            // stop position - through last row
                    ScanController.GT);                // stopSearchOperation

            foundRow=(scanController.next());
        }finally{
            if(scanController!=null){
                scanController.close();
            }
            if(heapCC!=null){
                heapCC.close();
            }
        }

        return foundRow;
    }

    /**
     * Drop the table descriptor.
     *
     * @param td     The table descriptor to drop
     * @param schema A descriptor for the schema the table
     *               is a part of.  If this parameter is
     *               NULL, then the table is part of the
     *               current (default) schema
     * @param tc     TransactionController for the transaction
     * @throws StandardException Thrown on error
     */
    @Override
    public void dropTableDescriptor(TableDescriptor td,SchemaDescriptor schema,TransactionController tc) throws StandardException{
        ExecIndexRow keyRow1;
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor tableNameOrderable;
        TabInfoImpl ti=coreInfo[SYSTABLES_CORE_NUM];

        /* Use tableIdOrderable and schemaIdOrderable in both start
         * and stop position for index 1 scan.
         */
        tableNameOrderable=new SQLVarchar(td.getName());
        schemaIDOrderable=getIDValueAsCHAR(schema.getUUID());

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(2);
        keyRow1.setColumn(1,tableNameOrderable);
        keyRow1.setColumn(2,schemaIDOrderable);

        ti.deleteRow(tc,keyRow1,SYSTABLESRowFactory.SYSTABLES_INDEX1_ID);
    }

    /**
     * Update the lockGranularity for the specified table.
     *
     * @param td              The TableDescriptor for the table
     * @param schema          The SchemaDescriptor for the table
     * @param lockGranularity The new lockGranularity
     * @param tc              The TransactionController to use.
     * @throws StandardException Thrown on error
     */
    @Override
    public void updateLockGranularity(TableDescriptor td,SchemaDescriptor schema,char lockGranularity,TransactionController tc) throws StandardException{
        ExecIndexRow keyRow1;
        ExecRow row;
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor tableNameOrderable;
        TabInfoImpl ti=coreInfo[SYSTABLES_CORE_NUM];
        SYSTABLESRowFactory rf=(SYSTABLESRowFactory)ti.getCatalogRowFactory();

        /* Use tableIdOrderable and schemaIdOrderable in both start
         * and stop position for index 1 scan.
         */
        tableNameOrderable=new SQLVarchar(td.getName());
        schemaIDOrderable=getIDValueAsCHAR(schema.getUUID());

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(2);
        keyRow1.setColumn(1,tableNameOrderable);
        keyRow1.setColumn(2,schemaIDOrderable);

        // build the row to be stuffed into SYSTABLES. 
        row=rf.makeRow(td,schema);
        // update row in catalog (no indexes)
        boolean[] bArray=new boolean[2];
        for(int index=0;index<2;index++){
            bArray[index]=false;
        }
        ti.updateRow(keyRow1,row,SYSTABLESRowFactory.SYSTABLES_INDEX1_ID,bArray,null,tc);
    }

    /**
     * 10.6 upgrade logic to update the return type of SYSIBM.CLOBGETSUBSTRING. The length of the
     * return type was changed in 10.5 but old versions of the metadata were not
     * upgraded at that time. See DERBY-4214.
     */
    void upgradeCLOBGETSUBSTRING_10_6(TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSALIASES_CATALOG_NUM);
        ExecIndexRow keyRow=exFactory.getIndexableRow(3);
        DataValueDescriptor aliasNameOrderable=new SQLVarchar("CLOBGETSUBSTRING");
        DataValueDescriptor nameSpaceOrderable=new SQLChar(new String(new char[]{AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR}));

        keyRow.setColumn(1,new SQLChar(SchemaDescriptor.SYSIBM_SCHEMA_UUID));
        keyRow.setColumn(2,aliasNameOrderable);
        keyRow.setColumn(3,nameSpaceOrderable);

        AliasDescriptor oldAD=(AliasDescriptor)getDescriptorViaIndex(
                SYSALIASESRowFactory.SYSALIASES_INDEX1_ID,
                keyRow,
                null,
                ti,
                null,
                null,
                true,
                TransactionController.ISOLATION_REPEATABLE_READ,
                tc);
        RoutineAliasInfo oldRai=(RoutineAliasInfo)oldAD.getAliasInfo();
        TypeDescriptor newReturnType=DataTypeDescriptor.getCatalogType(Types.VARCHAR,Limits.MAX_CLOB_RETURN_LEN);
        RoutineAliasInfo newRai=new RoutineAliasInfo(
                oldRai.getMethodName(),
                oldRai.getLanguage(),
                oldRai.getParameterCount(),
                oldRai.getParameterNames(),
                oldRai.getParameterTypes(),
                oldRai.getParameterModes(),
                oldRai.getMaxDynamicResultSets(),
                oldRai.getParameterStyle(),
                oldRai.getSQLAllowed(),
                oldRai.isDeterministic(),
                oldRai.hasDefinersRights(),
                oldRai.calledOnNullInput(),
                newReturnType,
                null
        );
        AliasDescriptor newAD=new AliasDescriptor(
                this,
                oldAD.getUUID(),
                oldAD.getObjectName(),
                oldAD.getSchemaUUID(),
                oldAD.getJavaClassName(),
                oldAD.getAliasType(),
                oldAD.getNameSpace(),
                oldAD.getSystemAlias(),
                newRai,
                oldAD.getSpecificName()
        );
        ExecRow newRow=ti.getCatalogRowFactory().makeRow(newAD,null);

        ti.updateRow(keyRow,newRow,SYSALIASESRowFactory.SYSALIASES_INDEX1_ID,new boolean[]{false,false,false},null,tc);
    }

    /**
     * 10.6 upgrade logic to update the permissions granted to SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE.
     * If a 10.0 database was upgraded to 10.2, 10.3, or 10.4, then there will
     * be an extra permissions tuple in SYSROUTINEPERMS--that tuple will have a
     * null grantor field. We must delete this tuple. See DERBY-4215.
     */
    void upgradeSYSROUTINEPERMS_10_6(TransactionController tc) throws StandardException{
        //
        // Get the aliasID of SYSCS_INPLACE_COMPRESS_TABLE
        //
        TabInfoImpl aliasTI=getNonCoreTI(SYSALIASES_CATALOG_NUM);
        ExecIndexRow aliasKeyRow=exFactory.getIndexableRow(3);
        DataValueDescriptor aliasNameOrderable=new SQLVarchar("SYSCS_INPLACE_COMPRESS_TABLE");
        DataValueDescriptor nameSpaceOrderable=new SQLChar(new String(new char[]{AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR}));

        aliasKeyRow.setColumn(1,new SQLChar(SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID));
        aliasKeyRow.setColumn(2,aliasNameOrderable);
        aliasKeyRow.setColumn(3,nameSpaceOrderable);

        AliasDescriptor oldAD=(AliasDescriptor)getDescriptorViaIndex(
                SYSALIASESRowFactory.SYSALIASES_INDEX1_ID,
                aliasKeyRow,
                null,
                aliasTI,
                null,
                null,
                true,
                TransactionController.ISOLATION_REPEATABLE_READ,
                tc);
        UUID aliasID=oldAD.getUUID();

        //
        // Now delete the permissions tuple which has a null grantor
        //
        TabInfoImpl rpTI=getNonCoreTI(SYSROUTINEPERMS_CATALOG_NUM);
        ExecIndexRow rpKeyRow=exFactory.getIndexableRow(3);

        rpKeyRow.setColumn(1,new SQLVarchar("PUBLIC"));
        rpKeyRow.setColumn(2,new SQLChar(aliasID.toString()));
        rpKeyRow.setColumn(3,new SQLVarchar((String)null));

        rpTI.deleteRow(tc,rpKeyRow,SYSROUTINEPERMSRowFactory.GRANTEE_ALIAS_GRANTOR_INDEX_NUM);
    }

    /**
     * Drop all table descriptors for a schema.
     *
     * @param schema    A descriptor for the schema to drop the tables
     *            from.
     *
     * @return Nothing.
     *
     * @exception StandardException        Thrown on failure
     */
    /*
    public void dropAllTableDescriptors(SchemaDescriptor schema)
                        throws StandardException
    {
        if (SanityManager.DEBUG) SanityManager.NOTREACHED();
    }
    */

    /**
     * Get a ColumnDescriptor given its Default ID.
     *
     * @param uuid The UUID of the default
     * @return The ColumnDescriptor for the column.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ColumnDescriptor getColumnDescriptorByDefaultId(UUID uuid) throws StandardException{
        DataValueDescriptor UUIDStringOrderable;
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];

        /* Use UUIDStringOrderable in both start and stop positions for scan */
        UUIDStringOrderable=getIDValueAsCHAR(uuid);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,UUIDStringOrderable);

        return (ColumnDescriptor)getDescriptorViaIndex(SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX2_ID,keyRow,null,ti,null,null,false);
    }


    /**
     * Populate the ColumnDescriptorList for the specified TableDescriptor.
     * <p/>
     * MT synchronization: it is assumed that the caller has synchronized
     * on the CDL in the given TD.
     *
     * @param td The TableDescriptor.
     * @throws StandardException Thrown on failure
     */
    private void getColumnDescriptorsScan(TableDescriptor td) throws StandardException{
        getColumnDescriptorsScan(td.getUUID(),td.getColumnDescriptorList(),td);
    }

    /**
     * Populate the ColumnDescriptorList for the specified TableDescriptor.
     * <p/>
     * MT synchronization: it is assumed that the caller has synchronized
     * on the CDL in the given TD.
     *
     * @param uuid The referencing UUID
     * @param cdl  The column descriptor list
     * @param td   The parent tuple descriptor
     * @throws StandardException Thrown on failure
     */
    private void getColumnDescriptorsScan(UUID uuid,ColumnDescriptorList cdl,TupleDescriptor td) throws StandardException{
        DataValueDescriptor refIDOrderable;
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];

        /* Use refIDOrderable in both start and stop position for scan. */
        refIDOrderable=getIDValueAsCHAR(uuid);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,refIDOrderable);

        getDescriptorViaIndex(
                SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,
                keyRow,
                null,
                ti,
                td,
                cdl,
                false);

        /* The TableDescriptor's column descriptor list must be ordered by
         * columnNumber.  (It is probably not ordered correctly at this point due
         * to the index on syscolumns being on (tableId, columnName).)  The
         * cheapest way to reorder the list appears to be to copy it (above), and then
         * walk the copy and put the elements back into the original in the
         * expected locations.
         */

        Collections.sort(cdl, Comparator.comparingInt(ColumnDescriptor::getPosition));
    }

    /**
     * Given a column name and a table ID, drops the column descriptor
     * from the table.
     *
     * @param tableID    The UUID of the table to drop the column from
     * @param columnName The name of the column to drop
     * @param tc         TransactionController for the transaction
     * @throws StandardException Thrown on error
     */
    @Override
    public void dropColumnDescriptor(UUID tableID,String columnName,TransactionController tc) throws StandardException{
        DataValueDescriptor columnNameOrderable;
        DataValueDescriptor tableIdOrderable;

        /* Use tableIDOrderable and columnNameOrderable in both start
         * and stop position for scan.
         */
        tableIdOrderable=getIDValueAsCHAR(tableID);
        columnNameOrderable=new SQLVarchar(columnName);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,tableIdOrderable);
        keyRow.setColumn(2,columnNameOrderable);

        dropColumnDescriptorCore(tc,keyRow);
    }

    /**
     * Drops all column descriptors from the given table.  Useful for
     * DROP TABLE.
     *
     * @param tableID The UUID of the table from which to drop
     *                all the column descriptors
     * @param tc      TransactionController for the transaction
     * @throws StandardException Thrown on error
     */
    @Override
    public void dropAllColumnDescriptors(UUID tableID,TransactionController tc) throws StandardException{
        DataValueDescriptor tableIdOrderable;

        /* Use tableIDOrderable in both start and stop position for scan. */
        tableIdOrderable=getIDValueAsCHAR(tableID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,tableIdOrderable);

        dropColumnDescriptorCore(tc,keyRow);
    }

    /**
     * Drops all table and column permission descriptors for the given table.
     *
     * @param tableID The UUID of the table from which to drop
     *                all the permission descriptors
     * @param tc      TransactionController for the transaction
     * @throws StandardException Thrown on error
     */
    @Override
    public void dropAllTableAndColPermDescriptors(UUID tableID,TransactionController tc) throws StandardException{
        DataValueDescriptor tableIdOrderable;

        // In Derby authorization mode, permission catalogs may not be present
        if(!usesSqlAuthorization)
            return;

        /* Use tableIDOrderable in both start and stop position for scan. */
        tableIdOrderable=getIDValueAsCHAR(tableID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,tableIdOrderable);

        dropTablePermDescriptor(tc,keyRow);
        dropColumnPermDescriptor(tc,keyRow);
    }

    /**
     * Drop all the schemas descriptors for a specific schema.
     * Useful for DROP SCHEMA
     * @param schemaUUID
     * @param tc
     * @throws StandardException
     */

    @Override
    public void dropAllSchemaPermDescriptors(UUID schemaUUID,TransactionController tc) throws StandardException{
        DataValueDescriptor schemaIdOrderable;

        // In Derby authorization mode, permission catalogs may not be present
        if(!usesSqlAuthorization)
            return;

        /* Use schemaIDOrderable in both start and stop position for scan. */
        schemaIdOrderable=getIDValueAsCHAR(schemaUUID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,schemaIdOrderable);

        dropSchemaPermDescriptor(tc,keyRow);

    }


    /**
     * Need to update SYSCOLPERMS for a given table because a new column has
     * been added to that table. SYSCOLPERMS has a column called "COLUMNS"
     * which is a bit map for all the columns in a given user table. Since
     * ALTER TABLE .. ADD COLUMN .. has added one more column, we need to
     * expand "COLUMNS" for that new column
     * <p/>
     * Currently, this code gets called during execution phase of
     * ALTER TABLE .. ADD COLUMN ..
     *
     * @param tableID The UUID of the table to which a column has been added
     * @param tc      TransactionController for the transaction
     * @throws StandardException Thrown on error
     */
    @Override
    public void updateSYSCOLPERMSforAddColumnToUserTable(UUID tableID,TransactionController tc) throws StandardException{
        rewriteSYSCOLPERMSforAlterTable(tableID,tc,null);
    }

    /**
     * Update SYSCOLPERMS due to dropping a column from a table.
     * <p/>
     * Since ALTER TABLE .. DROP COLUMN .. has removed a column from the
     * table, we need to shrink COLUMNS by removing the corresponding bit
     * position, and shifting all the subsequent bits "left" one position.
     *
     * @param tableID          The UUID of the table from which a col has been dropped
     * @param tc               TransactionController for the transaction
     * @param columnDescriptor Information about the dropped column
     * @throws StandardException Thrown on error
     */
    @Override
    public void updateSYSCOLPERMSforDropColumn(UUID tableID,
                                               TransactionController tc,
                                               ColumnDescriptor columnDescriptor) throws StandardException{
        rewriteSYSCOLPERMSforAlterTable(tableID,tc,columnDescriptor);
    }

    /**
     * Workhorse for ALTER TABLE-driven mods to SYSCOLPERMS
     * <p/>
     * This method finds all the SYSCOLPERMS rows for this table. Then it
     * iterates through each row, either adding a new column to the end of
     * the table, or dropping a column from the table, as appropriate. It
     * updates each SYSCOLPERMS row to store the new COLUMNS value.
     *
     * @param tableID          The UUID of the table being altered
     * @param tc               TransactionController for the transaction
     * @param columnDescriptor Dropped column info, or null if adding
     * @throws StandardException Thrown on error
     */
    private void rewriteSYSCOLPERMSforAlterTable(UUID tableID,
                                                 TransactionController tc,
                                                 ColumnDescriptor columnDescriptor) throws StandardException{
        // In Derby authorization mode, permission catalogs may not be present
        if(!usesSqlAuthorization)
            return;

        /* This method has 2 steps to it. First get all the ColPermsDescriptor
        for given tableid. And next step is to go back to SYSCOLPERMS to find
        unique row corresponding to each of ColPermsDescriptor and update the
        "COLUMNS" column in SYSCOLPERMS. The reason for this 2 step process is
        that SYSCOLPERMS has a non-unique row on "TABLEID" column and hence
        we can't get a unique handle on each of the affected row in SYSCOLPERMS
        using just the "TABLEID" column */

        // First get all the ColPermsDescriptor for the given tableid from   
        //SYSCOLPERMS using getDescriptorViaIndex(). 
        List<ColPermsDescriptor> permissionDescriptorsList;//all ColPermsDescriptor for given tableid
        DataValueDescriptor tableIDOrderable=getIDValueAsCHAR(tableID);
        TabInfoImpl ti=getNonCoreTI(SYSCOLPERMS_CATALOG_NUM);
        SYSCOLPERMSRowFactory rf=(SYSCOLPERMSRowFactory)ti.getCatalogRowFactory();
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,tableIDOrderable);
        permissionDescriptorsList=newSList();
        getDescriptorViaIndex(SYSCOLPERMSRowFactory.TABLEID_INDEX_NUM,
                keyRow,
                null,
                ti,
                null,
                permissionDescriptorsList,
                false);

        /* Next, using each of the ColPermDescriptor's uuid, get the unique row
        in SYSCOLPERMS and adjust the "COLUMNS" column in SYSCOLPERMS to
        accomodate the added or dropped column in the tableid*/
        ColPermsDescriptor colPermsDescriptor;
        ExecRow curRow;
        ExecIndexRow uuidKey;
        // Not updating any indexes on SYSCOLPERMS
        boolean[] bArray=new boolean[SYSCOLPERMSRowFactory.TOTAL_NUM_OF_INDEXES];
        int[] colsToUpdate={SYSCOLPERMSRowFactory.COLUMNS_COL_NUM};
        for(ColPermsDescriptor aPermissionDescriptorsList : permissionDescriptorsList){
            colPermsDescriptor=aPermissionDescriptorsList;
            removePermEntryInCache(colPermsDescriptor);
            uuidKey=rf.buildIndexKeyRow(SYSCOLPERMSRowFactory.COLPERMSID_INDEX_NUM,colPermsDescriptor);
            curRow=ti.getRow(tc,uuidKey,SYSCOLPERMSRowFactory.COLPERMSID_INDEX_NUM);
            FormatableBitSet columns=(FormatableBitSet)curRow.getColumn(
                    SYSCOLPERMSRowFactory.COLUMNS_COL_NUM).getObject();
            // See whether this is ADD COLUMN or DROP COLUMN. If ADD, then
            // add a new bit to the bit set. If DROP, then remove the bit
            // for the dropped column.
            if(columnDescriptor==null){
                int currentLength=columns.getLength();
                columns.grow(currentLength+1);
            }else{
                FormatableBitSet modifiedColumns=new FormatableBitSet(columns);
                modifiedColumns.shrink(columns.getLength()-1);
                // All the bits from 0 ... colPosition-2 are OK. The bits from
                // colPosition to the end need to be shifted 1 to the left.
                // The bit for colPosition-1 simply disappears from COLUMNS.
                // ColumnPosition values count from 1, while bits in the
                // FormatableBitSet count from 0.
                for(int i=columnDescriptor.getPosition()-1;
                    i<modifiedColumns.getLength();
                    i++){
                    if(columns.isSet(i+1))
                        modifiedColumns.set(i);
                    else
                        modifiedColumns.clear(i);
                }
                columns=modifiedColumns;
            }
            curRow.setColumn(SYSCOLPERMSRowFactory.COLUMNS_COL_NUM,new UserType(columns));
            ti.updateRow(uuidKey,curRow,
                    SYSCOLPERMSRowFactory.COLPERMSID_INDEX_NUM,
                    bArray,
                    colsToUpdate,
                    tc);
        }
    }


    /**
     * Remove PermissionsDescriptor from permissions cache if present
     */
    private void removePermEntryInCache(PermissionsDescriptor perm) throws StandardException{
        // Remove cached permissions entry if present
        dataDictionaryCache.permissionCacheRemove(perm);
    }

    /**
     * Drops all routine permission descriptors for the given routine.
     *
     * @param routineID The UUID of the routine from which to drop
     *                  all the permission descriptors
     * @param tc        TransactionController for the transaction
     * @throws StandardException Thrown on error
     */
    @Override
    public void dropAllRoutinePermDescriptors(UUID routineID,TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSROUTINEPERMS_CATALOG_NUM);
        SYSROUTINEPERMSRowFactory rf=(SYSROUTINEPERMSRowFactory)ti.getCatalogRowFactory();
        DataValueDescriptor routineIdOrderable;
        ExecRow curRow;
        PermissionsDescriptor perm;

        // In Derby authorization mode, permission catalogs may not be present
        if(!usesSqlAuthorization)
            return;

        /* Use tableIDOrderable in both start and stop position for scan. */
        routineIdOrderable=getIDValueAsCHAR(routineID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,routineIdOrderable);

        while((curRow=ti.getRow(tc,keyRow,SYSROUTINEPERMSRowFactory.ALIASID_INDEX_NUM))!=null){
            perm=(PermissionsDescriptor)rf.buildDescriptor(curRow,null,this);
            removePermEntryInCache(perm);

            // Build new key based on UUID and drop the entry as we want to drop
            // only this row
            ExecIndexRow uuidKey;
            uuidKey=rf.buildIndexKeyRow(SYSROUTINEPERMSRowFactory.ROUTINEPERMSID_INDEX_NUM,perm);
            ti.deleteRow(tc,uuidKey,SYSROUTINEPERMSRowFactory.ROUTINEPERMSID_INDEX_NUM);
        }
    }

    @Override
    public void dropRoleGrantsByGrantee(String grantee,TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);
        SYSROLESRowFactory rf=(SYSROLESRowFactory)ti.getCatalogRowFactory();

        visitRoleGrants(ti,
                rf,
                SYSROLESRowFactory.SYSROLES_GRANTEE_COLPOS_IN_INDEX_ID_EE_OR,
                grantee,
                tc,
                DataDictionaryImpl.DROP);
    }


    /**
     * Return true if there exists a role grant to authorization
     * identifier.
     *
     * @param grantee authorization identifier
     * @param tc      Transaction Controller
     * @return true if there exists such a grant
     * @throws StandardException Thrown on failure
     */
    private boolean existsRoleGrantByGrantee(String grantee,TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);
        SYSROLESRowFactory rf=(SYSROLESRowFactory)ti.getCatalogRowFactory();

        return visitRoleGrants(ti,
                rf,
                SYSROLESRowFactory.SYSROLES_GRANTEE_COLPOS_IN_INDEX_ID_EE_OR,
                grantee,
                tc,
                DataDictionaryImpl.EXISTS);
    }

    @Override
    public void dropRoleGrantsByName(String roleName,TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);
        SYSROLESRowFactory rf=(SYSROLESRowFactory)ti.getCatalogRowFactory();
        dataDictionaryCache.roleCacheRemove(roleName);
        visitRoleGrants(ti,
                rf,
                SYSROLESRowFactory.SYSROLES_ROLEID_COLPOS_IN_INDEX_ID_EE_OR,
                roleName,
                tc,
                DataDictionaryImpl.DROP);
    }

    /**
     * Scan the {roleid, grantee, grantor} index on SYSROLES,
     * locate rows containing authId in column columnNo.
     * <p/>
     * The action argument can be either <code>EXISTS</code> or
     * <code>DROP</code> (to check for existence, or to drop that row).
     * <p/>
     * If the scan proves too slow, we should add more indexes.  only.
     *
     * @param ti       <code>TabInfoImpl</code> for SYSROLES.
     * @param rf       row factory for SYSROLES
     * @param columnNo the column number to match <code>authId</code> against
     * @param tc       transaction controller
     * @param action   drop matching rows (<code>DROP</code>), or return
     *                 <code>true</code> if there is a matching row
     *                 (<code>EXISTS</code>)
     * @return action=EXISTS: return {@code true} if there is a matching row
     * else return {@code false}.
     * @throws StandardException
     */
    private boolean visitRoleGrants(TabInfoImpl ti,
                                    SYSROLESRowFactory rf,
                                    int columnNo,
                                    String authId,
                                    TransactionController tc,
                                    int action) throws StandardException{
        ConglomerateController heapCC=tc.openConglomerate(
                ti.getHeapConglomerate(),false,0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

        DataValueDescriptor authIdOrderable=new SQLVarchar(authId);
        ScanQualifier[][] scanQualifier=exFactory.getScanQualifier(1);

        scanQualifier[0][0].setQualifier(
                columnNo-1,    /* to zero-based */
                authIdOrderable,
                Orderable.ORDER_OP_EQUALS,
                false,
                false,
                false);

        ScanController sc=tc.openScan(
                ti.getIndexConglomerate(SYSROLESRowFactory.SYSROLES_INDEX_ID_EE_OR_IDX),
                false,   // don't hold open across commit
                0,       // for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,      // all fields as objects
                null, // start position -
                0,                            // startSearchOperation - none
                scanQualifier,                //
                null, // stop position -through last row
                0);                           // stopSearchOperation - none

        try{
            ExecRow outRow=rf.makeEmptyRow();
            ExecIndexRow indexRow=getIndexRowFromHeapRow(
                    ti.getIndexRowGenerator(SYSROLESRowFactory.SYSROLES_INDEX_ID_EE_OR_IDX),
                    heapCC.newRowLocationTemplate(),
                    outRow);

            while(sc.fetchNext(indexRow.getRowArray())){
                if(action==DataDictionaryImpl.EXISTS){
                    return true;
                }else if(action==DataDictionaryImpl.DROP){
                    ti.deleteRow(tc,indexRow,
                            SYSROLESRowFactory.SYSROLES_INDEX_ID_EE_OR_IDX);
                }
            }
        }finally{
            if(sc!=null){
                sc.close();
            }

            if(heapCC!=null){
                heapCC.close();
            }
        }
        return false;
    }


    /**
     * Return an in-memory representation of the role grant graph (sans
     * grant of roles to users, only role-role relation.
     *
     * @param tc      Transaction Controller
     * @param inverse make graph on inverse grant relation
     * @return hash map representing role grant graph.
     * <ul><li>Key: rolename,</li>
     * <li>Value: List<RoleGrantDescriptor> representing a
     * grant of that rolename to another role (not user).
     * </li>
     * </ul>
     * <p/>
     * FIXME: Need to cache graph and invalidate when role graph is modified.
     * Currently, we always read from SYSROLES.
     */
    public Map<String, List<RoleGrantDescriptor>> getRoleGrantGraph(TransactionController tc,boolean inverse, boolean roleOnly) throws StandardException{

        Map<String, List<RoleGrantDescriptor>> hm=new HashMap<>();

        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);
        SYSROLESRowFactory rf=(SYSROLESRowFactory)ti.getCatalogRowFactory();

        DataValueDescriptor isDefOrderable=new SQLVarchar("N");
        ScanQualifier[][] scanQualifier=exFactory.getScanQualifier(1);

        scanQualifier[0][0].setQualifier(
                SYSROLESRowFactory.SYSROLES_ISDEF-1, /* to zero-based */
                isDefOrderable,
                Orderable.ORDER_OP_EQUALS,
                false,
                false,
                false);

        ScanController sc=tc.openScan(
                ti.getHeapConglomerate(),
                false,   // don't hold open across commit
                0,       // for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,      // all fields as objects
                null, // start position -
                0,                            // startSearchOperation - none
                scanQualifier,                //
                null, // stop position -through last row
                0);                           // stopSearchOperation - none

        ExecRow outRow=rf.makeEmptyRow();
        RoleGrantDescriptor grantDescr;

        while(sc.fetchNext(outRow.getRowArray())){
            grantDescr=(RoleGrantDescriptor)rf.buildDescriptor(outRow,null,this);

            if (roleOnly) {
                // Next call is potentially inefficient.  We could read in
                // definitions first in a separate hash table limiting
                // this to a 2-pass scan.
                RoleGrantDescriptor granteeDef = getRoleDefinitionDescriptor(grantDescr.getGrantee());

                if (granteeDef == null) {
                    // not a role, must be user authid, skip
                    continue;
                }
            }

            String hashKey;
            if(inverse){
                hashKey=grantDescr.getGrantee();
            }else{
                hashKey=grantDescr.getRoleName();
            }

            List<RoleGrantDescriptor> arcs=hm.get(hashKey);
            if(arcs==null){
                arcs=new LinkedList<>();
            }

            arcs.add(grantDescr);
            hm.put(hashKey,arcs);
        }

        sc.close();

        return hm;

    }

    @Override
    public RoleClosureIterator createRoleClosureIterator(TransactionController tc,String role,boolean inverse) throws StandardException{
        return new RoleClosureIteratorImpl(role,inverse,this,tc);
    }


    /**
     * Drop all permission descriptors corresponding to a grant to
     * the named authentication identifier
     *
     * @param authId The authentication identifier
     * @param tc     Transaction Controller
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropAllPermsByGrantee(String authId,TransactionController tc) throws StandardException{
        dropPermsByGrantee(
                authId,
                tc,
                SYSSCHEMAPERMS_CATALOG_NUM,
                SYSSCHEMAPERMSRowFactory.GRANTEE_SCHEMA_GRANTOR_INDEX_NUM,
                SYSSCHEMAPERMSRowFactory.
                        GRANTEE_COL_NUM_IN_GRANTEE_SCHEMA_GRANTOR_INDEX);

        dropPermsByGrantee(
                authId,
                tc,
                SYSTABLEPERMS_CATALOG_NUM,
                SYSTABLEPERMSRowFactory.GRANTEE_TABLE_GRANTOR_INDEX_NUM,
                SYSTABLEPERMSRowFactory.
                        GRANTEE_COL_NUM_IN_GRANTEE_TABLE_GRANTOR_INDEX);

        dropPermsByGrantee(
                authId,
                tc,
                SYSCOLPERMS_CATALOG_NUM,
                SYSCOLPERMSRowFactory.GRANTEE_TABLE_TYPE_GRANTOR_INDEX_NUM,
                SYSCOLPERMSRowFactory.
                        GRANTEE_COL_NUM_IN_GRANTEE_TABLE_TYPE_GRANTOR_INDEX);

        dropPermsByGrantee(
                authId,
                tc,
                SYSROUTINEPERMS_CATALOG_NUM,
                SYSROUTINEPERMSRowFactory.GRANTEE_ALIAS_GRANTOR_INDEX_NUM,
                SYSROUTINEPERMSRowFactory.
                        GRANTEE_COL_NUM_IN_GRANTEE_ALIAS_GRANTOR_INDEX);

        dropPermsByGrantee(
                authId,
                tc,
                SYSPERMS_CATALOG_NUM,
                SYSPERMSRowFactory.GRANTEE_OBJECTID_GRANTOR_INDEX_NUM,
                SYSPERMSRowFactory.
                        GRANTEE_COL_NUM_IN_GRANTEE_OBJECTID_GRANTOR_INDEX);
    }


    /**
     * Presently only used when dropping roles - user dropping is not under
     * Derby control (well, built-in users are if properties are stored in
     * database), any permissions granted to users remain in place even if the
     * user is no more.
     */
    private void dropPermsByGrantee(String authId,
                                    TransactionController tc,
                                    int catalog,
                                    int indexNo,
                                    int granteeColnoInIndex) throws StandardException{
        visitPermsByGrantee(authId,tc,catalog,indexNo,granteeColnoInIndex,DataDictionaryImpl.DROP);
    }

    /**
     * Return true if there exists a permission grant descriptor to this
     * authorization id.
     */
    private boolean existsPermByGrantee(String authId,
                                        TransactionController tc,
                                        int catalog,
                                        int indexNo,
                                        int granteeColnoInIndex) throws StandardException{
        return visitPermsByGrantee(authId,tc,catalog,indexNo,granteeColnoInIndex,DataDictionaryImpl.EXISTS);
    }


    /**
     * Possible action for visitPermsByGrantee and visitRoleGrants.
     */
    static final int DROP=0;
    /**
     * Possible action for visitPermsByGrantee and visitRoleGrants.
     */
    static final int EXISTS=1;

    /**
     * Scan <code>indexNo</code> index on a permission table
     * <code>catalog</code>, looking for match(es) for the grantee column
     * (given by granteeColnoInIndex for the catalog in question).
     * <p/>
     * The action argument can be either <code>EXISTS</code> or
     * <code>DROP</code> (to check for existence, or to drop that row).
     * <p/>
     * There is no index on grantee column only on on any of the
     * permissions tables, so we use the index which contain grantee
     * and scan that, setting up a scan qualifier to match the
     * grantee, then fetch the base row.
     * <p/>
     * If this proves too slow, we should add an index on grantee
     * only.
     *
     * @param authId              grantee to match against
     * @param tc                  transaction controller
     * @param catalog             the underlying permission table to visit
     * @param indexNo             the number of the index by which to access the catalog
     * @param granteeColnoInIndex the column number to match
     *                            <code>authId</code> against
     * @param action              drop matching rows (<code>DROP</code>), or return
     *                            <code>true</code> if there is a matching row
     *                            (<code>EXISTS</code>)
     * @return action=EXISTS: return {@code true} if there is a matching row
     * else return {@code false}.
     * @throws StandardException
     */
    private boolean visitPermsByGrantee(String authId,
                                        TransactionController tc,
                                        int catalog,
                                        int indexNo,
                                        int granteeColnoInIndex,
                                        int action) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(catalog);
        PermissionsCatalogRowFactory rf=(PermissionsCatalogRowFactory)ti.getCatalogRowFactory();

        ConglomerateController heapCC=tc.openConglomerate(
                ti.getHeapConglomerate(),false,0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

        DataValueDescriptor authIdOrderable=new SQLVarchar(authId);
        ScanQualifier[][] scanQualifier=exFactory.getScanQualifier(1);

        scanQualifier[0][0].setQualifier(
                granteeColnoInIndex-1,    /* to zero-based */
                authIdOrderable,
                Orderable.ORDER_OP_EQUALS,
                false,
                false,
                false);

        ScanController sc=tc.openScan(
                ti.getIndexConglomerate(indexNo),
                false,                        // don't hold open across commit
                0,                            // for update
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,      // all fields as objects
                null, // start position -
                0,                            // startSearchOperation - none
                scanQualifier,                //
                null, // stop position -through last row
                0);                           // stopSearchOperation - none

        try{
            ExecRow outRow=rf.makeEmptyRow();
            ExecIndexRow indexRow=getIndexRowFromHeapRow(ti.getIndexRowGenerator(indexNo),heapCC.newRowLocationTemplate(),outRow);

            while(sc.fetchNext(indexRow.getRowArray())){
                RowLocation baseRowLocation=(RowLocation)indexRow.getColumn(indexRow.nColumns());

                boolean base_row_exists=heapCC.fetch(baseRowLocation,outRow,null);

                if(SanityManager.DEBUG){
                    // it can not be possible for heap row to
                    // disappear while holding scan cursor on index at
                    // ISOLATION_REPEATABLE_READ.
                    SanityManager.ASSERT(base_row_exists,"base row doesn't exist");
                }

                if(action==DataDictionaryImpl.EXISTS){
                    return true;
                }else if(action==DataDictionaryImpl.DROP){
                    PermissionsDescriptor perm=(PermissionsDescriptor)rf.buildDescriptor(outRow,null,this);
                    removePermEntryInCache(perm);
                    ti.deleteRow(tc,indexRow,indexNo);
                }
            }
        }finally{
            if(sc!=null){
                sc.close();
            }

            if(heapCC!=null){
                heapCC.close();
            }
        }
        return false;
    }


    /**
     * Delete the appropriate rows from syscolumns when
     * dropping 1 or more columns.
     *
     * @param tc     The TransactionController
     * @param keyRow Start/stop position.
     * @throws StandardException Thrown on failure
     */
    private void dropColumnDescriptorCore(TransactionController tc,ExecIndexRow keyRow) throws StandardException{
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];

        ti.deleteRow(tc,keyRow,SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID);
    }

    /**
     * Drop Schema descriptor
     * Useful for DROP SCHEMA
     * @param tc
     * @param keyRow
     * @throws StandardException
     */

    private void dropSchemaPermDescriptor(TransactionController tc,ExecIndexRow keyRow) throws StandardException{
        ExecRow curRow;
        PermissionsDescriptor perm;
        TabInfoImpl ti=getNonCoreTI(SYSSCHEMAPERMS_CATALOG_NUM);
        SYSSCHEMAPERMSRowFactory rf=(SYSSCHEMAPERMSRowFactory)ti.getCatalogRowFactory();

        while((curRow=ti.getRow(tc,keyRow,SYSSCHEMAPERMSRowFactory.SCHEMAID_INDEX_NUM))!=null){
            perm=(PermissionsDescriptor)rf.buildDescriptor(curRow,null,this);
            removePermEntryInCache(perm);

            // Build key on UUID and drop the entry as we want to drop only this row
            ExecIndexRow uuidKey;
            uuidKey=rf.buildIndexKeyRow(SYSSCHEMAPERMSRowFactory.SCHEMAID_INDEX_NUM,perm);
            ti.deleteRow(tc,uuidKey,SYSSCHEMAPERMSRowFactory.SCHEMAID_INDEX_NUM);
        }
    }


    /**
     * Delete the appropriate rows from systableperms when
     * dropping a table
     *
     * @param tc     The TransactionController
     * @param keyRow Start/stop position.
     * @throws StandardException Thrown on failure
     */
    private void dropTablePermDescriptor(TransactionController tc,ExecIndexRow keyRow) throws StandardException{
        ExecRow curRow;
        PermissionsDescriptor perm;
        TabInfoImpl ti=getNonCoreTI(SYSTABLEPERMS_CATALOG_NUM);
        SYSTABLEPERMSRowFactory rf=(SYSTABLEPERMSRowFactory)ti.getCatalogRowFactory();

        while((curRow=ti.getRow(tc,keyRow,SYSTABLEPERMSRowFactory.TABLEID_INDEX_NUM))!=null){
            perm=(PermissionsDescriptor)rf.buildDescriptor(curRow,null,this);
            removePermEntryInCache(perm);

            // Build key on UUID and drop the entry as we want to drop only this row
            ExecIndexRow uuidKey;
            uuidKey=rf.buildIndexKeyRow(SYSTABLEPERMSRowFactory.TABLEPERMSID_INDEX_NUM,perm);
            ti.deleteRow(tc,uuidKey,SYSTABLEPERMSRowFactory.TABLEPERMSID_INDEX_NUM);
        }
    }

    /**
     * Delete the appropriate rows from syscolperms when
     * dropping a table
     *
     * @param tc     The TransactionController
     * @param keyRow Start/stop position.
     * @throws StandardException Thrown on failure
     */
    private void dropColumnPermDescriptor(TransactionController tc,ExecIndexRow keyRow) throws StandardException{
        ExecRow curRow;
        PermissionsDescriptor perm;
        TabInfoImpl ti=getNonCoreTI(SYSCOLPERMS_CATALOG_NUM);
        SYSCOLPERMSRowFactory rf=(SYSCOLPERMSRowFactory)ti.getCatalogRowFactory();

        while((curRow=ti.getRow(tc,keyRow,SYSCOLPERMSRowFactory.TABLEID_INDEX_NUM))!=null){
            perm=(PermissionsDescriptor)rf.buildDescriptor(curRow,null,this);
            removePermEntryInCache(perm);

            // Build key on UUID and drop the entry as we want to drop only this row
            ExecIndexRow uuidKey;
            uuidKey=rf.buildIndexKeyRow(SYSCOLPERMSRowFactory.COLPERMSID_INDEX_NUM,perm);
            ti.deleteRow(tc,uuidKey,SYSCOLPERMSRowFactory.COLPERMSID_INDEX_NUM);
        }
    }

    /**
     * Update the column descriptor in question.  Updates
     * every row in the base conglomerate.
     *
     * @param cd         The ColumnDescriptor
     * @param formerUUID The UUID for this column in SYSCOLUMNS,
     *                   may differ from what is in cd if this
     *                   is the column that is being set.
     * @param formerName The name for this column in SYSCOLUMNS
     *                   may differ from what is in cd if this
     *                   is the column that is being set.
     * @param colsToSet  Array of ints of columns to be modified,
     *                   1 based.  May be null (all cols).
     * @param tc         The TransactionController to use
     * @throws StandardException Thrown on failure
     */
    private void updateColumnDescriptor(ColumnDescriptor cd,
                                        UUID formerUUID,
                                        String formerName,
                                        int[] colsToSet,
                                        TransactionController tc) throws StandardException{
        ExecIndexRow keyRow1;
        ExecRow row;
        DataValueDescriptor refIDOrderable;
        DataValueDescriptor columnNameOrderable;
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];
        SYSCOLUMNSRowFactory rf=(SYSCOLUMNSRowFactory)ti.getCatalogRowFactory();

        /* Use objectID/columnName in both start
         * and stop position for index 1 scan.
         */
        refIDOrderable=getIDValueAsCHAR(formerUUID);
        columnNameOrderable=new SQLVarchar(formerName);

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(2);
        keyRow1.setColumn(1,refIDOrderable);
        keyRow1.setColumn(2,columnNameOrderable);

        // build the row to be stuffed into SYSCOLUMNS. 
        row=rf.makeRow(cd,null);

        /*
        ** Figure out if the index in syscolumns needs
        ** to be updated.
        */
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(rf.getNumIndexes()==2,
                    "There are more indexes on syscolumns than expected, the code herein needs to change");
        }

        boolean[] bArray=new boolean[rf.getNumIndexes()];

        /*
        ** Do we need to update indexes?
        */
        if(colsToSet==null){
            bArray[0]=true;
            bArray[1]=true;
        }else{
            /*
            ** Check the specific columns for indexed
            ** columns.
            */
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i<colsToSet.length;i++){
                if((colsToSet[i]==SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNNAME) ||
                        (colsToSet[i]==SYSCOLUMNSRowFactory.SYSCOLUMNS_REFERENCEID)){
                    bArray[0]=true;
                    break;
                }else if(colsToSet[i]==SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDEFAULTID){
                    bArray[1]=true;
                    break;
                }
            }
        }

        ti.updateRow(keyRow1,row,SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,bArray,colsToSet,tc);
    }

    /**
     * Gets the viewDescriptor for the view with the given UUID.
     *
     * @param uuid The UUID for the view
     * @return A descriptor for the view
     * @throws StandardException Thrown on error
     */
    @Override
    public ViewDescriptor getViewDescriptor(UUID uuid) throws StandardException{
        return getViewDescriptor(getTableDescriptor(uuid));
    }

    /**
     * Gets the viewDescriptor for the view given the TableDescriptor.
     *
     * @param td The TableDescriptor for the view.
     * @return A descriptor for the view
     * @throws StandardException Thrown on error
     */
    @Override
    public ViewDescriptor getViewDescriptor(TableDescriptor td) throws StandardException{

        /* See if the view info is cached */
        if(td.getViewDescriptor()!=null){
            return td.getViewDescriptor();
        }

        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized(td){
            /* See if we were waiting on someone who just filled it in */
            if(td.getViewDescriptor()!=null){
                return td.getViewDescriptor();
            }

            td.setViewDescriptor(getViewDescriptorScan(td));
        }
        return td.getViewDescriptor();
    }

    /**
     * Get the information for the view from sys.sysviews.
     *
     * @param tdi The TableDescriptor for the view.
     * @return ViewDescriptor    The ViewDescriptor for the view.
     * @throws StandardException Thrown on error
     */
    private ViewDescriptor getViewDescriptorScan(TableDescriptor tdi) throws StandardException{
        ViewDescriptor vd;
        DataValueDescriptor viewIdOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSVIEWS_CATALOG_NUM);
        UUID viewID=tdi.getUUID();

        /* Use viewIdOrderable in both start
         * and stop position for scan.
         */
        viewIdOrderable=getIDValueAsCHAR(viewID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,viewIdOrderable);

        vd=(ViewDescriptor)getDescriptorViaIndex(SYSVIEWSRowFactory.SYSVIEWS_INDEX1_ID,keyRow,null,ti,null,null,false);

        if(vd!=null){
            vd.setViewName(tdi.getName());
        }
        return vd;
    }

    /**
     * Drops the view descriptor from the data dictionary.
     *
     * @param vd A descriptor for the view to be dropped
     * @param tc TransactionController to use
     * @throws StandardException Thrown on error
     */
    @Override
    public void dropViewDescriptor(ViewDescriptor vd,TransactionController tc) throws StandardException{
        DataValueDescriptor viewIdOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSVIEWS_CATALOG_NUM);

        /* Use aliasNameOrderable in both start
         * and stop position for scan.
         */
        viewIdOrderable=getIDValueAsCHAR(vd.getUUID());

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,viewIdOrderable);

        ti.deleteRow(tc,keyRow,SYSVIEWSRowFactory.SYSVIEWS_INDEX1_ID);
    }

    /**
     * Scan sysfiles_index2 (id) for a match.
     *
     * @return TableDescriptor    The matching descriptor, or null.
     * @throws StandardException Thrown on failure
     */
    private FileInfoDescriptor getFileInfoDescriptorIndex2Scan(UUID id) throws StandardException{
        DataValueDescriptor idOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSFILES_CATALOG_NUM);
        idOrderable=getIDValueAsCHAR(id);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,idOrderable);

        return (FileInfoDescriptor)
                getDescriptorViaIndex(SYSFILESRowFactory.SYSFILES_INDEX2_ID,keyRow,null,ti,null,null,false);
    }

    @Override
    public FileInfoDescriptor getFileInfoDescriptor(UUID id) throws StandardException{
        return getFileInfoDescriptorIndex2Scan(id);
    }

    /**
     * Scan sysfiles_index1 (schemaid,name) for a match.
     *
     * @return The matching descriptor or null.
     * @throws StandardException Thrown on failure
     */
    private FileInfoDescriptor getFileInfoDescriptorIndex1Scan(UUID schemaId,
                                                               String name) throws StandardException{
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor nameOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSFILES_CATALOG_NUM);

        nameOrderable=new SQLVarchar(name);
        schemaIDOrderable=getIDValueAsCHAR(schemaId);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,nameOrderable);
        keyRow.setColumn(2,schemaIDOrderable);
        return (FileInfoDescriptor)
                getDescriptorViaIndex(SYSFILESRowFactory.SYSFILES_INDEX1_ID,keyRow,null,ti,null,null,false);
    }

    @Override
    public FileInfoDescriptor getFileInfoDescriptor(SchemaDescriptor sd,String name) throws StandardException{
        return getFileInfoDescriptorIndex1Scan(sd.getUUID(),name);
    }

    @Override
    public void dropFileInfoDescriptor(FileInfoDescriptor fid) throws StandardException{
        ExecIndexRow keyRow1;
        DataValueDescriptor idOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSFILES_CATALOG_NUM);
        TransactionController tc=getTransactionExecute();

        /* Use tableIdOrderable and schemaIdOrderable in both start
         * and stop position for index 1 scan.
         */
        idOrderable=getIDValueAsCHAR(fid.getUUID());

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(1);
        keyRow1.setColumn(1,idOrderable);
        ti.deleteRow(tc,keyRow1,SYSFILESRowFactory.SYSFILES_INDEX2_ID);
    }

    /**
     * Get a SPSDescriptor given its UUID.
     *
     * @param uuid The UUID
     * @return The SPSDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SPSDescriptor getSPSDescriptor(UUID uuid) throws StandardException{
        SPSDescriptor sps;
        /* Make sure that non-core info is initialized */
        getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);
        sps = dataDictionaryCache.storedPreparedStatementCacheFind(uuid);
        if(sps!=null)
                return sps;
        sps=getSPSDescriptorIndex2Scan(uuid.toString());
        dataDictionaryCache.storedPreparedStatementCacheAdd(sps);
        return sps;
    }

    /**
     * This method can get called from the DataDictionary cache.
     *
     * @param stmtKey The TableKey of the sps
     * @return The descriptor for the sps, null if the sps does
     * not exist.
     * @throws StandardException Thrown on failure
     */
    public SPSDescriptor getUncachedSPSDescriptor(TableKey stmtKey) throws StandardException{
        return getSPSDescriptorIndex1Scan(stmtKey.getTableName(),stmtKey.getSchemaId().toString());
    }

    /**
     * Scan sysstatements_index2 (stmtid) for a match.
     * Note that we do not do a lookup of parameter info.
     *
     * @return SPSDescriptor    The matching descriptor, if any.
     * @throws StandardException Thrown on failure
     */
    private SPSDescriptor getSPSDescriptorIndex2Scan(String stmtUUID) throws StandardException{
        DataValueDescriptor stmtIDOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

        /* Use stmtIdOrderable in both start
         * and stop position for scan.
         */
        stmtIDOrderable=new SQLChar(stmtUUID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,stmtIDOrderable);

        return (SPSDescriptor)getDescriptorViaIndex(
                SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX1_ID,
                keyRow,
                null,
                ti,
                null,
                null,
                false);
    }

    /**
     * Get a SPSDescriptor given its name.
     * Currently no cacheing.  With caching
     * we need to be very careful about invalidation.
     * No caching means invalidations block on
     * existing SPSD instances (since they were read in
     *
     * @param stmtName the statement name
     * @param sd       The SchemaDescriptor
     * @return The SPSDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SPSDescriptor getSPSDescriptor(String stmtName,SchemaDescriptor sd) throws StandardException{
        /*
        ** If we didn't get a schema descriptor, we had better
        ** have a system table.
        */
        assert sd!=null:"null schema for statement "+stmtName;

        UUID schemaUUID =sd.getUUID();
        TableKey stmtKey =new TableKey(schemaUUID,stmtName);
        SPSDescriptor sps = dataDictionaryCache.spsNameCacheFind(stmtKey);
        if (sps!=null)
            return sps;
        sps = getSPSDescriptorIndex1Scan(stmtName,schemaUUID.toString());
        dataDictionaryCache.spsNameCacheAdd(stmtKey,sps);
        return sps;
    }

    /**
     * Scan sysschemas_index1 (stmtname, schemaid) for a match.
     *
     * @return SPSDescriptor    The matching descriptor, if any.
     * @throws StandardException Thrown on failure
     */
    private SPSDescriptor getSPSDescriptorIndex1Scan(String stmtName,String schemaUUID) throws StandardException{
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor stmtNameOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

        /* Use stmtNameOrderable and schemaIdOrderable in both start
         * and stop position for scan.
         */
        stmtNameOrderable=new SQLVarchar(stmtName);
        schemaIDOrderable=new SQLChar(schemaUUID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,stmtNameOrderable);
        keyRow.setColumn(2,schemaIDOrderable);

        SPSDescriptor spsd=(SPSDescriptor)
                getDescriptorViaIndex(
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX2_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false);

        /*
        ** Set up the parameter defaults.  We are only
        ** doing this when we look up by name because
        ** this is the only time we cache, and it can
        ** be foolish to look up the parameter defaults
        ** for someone that doesn't need them.
        */
        if(spsd!=null){
            List<DataValueDescriptor> tmpDefaults=new ArrayList<>();
            spsd.setParams(getSPSParams(spsd,tmpDefaults));
            Object[] defaults=tmpDefaults.toArray();
            spsd.setParameterDefaults(defaults);
        }

        return spsd;
    }

    /**
     * Adds the given SPSDescriptor to the data dictionary,
     * associated with the given table and constraint type.
     *
     * @param descriptor The descriptor to add
     * @param tc         The transaction controller
     * @throws StandardException Thrown on error
     */
    @Override
    public void addSPSDescriptor(SPSDescriptor descriptor,TransactionController tc) throws StandardException{
        ExecRow row;
        TabInfoImpl ti=getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);
        SYSSTATEMENTSRowFactory rf=(SYSSTATEMENTSRowFactory)ti.getCatalogRowFactory();
        int insertRetCode;

        /*
        ** We must make sure the descriptor is locked
        ** while we are writing it out.  Otherwise,
        ** the descriptor could be invalidated while
        ** we are writing.
        */
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized(descriptor){
            // build the row to be stuffed into SYSSTATEMENTS. this will stuff an
            // UUID into the descriptor
            boolean compileMe=descriptor.initiallyCompilable();
            row=rf.makeSYSSTATEMENTSrow(compileMe,descriptor);

            // insert row into catalog and all its indices
            insertRetCode=ti.insertRow(row,tc);

        }

        // Throw an exception duplicate table descriptor
        if(insertRetCode!=TabInfoImpl.ROWNOTDUPLICATE){
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                    descriptor.getDescriptorType(),
                    descriptor.getDescriptorName(),
                    descriptor.getSchemaDescriptor().getDescriptorType(),
                    descriptor.getSchemaDescriptor().getSchemaName());
        }

        addSPSParams(descriptor,tc);
    }

    /**
     * Add a column in SYS.SYSCOLUMNS for each parameter in the
     * parameter list.
     */
    private void addSPSParams(SPSDescriptor spsd,TransactionController tc) throws StandardException{
        UUID uuid=spsd.getUUID();
        DataTypeDescriptor[] params=spsd.getParams();
        Object[] parameterDefaults=spsd.getParameterDefaults();

        if(params==null)
            return;

        /* Create the columns */
        int pdlSize=params.length;
        for(int index=0;index<pdlSize;index++){
            int parameterId=index+1;

            //RESOLVEAUTOINCREMENT
            ColumnDescriptor cd=
                    new ColumnDescriptor(
                            "PARAM"+parameterId,
                            parameterId,    // position
                            parameterId,    // storagePosition
                            params[index],
                            ((parameterDefaults==null) || (index>=parameterDefaults.length))?
                                    null:
                                    (DataValueDescriptor)parameterDefaults[index],
                            null,
                            uuid,
                            null,0,0,0,parameterId-1);
            // no chance of duplicates here
            addDescriptor(cd,null,SYSCOLUMNS_CATALOG_NUM,false,tc,false);
        }
    }

    /**
     * Get all the parameter descriptors for an SPS.
     * Look up the params in SYSCOLUMNS and turn them
     * into parameter descriptors.
     *
     * @param spsd     sps descriptor
     * @param defaults list for storing column defaults
     * @return array of data type descriptors
     * @throws StandardException Thrown on error
     */
    @Override
    public DataTypeDescriptor[] getSPSParams(SPSDescriptor spsd,List<DataValueDescriptor> defaults) throws StandardException{
        ColumnDescriptorList cdl=new ColumnDescriptorList();
        getColumnDescriptorsScan(spsd.getUUID(),cdl,spsd);

        int cdlSize=cdl.size();
        DataTypeDescriptor[] params=new DataTypeDescriptor[cdlSize];
        for(int index=0;index<cdlSize;index++){
            ColumnDescriptor cd=cdl.elementAt(index);
            params[index]=cd.getType();
            if(defaults!=null){
                defaults.add(cd.getDefaultValue());
            }
        }

        return params;
    }

    /**
     * Updates SYS.SYSSTATEMENTS with the info from the
     * SPSD.
     *
     * @param spsd                   The descriptor to add
     * @param tc                     The transaction controller
     * @param updateParamDescriptors If true, will update the
     *                               parameter descriptors in SYS.SYSCOLUMNS.
     * @param firstCompilation       true, if Statement is getting compiled for first
     *                               time and SPS was created with NOCOMPILE option.
     * @throws StandardException Thrown on error
     */
    @Override
    public void updateSPS(SPSDescriptor spsd,
                          TransactionController tc,
                          boolean recompile,
                          boolean updateParamDescriptors,
                          boolean firstCompilation) throws StandardException{
        ExecIndexRow keyRow1;
        ExecRow row;
        DataValueDescriptor idOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);
        SYSSTATEMENTSRowFactory rf=(SYSSTATEMENTSRowFactory)ti.getCatalogRowFactory();
        int[] updCols;
        if(recompile){
            if(firstCompilation){
                updCols=new int[]{SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_TEXT,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_LASTCOMPILED,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_USINGTEXT,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INITIALLY_COMPILABLE};
            }else{

                updCols=new int[]{SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_TEXT,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_LASTCOMPILED,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_USINGTEXT,
                        SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE};
            }
        }else{
            // This is an invalidation request.
            // Update the VALID column (to false) and clear the plan stored in the CONSTANTSTATE column.
            updCols=new int[]{
                    SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID,
                    SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE,
            };
        }

        idOrderable=getIDValueAsCHAR(spsd.getUUID());

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(1);
        keyRow1.setColumn(1,idOrderable);

        row=rf.makeSYSSTATEMENTSrow(false,    // don't compile
                spsd);

        /*
        ** Not updating any indexes
        */
        boolean[] bArray=new boolean[2];

        /*
        ** Partial update
        */
        ti.updateRow(keyRow1,row,SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX1_ID,bArray,updCols,tc);

        /*
        ** If we don't need to update the parameter
        ** descriptors, we are done.
        */
        if(!updateParamDescriptors){
            return;
        }

        /*
        ** Set the defaults and datatypes for the parameters, if
        ** there are parameters.
        */
        DataTypeDescriptor[] params=spsd.getParams();
        if(params==null){
            return;
        }

        if(firstCompilation){
            /*beetle:5119, reason for doing add here instead of update
             *is with NOCOMPILE option of create statement/boot time SPS,
             *SPS statement is not compiled to find out the parameter info.
             *Because of the parameter info was not inserted at SPSDescriptor
             *creation time. As this is the first time we are compiling paramter
             *infor should be inserted instead of the update.
             */
            addSPSParams(spsd,tc);
        }else{
            Object[] parameterDefaults=spsd.getParameterDefaults();

            /*
            ** Update each column with the new defaults and with
            ** the new datatypes.  It is possible that someone has
            ** done a drop/create on the underlying table and
            ** changed the type of a column, which has changed
            ** the type of a parameter to our statement.
            */
            int[] columnsToSet=new int[2];
            columnsToSet[0]=SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDATATYPE;
            columnsToSet[1]=SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDEFAULT;

            UUID uuid=spsd.getUUID();

            for(int index=0;index<params.length;index++){
                int parameterId=index+1;

                //RESOLVEAUTOINCREMENT
                ColumnDescriptor cd=new ColumnDescriptor("PARAM"+parameterId,
                        parameterId,    // position
                        parameterId,    // storage position
                        params[index],
                        ((parameterDefaults==null) || // default
                                (index>=parameterDefaults.length))?
                                null:
                                (DataValueDescriptor)parameterDefaults[index],
                        null,
                        uuid,
                        null,
                        0,0,0,parameterId-1);

                updateColumnDescriptor(cd,cd.getReferencingUUID(),cd.getColumnName(),columnsToSet,tc);
            }
        }
    }

    @Override
    public void recompileInvalidSPSPlans(LanguageConnectionContext lcc) throws StandardException{
        for(Object o : getAllSPSDescriptors()){
            SPSDescriptor spsd=(SPSDescriptor)o;
            spsd.getPreparedStatement(true);
        }
    }

    @Override
    public void invalidateAllSPSPlans() throws StandardException{
        LanguageConnectionContext lcc=(LanguageConnectionContext)ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
        invalidateAllSPSPlans(lcc);
    }

    @Override
    public void invalidateAllSPSPlans(LanguageConnectionContext lcc) throws StandardException{
        startWriting(lcc);

        for(Object o : getAllSPSDescriptors()){
            SPSDescriptor spsd=(SPSDescriptor)o;
            spsd.makeInvalid(DependencyManager.USER_RECOMPILE_REQUEST,lcc);
        }
    }


    /**
     * Mark all SPS plans in the data dictionary invalid. This does
     * not invalidate cached plans. This function is for use by
     * the boot-up code.
     *
     * @throws StandardException Thrown on error
     */
    public void clearSPSPlans() throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);
        faultInTabInfo(ti);

        TransactionController tc=getTransactionExecute();

        FormatableBitSet columnToReadSet=new FormatableBitSet(SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COLUMN_COUNT);
        FormatableBitSet columnToUpdateSet=new FormatableBitSet(SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COLUMN_COUNT);
        for(int i=0;i<SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COLUMN_COUNT;i++){
            // we do not want to read the saved serialized plan
            if (i+1 != SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE) {
                columnToReadSet.set(i);
            }
            columnToUpdateSet.set(i);
        }
        /* Set up a couple of row templates for fetching CHARS */
        DataValueDescriptor[] rowTemplate =
                new DataValueDescriptor[SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COLUMN_COUNT];

        DataValueDescriptor[] replaceRow=
                new DataValueDescriptor[SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COLUMN_COUNT];

        /* Scan the entire heap */
        ScanController sc=
                tc.openScan(
                        ti.getHeapConglomerate(),
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_REPEATABLE_READ,
                        columnToReadSet,
                        null,
                        ScanController.NA,
                        null,
                        null,
                        ScanController.NA);

        while(sc.fetchNext(rowTemplate)){
            /* Replace the column in the table */
            for (int i=0; i<rowTemplate.length; i++) {
                if (i+1 == SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID)
                    replaceRow[i] = new SQLBoolean(false);
                else if (i+1 == SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE)
                    replaceRow[i] = new UserType(null);
                else
                    replaceRow[i] = rowTemplate[i].cloneValue(false);
            }
            sc.replace(replaceRow,columnToUpdateSet);
        }

        sc.close();
    }

    /**
     * Drops the given SPSDescriptor.
     *
     * @param descriptor The descriptor to drop
     * @param tc         The TransactionController.
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropSPSDescriptor(SPSDescriptor descriptor,TransactionController tc) throws StandardException{
        dropSPSDescriptor(descriptor.getUUID(),tc);
    }

    /**
     * Drops the given SPSDescriptor.
     *
     * @param uuid the statement uuid
     * @param tc   The TransactionController.
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropSPSDescriptor(UUID uuid,TransactionController tc) throws StandardException{
        DataValueDescriptor stmtIdOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

        stmtIdOrderable=getIDValueAsCHAR(uuid);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,stmtIdOrderable);

        // DERBY-3870: The compiled plan may not be possible to deserialize
        // during upgrade. Skip the column that contains the compiled plan to
        // prevent deserialization errors when reading the rows. We don't care
        // about the value in that column, since this method is only called
        // when we want to drop or invalidate rows in SYSSTATEMENTS.
        FormatableBitSet cols=new FormatableBitSet(ti.getCatalogRowFactory().getHeapColumnCount());
        for(int i=0;i<cols.size();i++){
            if(i+1==SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE){
                cols.clear(i);
            }else{
                cols.set(i);
            }
        }
        ti.deleteRow(tc,keyRow,SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX1_ID, cols);

        /* drop all columns in SYSCOLUMNS */
        dropAllColumnDescriptors(uuid,tc);
    }

    /**
     * Get every statement in this database.
     * Return the SPSDescriptors in an list.
     * The returned descriptors don't contain the compiled statement, so it
     * it safe to call this method during upgrade when it isn't known if the
     * saved statement can still be deserialized with the new version.
     *
     * @return the list of descriptors
     * @throws StandardException Thrown on failure
     */
    @Override
    public List getAllSPSDescriptors() throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

        List list=newSList();

        // DERBY-3870: The compiled plan may not be possible to deserialize
        // during upgrade. Skip the column that contains the compiled plan to
        // prevent deserialization errors when reading the rows. We don't care
        // about the value in that column, since this method is only called
        // when we want to drop or invalidate rows in SYSSTATEMENTS.
        FormatableBitSet cols=new FormatableBitSet(ti.getCatalogRowFactory().getHeapColumnCount());
        for(int i=0;i<cols.size();i++){
            if(i+1==SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE){
                cols.clear(i);
            }else{
                cols.set(i);
            }
        }

        getDescriptorViaHeap(cols,null,ti,null,list);

        return list;
    }

    /**
     * Get every constraint in this database.
     * Note that this list of ConstraintDescriptors is
     * not going to be the same objects that are typically
     * cached off of the table descriptors, so this will
     * most likely instantiate some duplicate objects.
     *
     * @return the list of descriptors
     * @throws StandardException Thrown on failure
     */
    private ConstraintDescriptorList getAllConstraintDescriptors() throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

        ConstraintDescriptorList list=new ConstraintDescriptorList();

        getConstraintDescriptorViaHeap(null,ti,list);
        return list;
    }

    /**
     * Get every trigger in this database.
     * Note that this list of TriggerDescriptors is
     * not going to be the same objects that are typically
     * cached off of the table descriptors, so this will
     * most likely instantiate some duplicate objects.
     *
     * @return the list of descriptors
     * @throws StandardException Thrown on failure
     */
    private GenericDescriptorList getAllTriggerDescriptors() throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);

        GenericDescriptorList list=new GenericDescriptorList();

        getDescriptorViaHeap(null,null,ti,null,list);
        return list;
    }

    /**
     * Comparator that can be used for sorting lists of column references
     * on the position they have in the SQL query string.
     */
    private static final Comparator<ColumnReference> OFFSET_COMPARATOR=new Comparator<ColumnReference>(){
        @Override
        public int compare(ColumnReference o1,ColumnReference o2){
            // Return negative int, zero, or positive int if the first column
            // reference has an offset which is smaller than, equal to, or
            // greater than the offset of the second column reference.
            return o1.getBeginOffset()-o2.getBeginOffset();
        }
    };

    public int[] examineTriggerNodeAndCols(
                    Visitable actionStmt,
                    String oldReferencingName,
                    String newReferencingName,
                    int[] referencedCols,
                    int[] referencedColsInTriggerAction,
                    TableDescriptor triggerTableDescriptor,
                    TriggerEventDML triggerEventMask,
                    boolean createTriggerTime) throws StandardException
    {
            //Total Number of columns in the trigger table
            int numberOfColsInTriggerTable = triggerTableDescriptor.getNumberOfColumns();

            //The purpose of following array(triggerColsAndTriggerActionCols)
            //is to identify all the trigger columns and all the columns from
            //the trigger action which are referenced though old/new
            //transition variables(in other words, accessed through the
            //REFERENCING clause section of CREATE TRIGGER sql). This array
            //will be initialized to -1 at the beginning. By the end of this
            //method, all the columns referenced by the trigger action
            //through the REFERENCING clause and all the trigger columns will
            //have their column positions in the trigger table noted in this
            //array.
            //eg
            //CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1
            //    REFERENCING OLD AS oldt NEW AS newt
            //    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
            //For the trigger above, triggerColsAndTriggerActionCols will
            //finally have [-1,2,-1,4,-1] This list will include all the
            //columns that need to be fetched into memory during trigger
            //execution. All the columns with their entries marked -1 will
            //not be read into memory because they are not referenced in the
            //trigger action through old/new transition variables and they are
            //not recognized as trigger columns.
            int[] triggerColsAndTriggerActionCols = new int[numberOfColsInTriggerTable];

            /**
             * It identifies all the trigger action columns and is initialized to -1.
             */

            int[] triggerActionColsOnly = new int[numberOfColsInTriggerTable];
            java.util.Arrays.fill(triggerActionColsOnly, -1);

            if (referencedCols == null) {
                    //This means that even though the trigger is defined at row
                    //level, it is either an INSERT/DELETE trigger. Or it is an
                    //UPDATE trigger with no specific column(s) identified as the
                    //trigger column(s). In these cases, Derby is going to read all
                    //the columns from the trigger table during trigger execution.
                    //eg of an UPDATE trigger with no specific trigger column(s)
                    // CREATE TRIGGER tr1 AFTER UPDATE ON table1
                    //    REFERENCING OLD AS oldt NEW AS newt
                    //    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
                    for (int i=0; i < numberOfColsInTriggerTable; i++) {
                            triggerColsAndTriggerActionCols[i]=i+1;
                    }
            } else {
                    //This means that this row level trigger is an UPDATE trigger
                    //defined on specific column(s).
                    java.util.Arrays.fill(triggerColsAndTriggerActionCols, -1);
                    for(int referencedCol : referencedCols){
                        //Make a note of this trigger column's column position in
                        //triggerColsAndTriggerActionCols. This will tell us that
                        //this column needs to be read in when the trigger fires.
                        //eg for the CREATE TRIGGER below, we will make a note of
                        //column c12's position in triggerColsAndTriggerActionCols
                        //eg
                        //CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1
                        //    REFERENCING OLD AS oldt NEW AS newt
                        //    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
                        triggerColsAndTriggerActionCols[referencedCol-1]=referencedCol;
                    }
            }
            if (referencedColsInTriggerAction != null) {
                    for (int i=0; i < referencedColsInTriggerAction.length; i++){
                            if( referencedColsInTriggerAction[i] > 0 )
                            triggerColsAndTriggerActionCols[referencedColsInTriggerAction[i]-1] = referencedColsInTriggerAction[i];
                    }
            }

            /* we need to sort on position in string, beetle 4324
             */
            SortedSet<ColumnReference> refs = getTransitionVariables(
                                                 actionStmt, oldReferencingName, newReferencingName);

            if (createTriggerTime) {
                    //The purpose of following array(triggerActionColsOnly) is to
                    //identify all the columns from the trigger action which are
                    //referenced though old/new transition variables(in other words,
                    //accessed through the REFERENCING clause section of
                    //CREATE TRIGGER sql). This array will be initialized to -1 at the
                    //beginning. By the end of this method, all the columns referenced
                    //by the trigger action through the REFERENCING clause will have
                    //their column positions in the trigger table noted in this array.
                    //eg
                    //CREATE TABLE table1 (c11 int, c12 int, c13 int, c14 int, c15 int);
                    //CREATE TABLE table2 (c21 int, c22 int, c23 int, c24 int, c25 int);
                    //CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1
                    //    REFERENCING OLD AS oldt NEW AS newt
                    //    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
                    //For the trigger above, triggerActionColsOnly will finally have
                    //[-1,-1,-1,4,-1]. We will note all the entries for this array
                    //which are not -1 into SYSTRIGGERS(-1 indiciates columns with
                    //those column positions from the trigger table are not being
                    //referenced in the trigger action through the old/new transition
                    //variables.

                    //By this time, we have collected the positions of the trigger
                    //columns in array triggerColsAndTriggerActionCols. Now we need
                    //to start looking at the columns in trigger action to collect
                    //all the columns referenced through REFERENCES clause. These
                    //columns will be noted in triggerColsAndTriggerActionCols and
                    //triggerActionColsOnly arrays.

                    //At the end of the for loop below, we will have both arrays
                    //triggerColsAndTriggerActionCols & triggerActionColsOnly
                    //filled up with the column positions of the columns which are
                    //either trigger columns or triger action columns which are
                    //referenced through old/new transition variables.
                    //eg
                    //CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1
                    //    REFERENCING OLD AS oldt NEW AS newt
                    //    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
                    //For the above trigger, before the for loop below, the contents
                    //of the 2 arrays will be as follows
                    //triggerActionColsOnly [-1,-1,-1,-1,-1]
                    //triggerColsAndTriggerActionCols [-1,2,-1,-1,-1]
                    //After the for loop below, the 2 arrays will look as follows
                    //triggerActionColsOnly [-1,-1,-1,4,-1]
                    //triggerColsAndTriggerActionCols [-1,2,-1,4,-1]

                    for (ColumnReference ref : refs)
                    {
                            if (ref.getBeginOffset()==-1)
                                continue;
                            TableName tableName = ref.getTableNameNode();

                            if((tableName==null) ||
                                    ((oldReferencingName==null || !oldReferencingName.equals(tableName.getTableName())) &&
                                            (newReferencingName==null || !newReferencingName.equals(tableName.getTableName())))){
                                continue;
                            }

                            if(tableName.getBeginOffset()==-1){
                                continue;
                            }
                            checkInvalidTriggerReference(tableName.getTableName(),
                                            oldReferencingName,
                                            newReferencingName,
                                            triggerEventMask);
                            String colName = ref.getColumnName();

                            ColumnDescriptor triggerColDesc;

                            //Following will catch the case where an invalid column is
                            //used in trigger action through the REFERENCING clause. The
                            //following trigger is trying to use oldt.c13 but there is no
                            //column c13 in trigger table table1
                            //CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1
                            //    REFERENCING OLD AS oldt NEW AS newt
                            //    FOR EACH ROW UPDATE table2 SET c24=oldt.c14567;
                            if ((triggerColDesc = triggerTableDescriptor.getColumnDescriptor(colName)) ==
                            null) {
                                    throw StandardException.newException(
                                    SQLState.LANG_COLUMN_NOT_FOUND, tableName+"."+colName);
                            }
                            int triggerColDescPosition = triggerColDesc.getPosition();
                            triggerColsAndTriggerActionCols[triggerColDescPosition-1]=triggerColDescPosition;
                            triggerActionColsOnly[triggerColDescPosition-1]=triggerColDescPosition;
                            referencedColsInTriggerAction[triggerColDescPosition-1] = triggerColDescPosition;
                    }
            } else {
                    //We are here because we have come across an invalidated trigger
                    //which is being fired. This code gets called for such a trigger
                    //only if it is a row level trigger with REFERENCEs clause
                    //
                    // referencedColsInTriggerAction can be null if trigger action
                    // does not use any columns through REFERENCING clause. This can
                    // happen when we are coming here through ALTER TABLE DROP COLUMN
                    // and the trigger being rebuilt does not use any columns through
                    // REFERENCING clause. DERBY-4887
                    if(referencedCols!=null && referencedColsInTriggerAction!=null){
                        for(int aReferencedColsInTriggerAction : referencedColsInTriggerAction){
                            triggerColsAndTriggerActionCols[aReferencedColsInTriggerAction-1]=aReferencedColsInTriggerAction;
                        }
                    }
            }
            Arrays.sort( triggerColsAndTriggerActionCols );

            //Now that we know what columns we need for trigger columns and
            //trigger action columns, we can get rid of remaining -1 entries
            //for the remaining columns from trigger table.
            //eg
            //CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1
            //    REFERENCING OLD AS oldt NEW AS newt
            //    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
            //For the above trigger, before the justTheRequiredColumns() call,
            //the content of triggerColsAndTriggerActionCols array were as
            //follows [-1, 2, -1, 4, -1]
            //After the justTheRequiredColumns() call below,
            //triggerColsAndTriggerActionCols will have [2,4]. What this means
            //that, at run time, during trigger execution, these are the only
            //2 column positions that will be read into memory from the
            //trigger table. The columns in other column positions are not
            //needed for trigger execution.
            triggerColsAndTriggerActionCols = justTheRequiredColumns(
                            triggerColsAndTriggerActionCols, triggerTableDescriptor);

            return triggerColsAndTriggerActionCols;
    }

    /**
     * Get the trigger action string associated with the trigger after the
     * references to old/new transition tables/variables in trigger action
     * sql provided by CREATE TRIGGER have been transformed eg
     *
     * DELETE FROM t WHERE c = old.c
     * turns into
     * DELETE FROM t WHERE c = com.splicemachine.db.iapi.db.Factory::getTriggerExecutionContext().getOldRow().getInt(columnNumberFor'C'inRuntimeResultset)
     * or
     * DELETE FROM t WHERE c in (SELECT c FROM OLD)
     * turns into
     * DELETE FROM t WHERE c in (SELECT c FROM new TriggerOldTransitionTable OLD)
     *
     * @param actionStmt                    This is needed to get access to the various nodes
     *                                      generated by the Parser for the trigger action sql. These nodes will be
     *                                      used to find REFERENCEs column nodes.
     * @param oldReferencingName            The name specified by the user for REFERENCEs
     *                                      to old row columns
     * @param newReferencingName            The name specified by the user for REFERENCEs
     *                                      to new row columns
     * @param triggerDefinition             The original trigger action text provided by
     *                                      the user during CREATE TRIGGER time.
     * @param referencedCols                Trigger is defined on these columns (will be null
     *                                      in case of INSERT AND DELETE Triggers. Can also be null for DELETE
     *                                      Triggers if UPDATE trigger is not defined on specific column(s))
     * @param referencedColsInTriggerAction what columns does the trigger
     *                                      action reference through old/new transition variables (may be null)
     * @param actionOffset                  offset of start of action clause
     * @param triggerTableDescriptor        Table descriptor for trigger table
     * @param triggerEventMask              TriggerDescriptor.TRIGGER_EVENT_XXX
     * @param createTriggerTime             True if here for CREATE TRIGGER,
     *                                      false if here because an invalidated row level trigger with
     *                                      REFERENCEd columns has been fired and hence trigger action
     *                                      sql associated with SPSDescriptor may be invalid too.
     * @return Transformed trigger action sql
     * @throws StandardException
     */
    @Override
    public String getTriggerActionString(
            Visitable actionStmt,
            String oldReferencingName,
            String newReferencingName,
            String triggerDefinition,
            int[] referencedCols,
            int[] referencedColsInTriggerAction,
            int actionOffset,
            TableDescriptor triggerTableDescriptor,
            TriggerEventDML triggerEventMask,
            boolean createTriggerTime,
            List<int[]> replacements,
            int[] cols
            ) throws StandardException{

        StringBuilder newText = new StringBuilder();
        int start = 0;

        SortedSet<ColumnReference> refs = getTransitionVariables(
        actionStmt, oldReferencingName, newReferencingName);

        //This is where we do the actual transformation of trigger action
        //sql. An eg of that is
        //    DELETE FROM t WHERE c = old.c
        // turns into
        //    DELETE FROM t WHERE c = org.apache.derby.iapi.db.Factory::
        //      getTriggerExecutionContext().getOldRow().
        //    getInt(columnNumberFor'C'inRuntimeResultset)
        // or
        //    DELETE FROM t WHERE c in (SELECT c FROM OLD)
        // turns into
        //    DELETE FROM t WHERE c in
        //        (SELECT c FROM new TriggerOldTransitionTable OLD)
        for (ColumnReference ref : refs)
        {
            if(ref.getBeginOffset()==-1){
                continue;
            }
                    TableName tableName=ref.getTableNameNode();
            if((tableName==null) ||
                    ((oldReferencingName==null || !oldReferencingName.equals(tableName.getTableName())) &&
                            (newReferencingName==null || !newReferencingName.equals(tableName.getTableName())))){
                continue;
            }

            if(tableName.getBeginOffset()==-1){
                continue;
            }
            int tableBeginOffset = tableName.getBeginOffset() - actionOffset;

            String colName = ref.getColumnName();

            // Add whatever we've seen after the previous replacement.
            newText.append(triggerDefinition, start, tableBeginOffset);

            int colPositionInRuntimeResultSet = -1;
            ColumnDescriptor triggerColDesc = triggerTableDescriptor.getColumnDescriptor(colName);
            //DERBY-5121 We can come here if the column being used in trigger
            // action is getting dropped and we have come here through that
            // ALTER TABLE DROP COLUMN. In that case, we will not find the
            // column in the trigger table.
            if (triggerColDesc == null) {
                    throw StandardException.newException(
                    SQLState.LANG_COLUMN_NOT_FOUND, tableName+"."+colName);
            }
            int colPositionInTriggerTable = triggerColDesc.getPosition();

            //This part of code is little tricky and following will help
            //understand what mapping is happening here.
            //eg
            //CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1
            //    REFERENCING OLD AS oldt NEW AS newt
            //    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
            //For the above trigger, triggerColsAndTriggerActionCols will
            //have [2,4]. What this means that, at run time, during trigger
            //execution, these are the only 2 column positions that will be
            //read into memory from the trigger table. The columns in other
            //column positions are not needed for trigger execution. But
            //even though column positions in original trigger table are 2
            //and 4, their relative column positions in the columns read at
            //execution time is really [1,2]. At run time, when the trigger
            //gets fired, column position 2 from the trigger table will be
            //read as the first column and column position 4 from the
            //trigger table will be read as the second column. And those
            //relative column positions at runtime is what should be used
            //during trigger action conversion from
            //UPDATE table2 SET c24=oldt.c14
            //to
            //UPDATE table2 SET c24=
            //  org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().
            //  getOldRow().getInt(2)
            //Note that the generated code above refers to column c14 from
            //table1 by position 2 rather than position 4. Column c14's
            //column position in table1 is 4 but in the relative columns
            //that will be fetched during trigger execution, it's position
            //is 2. That is what the following code is doing.
            if (cols != null){
                for (int j = 0; j< cols.length; j++){
                        if (cols[j] == colPositionInTriggerTable)
                                colPositionInRuntimeResultSet=j+1;
                }
            } else
                colPositionInRuntimeResultSet=colPositionInTriggerTable;

            // Add the replacement code that accesses a value in the
            // transition variable.
            final int replacementOffset = newText.length();
            boolean isSetTarget = false;
            if (actionStmt instanceof SetNode) {
                String regex    =   "(^set[\\s]+$)|([\\s]+set[\\s]+$)|(,[\\s]*$)";
                Pattern pattern =   Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                Matcher matcher =   pattern.matcher(newText);
                if (matcher.find())
                    isSetTarget = true;
            }

            newText.append(genColumnReferenceSQL(triggerTableDescriptor, colName,
                           tableName.getTableName(),
                           tableName.getTableName().equals(oldReferencingName),
                           colPositionInRuntimeResultSet,
                           isSetTarget));

            start = ref.getEndOffset() + 1 - actionOffset;

            if (replacements != null) {
                // Record that we have made a change.
                replacements.add(new int[] {
                    tableBeginOffset,  // offset to replaced text
                    start,             // offset to token after replaced text
                    replacementOffset, // offset to replacement
                    newText.length()   // offset to token after replacement
                });
            }
    }

        //By this point, we are finished transforming the trigger action if
        //it has any references to old/new transition variables.
        if(start<triggerDefinition.length()) {
            newText.append(triggerDefinition, start, triggerDefinition.length());
        }

        return newText.toString();
    }


    /**
     * Get all columns that reference transition variables in triggers.
     * The columns should be returned in the same order as in the SQL text.
     *
     * @param node the node in which to look for transition variables
     * @param oldReferencingName the name of the old transition variable
     * @param newReferencingName the name of the new transition variable
     * @return all references to transition variables
     */
    private static SortedSet<ColumnReference> getTransitionVariables(
        Visitable node, String oldReferencingName, String newReferencingName)
        throws StandardException
    {
        // First get all column references.
        SortedSet<ColumnReference> refs =
            ((QueryTreeNode) node).getOffsetOrderedNodes(ColumnReference.class);

        // Then remove all that are not referencing a transition variable.
        Iterator<ColumnReference> it = refs.iterator();
        while (it.hasNext()) {
            TableName tableName = it.next().getTableNameNode();
            if (!isTransitionVariable(
                    tableName, oldReferencingName, newReferencingName)) {
                it.remove();
            }
        }

        // Return what's left. Should be all references to transition
        // variables.
        return refs;
    }

    /**
     * Check if a table name is actually a transition variable.
     *
     * @param tableName the table name to check
     * @param oldReferencingName the name of the old transition variable
     * @param newReferencingName the name of the new transition variable
     * @return {@code true} if the table name is a transition variable,
     *   {@code false} otherwise
     */
    private static boolean isTransitionVariable(TableName tableName,
            String oldReferencingName, String newReferencingName) {
        if (tableName != null) {
            if (tableName.hasSchema()) {
                // DERBY-6540: Schema-qualified names are not transition
                // variables.
                return false;
            }

            // If there is no schema, and the name is equal to the old or
            // the new transition variable, then it is a transition variable.
            String name = tableName.getTableName();
            if (name != null) {
                return name.equals(oldReferencingName)
                        || name.equals(newReferencingName);
            }
        }

        // Otherwise, it is not a transition variable.
        return false;
    }

    /*
     * The arrary passed will have either -1 or a column position as it's
     * elements. If the array only has -1 as for all it's elements, then
     * this method will return null. Otherwise, the method will create a
     * new arrary with all -1 entries removed from the original arrary.
     */
    private int[] justTheRequiredColumns(int[] columnsArrary,TableDescriptor triggerTableDescriptor){
        int countOfColsRefedInArray=0;
        int numberOfColsInTriggerTable=triggerTableDescriptor.getNumberOfColumns();

        //Count number of non -1 entries
        for(int i=0;i<numberOfColsInTriggerTable;i++){
            if(columnsArrary[i]!=-1)
                countOfColsRefedInArray++;
        }

        if(countOfColsRefedInArray>0){
            int[] tempArrayOfNeededColumns=new int[countOfColsRefedInArray];
            int j=0;
            for(int i=0;i<numberOfColsInTriggerTable;i++){
                if(columnsArrary[i]!=-1)
                    tempArrayOfNeededColumns[j++]=columnsArrary[i];
            }
            return tempArrayOfNeededColumns;
        }else
            return null;
    }

    /*
    * Check for illegal combinations here: insert & old or delete and new
    */
    private void checkInvalidTriggerReference(String tableName,
                                              String oldReferencingName,
                                              String newReferencingName,
                                              TriggerEventDML triggerEventMask) throws StandardException {
        if (tableName.equals(oldReferencingName) && triggerEventMask == TriggerEventDML.INSERT) {
            throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "INSERT", "new");
        } else if (tableName.equals(newReferencingName) && triggerEventMask == TriggerEventDML.DELETE) {
            throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "DELETE", "old");
        }
    }

    /*
    ** Make sure the given column name is found in the trigger
    ** target table.  Generate the appropriate SQL to get it.
    **
    ** @return a string that is used to get the column using
    ** getObject() on the desired result set and CAST it back
    ** to the proper type in the SQL domain.
    **
    ** @exception StandardException on invalid column name
    */
    private String genColumnReferenceSQL(TableDescriptor td,
                                         String colName,
                                         String tabName,
                                         boolean isOldTable,
                                         int colPositionInRuntimeResultSet,
                                         boolean isSetStatement) throws StandardException{
        ColumnDescriptor colDesc;
        if((colDesc=td.getColumnDescriptor(colName))==null){
            throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND,tabName+"."+colName);
        }

        /*
        ** Generate something like this:
        **
        **         CAST (com.splicemachine.db.iapi.db.Factory::
        **            getTriggerExecutionContext().getNewRow().
        **                getObject(<colPosition>) AS DECIMAL(6,2))
        **
        ** Column position is used to avoid the wrong column being
        ** selected problem (DERBY-1258) caused by the case insensitive
        ** JDBC rules for fetching a column by name.
        **
        ** The cast back to the SQL Domain may seem redundant
        ** but we need it to make the column reference appear
        ** EXACTLY like a regular column reference, so we need
        ** the object in the SQL Domain and we need to have the
        ** type information.  Thus a user should be able to do
        ** something like
        **
        **        CREATE TRIGGER ... INSERT INTO T length(Column), ...
        **
        */

        DataTypeDescriptor dts=colDesc.getType();
        TypeId typeId=dts.getTypeId();

        if(!typeId.isXMLTypeId()){

            @SuppressWarnings("StringBufferReplaceableByString") StringBuilder methodCall=new StringBuilder();
            if (isSetStatement) {
                methodCall.append("com.splicemachine.db.iapi.db.Factory::getTriggerExecutionContext().");
                methodCall.append(isOldTable?"getOldRow()":"getNewRow()");
                methodCall.append(".getColumn(");
                methodCall.append(colPositionInRuntimeResultSet);
                methodCall.append(") ");
            }
            else {
                methodCall.append("CAST (com.splicemachine.db.iapi.db.Factory::getTriggerExecutionContext().");
                methodCall.append(isOldTable ? "getOldRow()" : "getNewRow()");
                methodCall.append(".cloneColumn(");
                methodCall.append(colPositionInRuntimeResultSet);
                methodCall.append(").getObject() AS ");
                /*
                 ** getSQLString() returns <typeName>
                 ** for user types, so call getSQLTypeName in that
                 ** case.
                 */
                methodCall.append((typeId.userType() ? typeId.getSQLTypeName() : dts.getSQLstring()));

                methodCall.append(") ");
            }

            return methodCall.toString();
        }else{
            /*  DERBY-2350
            **
            **  Triggers currently use jdbc 1.2 to access columns.  The default
            **  uses getObject() which is not supported for an XML type until
            **  jdbc 4.  In the meantime use getString() and then call
            **  XMLPARSE() on the string to get the type.  See Derby issue and
            **  http://wiki.apache.org/db-db/TriggerImplementation , for
            **  better long term solutions.  Long term I think changing the
            **  trigger architecture to not rely on jdbc, but instead on an
            **  internal direct access interface to execution nodes would be
            **  best future direction, but believe such a change appropriate
            **  for a major release, not a bug fix.
            **
            **  Rather than the above described code generation, use the
            **  following for XML types to generate an XML column from the
            **  old or new row.
            **
            **          XMLPARSE(DOCUMENT
            **              CAST (com.splicemachine.db.iapi.db.Factory::
            **                  getTriggerExecutionContext().getNewRow().
            **                      getString(<colPosition>) AS CLOB)
            **                        PRESERVE WHITESPACE)
            */

            @SuppressWarnings("StringBufferReplaceableByString") StringBuilder methodCall=new StringBuilder();
            methodCall.append("XMLPARSE(DOCUMENT CAST( ");
            methodCall.append(
                    "com.splicemachine.db.iapi.db.Factory::getTriggerExecutionContext().");
            methodCall.append(isOldTable?"getOldRow()":"getNewRow()");
            methodCall.append(".cloneColumn(");
            methodCall.append(colPositionInRuntimeResultSet);
            methodCall.append(").getString() AS CLOB) PRESERVE WHITESPACE ) ");

            return methodCall.toString();
        }
    }

    /**
     * Get a TriggerDescriptor given its UUID.
     *
     * @param uuid The UUID
     * @return The TriggerDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public TriggerDescriptor getTriggerDescriptor(UUID uuid) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);
        DataValueDescriptor triggerIdOrderable=getIDValueAsCHAR(uuid);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,triggerIdOrderable);

        return (TriggerDescriptor)
                getDescriptorViaIndex(
                        SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX1_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false);
    }

    /**
     * Get the stored prepared statement descriptor given
     * a sps name.
     *
     * @param name The sps name.
     * @param sd   The schema descriptor.
     * @return The TriggerDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public TriggerDescriptor getTriggerDescriptor(String name,SchemaDescriptor sd) throws StandardException{
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor triggerNameOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);

        /* Use triggerNameOrderable and schemaIdOrderable in both start
         * and stop position for scan.
         */
        triggerNameOrderable=new SQLVarchar(name);
        schemaIDOrderable=getIDValueAsCHAR(sd.getUUID());

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,triggerNameOrderable);
        keyRow.setColumn(2,schemaIDOrderable);

        return (TriggerDescriptor)
                getDescriptorViaIndex(SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX2_ID,keyRow,null,ti,null,null,false);
    }

    /**
     * Load up the trigger descriptor list for this table
     * descriptor and return it.  If the descriptor list
     * is already loaded up, it is retuned without further
     * ado.
     *
     * @param td The table descriptor.
     * @return The ConstraintDescriptorList for the table
     * @throws StandardException Thrown on failure
     */
    @Override
    public GenericDescriptorList getTriggerDescriptors(TableDescriptor td) throws StandardException{
        GenericDescriptorList gdl;

        if(td==null){
            return getAllTriggerDescriptors();
        }

        /* Build the TableDescriptor's TDL if it is currently empty */
        gdl=td.getTriggerDescriptorList();

        /*
        ** Synchronize the building of the TDL.  The TDL itself is created
        ** empty when the TD is created, so there is no need to synchronize
        ** the getting of the TDL.
        */
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized(gdl){
            if(!gdl.getScanned()){
                getTriggerDescriptorsScan(td,false);
            }
        }

        return gdl;
    }

    /**
     * Populate the GenericDescriptorList for the specified TableDescriptor.
     * <p/>
     * MT synchronization: it is assumed that the caller has synchronized
     * on the CDL in the given TD.
     *
     * @param td        The TableDescriptor.
     * @param forUpdate Whether or not to open scan for update
     * @throws StandardException Thrown on failure
     */
    private void getTriggerDescriptorsScan(TableDescriptor td,boolean forUpdate) throws StandardException{
        GenericDescriptorList gdl=(td).getTriggerDescriptorList();
        DataValueDescriptor tableIDOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);

        /* Use tableIDOrderable in both start and stop positions for scan */
        tableIDOrderable=getIDValueAsCHAR(td.getUUID());

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);

        keyRow.setColumn(1,tableIDOrderable);

        getDescriptorViaIndex(
                SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX3_ID,
                keyRow,
                null,
                ti,
                null,
                gdl,
                forUpdate);
        gdl.setScanned(true);
    }

    /**
     * Drops the given TriggerDescriptor.  WARNING: does
     * not drop its SPSes!!!
     *
     * @param descriptor The descriptor to drop
     * @param tc         The TransactionController.
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropTriggerDescriptor(TriggerDescriptor descriptor,TransactionController tc) throws StandardException{
        DataValueDescriptor idOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);

        idOrderable=getIDValueAsCHAR(descriptor.getUUID());

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,idOrderable);

        ti.deleteRow(tc,keyRow,SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX1_ID);
    }


    /**
     * Get a ConstraintDescriptor given its UUID.  Please
     * use getConstraintDescriptorById() is you have the
     * constraints table descriptor, it is much faster.
     *
     * @param uuid The UUID
     * @return The ConstraintDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConstraintDescriptor getConstraintDescriptor(UUID uuid) throws StandardException{
        DataValueDescriptor UUIDStringOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

        /* Use UUIDStringOrderable in both start and stop positions for scan */
        UUIDStringOrderable=getIDValueAsCHAR(uuid);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,UUIDStringOrderable);

        return getConstraintDescriptorViaIndex(SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX1_ID,keyRow,ti,null,null,false);
    }

    /**
     * Get a ConstraintDescriptor given its name and schema ID.
     * Please use getConstraintDescriptorByName() if you have the
     * constraint's table descriptor, it is much faster.
     *
     * @param constraintName Constraint name.
     * @param schemaID       The schema UUID
     * @return The ConstraintDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConstraintDescriptor getConstraintDescriptor(String constraintName,UUID schemaID) throws StandardException{
        DataValueDescriptor UUIDStringOrderable;
        DataValueDescriptor constraintNameOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

        /* Construct keys for both start and stop positions for scan */
        constraintNameOrderable=new SQLVarchar(constraintName);
        UUIDStringOrderable=getIDValueAsCHAR(schemaID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,constraintNameOrderable);
        keyRow.setColumn(2,UUIDStringOrderable);

        return getConstraintDescriptorViaIndex(SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX2_ID,keyRow,ti,null,null,false);
    }

    /**
     * Load up the constraint descriptor list for this table
     * descriptor and return it.  If the descriptor list
     * is already loaded up, it is retuned without further
     * ado.  If no table descriptor is passed in, then all
     * constraint descriptors are retrieved.  Note that in
     * this case, the constraint descriptor objects may be
     * duplicates of constraint descriptors that are hung
     * off of the table descriptor cache.
     *
     * @param td The table descriptor.  If null,
     *           all constraint descriptors are returned.
     * @return The ConstraintDescriptorList for the table
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConstraintDescriptorList getConstraintDescriptors(TableDescriptor td) throws StandardException{
        ConstraintDescriptorList cdl;

        if(td==null){
            return getAllConstraintDescriptors();
        }

        /* RESOLVE - need to look at multi-user aspects of hanging constraint
         * descriptor list off of table descriptor when we restore the cache.
         */

        /* Build the TableDescriptor's CDL if it is currently empty */
        cdl=td.getConstraintDescriptorList();

        /*
        ** Synchronize the building of the CDL.  The CDL itself is created
        ** empty when the TD is created, so there is no need to synchronize
        ** the getting of the CDL.
        */
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized(cdl){
            if(!cdl.getScanned()){
                getConstraintDescriptorsScan(td,false);
            }
        }

        return cdl;
    }

    /**
     * Convert a constraint descriptor list into a list
     * of active constraints, that is, constraints which
     * must be enforced. For the Core product, these
     * are just the constraints on the original list.
     * However, during REFRESH we may have deferred some
     * constraints until statement end. This method returns
     * the corresponding list of constraints which AREN'T
     * deferred.
     *
     * @param cdl The constraint descriptor list to wrap with
     *            an Active constraint descriptor list.
     * @return The corresponding Active ConstraintDescriptorList
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConstraintDescriptorList getActiveConstraintDescriptors(ConstraintDescriptorList cdl) throws StandardException{
        return cdl;
    }

    /**
     * Reports whether an individual constraint must be
     * enforced. For the Core product, this routine always
     * returns true.
     * <p/>
     * However, during REFRESH we may have deferred some
     * constraints until statement end. This method returns
     * false if the constraint deferred
     *
     * @param constraint the constraint to check
     * @return The corresponding Active ConstraintDescriptorList
     * @throws StandardException Thrown on failure
     */
    @Override
    public boolean activeConstraint(ConstraintDescriptor constraint) throws StandardException{
        return true;
    }

    /**
     * Get the constraint descriptor given a table and the UUID String
     * of the backing index.
     *
     * @param td   The table descriptor.
     * @param uuid the UUID for the backing index.
     * @return The ConstraintDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConstraintDescriptor getConstraintDescriptor(TableDescriptor td,UUID uuid) throws StandardException{
        return getConstraintDescriptors(td).getConstraintDescriptor(uuid);
    }


    /**
     * Get the constraint descriptor given a table and the UUID String
     * of the constraint
     *
     * @param td   The table descriptor.
     * @param uuid The UUID for the constraint
     * @return The ConstraintDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConstraintDescriptor getConstraintDescriptorById(TableDescriptor td,UUID uuid) throws StandardException{
        return getConstraintDescriptors(td).getConstraintDescriptorById(uuid);
    }

    /**
     * Get the constraint descriptor given a TableDescriptor and the constraint name.
     *
     * @param td             The table descriptor.
     * @param sd             The schema descriptor for the constraint
     * @param constraintName The constraint name.
     * @param forUpdate      Whether or not access is for update
     * @return The ConstraintDescriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConstraintDescriptor getConstraintDescriptorByName(TableDescriptor td,
                                                              SchemaDescriptor sd,
                                                              String constraintName,
                                                              boolean forUpdate) throws StandardException{
        /* If forUpdate, then we need to actually read from the table. */
        if(forUpdate){
            td.emptyConstraintDescriptorList();
            getConstraintDescriptorsScan(td,true);
        }
        return getConstraintDescriptors(td).getConstraintDescriptorByName(sd,constraintName);
    }

    /**
     * Populate the ConstraintDescriptorList for the specified TableDescriptor.
     * <p/>
     * MT synchronization: it is assumed that the caller has synchronized
     * on the CDL in the given TD.
     *
     * @param td        The TableDescriptor.
     * @param forUpdate Whether or not to open scan for update
     * @throws StandardException Thrown on failure
     */
    private void getConstraintDescriptorsScan(TableDescriptor td,boolean forUpdate) throws StandardException{
        ConstraintDescriptorList cdl=td.getConstraintDescriptorList();
        DataValueDescriptor tableIDOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

        /* Use tableIDOrderable in both start and stop positions for scan */
        tableIDOrderable=getIDValueAsCHAR(td.getUUID());

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);

        keyRow.setColumn(1,tableIDOrderable);

        getConstraintDescriptorViaIndex(
                SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX3_ID,
                keyRow,
                ti,
                td,
                cdl,
                forUpdate);
        cdl.setScanned(true);
    }

    /**
     * Return a (single or list of) ConstraintDescriptor(s) from
     * SYSCONSTRAINTS where the access is from the index to the heap.
     *
     * @param indexId   The id of the index (0 to # of indexes on table) to use
     * @param keyRow    The supplied ExecIndexRow for search
     * @param ti        The TabInfoImpl to use
     * @param td        The TableDescriptor, if supplied.
     * @param dList     The list to build, if supplied.  If null, then caller expects
     *                  a single descriptor
     * @param forUpdate Whether or not to open scan for update
     * @return The last matching descriptor
     * @throws StandardException Thrown on error
     */
    protected ConstraintDescriptor getConstraintDescriptorViaIndex(int indexId,
                                                                   ExecIndexRow keyRow,
                                                                   TabInfoImpl ti,
                                                                   TableDescriptor td,
                                                                   ConstraintDescriptorList dList,
                                                                   boolean forUpdate) throws StandardException{
        SYSCONSTRAINTSRowFactory rf=(SYSCONSTRAINTSRowFactory)ti.getCatalogRowFactory();
        ConglomerateController heapCC;
        ConstraintDescriptor cd=null;
        ExecIndexRow indexRow1;
        ExecRow outRow;
        RowLocation baseRowLocation;
        ScanController scanController;
        TransactionController tc;

        // Get the current transaction controller
        tc=getTransactionCompile();

        outRow=rf.makeEmptyRow();

        heapCC=tc.openConglomerate(ti.getHeapConglomerate(),false,0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);


        /* Scan the index and go to the data pages for qualifying rows to
         * build the column descriptor.
         */
        scanController=tc.openScan(
                ti.getIndexConglomerate(indexId),  // conglomerate to open
                false, // don't hold open across commit
                (forUpdate)?TransactionController.OPENMODE_FORUPDATE:0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,         // all fields as objects
                keyRow.getRowArray(),   // start position - exact key match.
                ScanController.GE,      // startSearchOperation
                null,                   //scanQualifier,
                keyRow.getRowArray(),   // stop position - exact key match.
                ScanController.GT);     // stopSearchOperation

        while(scanController.next()){
            SubConstraintDescriptor subCD=null;

            // create an index row template
            indexRow1=getIndexRowFromHeapRow(ti.getIndexRowGenerator(indexId),heapCC.newRowLocationTemplate(),outRow);

            scanController.fetch(indexRow1.getRowArray());

            baseRowLocation=(RowLocation)indexRow1.getColumn(
                    indexRow1.nColumns());

            boolean base_row_exists=heapCC.fetch(baseRowLocation,outRow,null);

            // it can not be possible for heap row to disappear while
            // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
            assert base_row_exists:"base row doesn't exist";

            switch(rf.getConstraintType(outRow)){
                case DataDictionary.PRIMARYKEY_CONSTRAINT:
                case DataDictionary.FOREIGNKEY_CONSTRAINT:
                case DataDictionary.UNIQUE_CONSTRAINT:
                    subCD=getSubKeyConstraint(
                            rf.getConstraintId(outRow),rf.getConstraintType(outRow));
                    break;

                case DataDictionary.CHECK_CONSTRAINT:
                    subCD=getSubCheckConstraint(
                            rf.getConstraintId(outRow));
                    break;

                default:
                    if(SanityManager.DEBUG){
                        SanityManager.THROWASSERT("unexpected value from rf.getConstraintType(outRow)"+
                                rf.getConstraintType(outRow));
                    }
            }

            assert subCD!=null:"subCD is expected to be non-null";

            /* Cache the TD in the SCD so that
             * the row factory doesn't need to go
             * out to disk to get it.
             */
            subCD.setTableDescriptor(td);

            cd=(ConstraintDescriptor)rf.buildDescriptor(outRow,subCD,this);

            /* If dList is null, then caller only wants a single descriptor - we're done
             * else just add the current descriptor to the list.
             */
            if(dList==null){
                break;
            }else{
                dList.add(cd);
            }
        }
        scanController.close();
        heapCC.close();
        return cd;
    }

    /**
     * Return a (single or list of) catalog row descriptor(s) from
     * SYSCONSTRAINTS through a heap scan
     *
     * @param scanQualifiers qualifiers
     * @param ti             The TabInfoImpl to use
     * @param list           The list to build, if supplied.
     *                       If null, then caller expects a single descriptor
     * @return The last matching descriptor
     * @throws StandardException Thrown on error
     */
    protected TupleDescriptor getConstraintDescriptorViaHeap(ScanQualifier[][] scanQualifiers,
                                                             TabInfoImpl ti,
                                                             List<ConstraintDescriptor> list) throws StandardException{
        SYSCONSTRAINTSRowFactory rf=(SYSCONSTRAINTSRowFactory)ti.getCatalogRowFactory();
        ExecRow outRow;
        ScanController scanController;
        TransactionController tc;
        ConstraintDescriptor cd=null;

        // Get the current transaction controller
        tc=getTransactionCompile();

        outRow=rf.makeEmptyRow();

        /*
        ** Table scan
        */
        scanController=tc.openScan(
                ti.getHeapConglomerate(),      // conglomerate to open
                false,                          // don't hold open across commit
                0,                              // for read
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,               // all fields as objects
                null, // start position - first row
                0,                          // startSearchOperation - none
                scanQualifiers,              // scanQualifier,
                null, // stop position -through last row
                0);                          // stopSearchOperation - none

        try{
            while(scanController.fetchNext(outRow.getRowArray())){
                SubConstraintDescriptor subCD=null;

                switch(rf.getConstraintType(outRow)){
                    case DataDictionary.PRIMARYKEY_CONSTRAINT:
                    case DataDictionary.FOREIGNKEY_CONSTRAINT:
                    case DataDictionary.UNIQUE_CONSTRAINT:
                        subCD=getSubKeyConstraint(rf.getConstraintId(outRow),rf.getConstraintType(outRow));
                        break;

                    case DataDictionary.CHECK_CONSTRAINT:
                        subCD=getSubCheckConstraint(rf.getConstraintId(outRow));
                        break;

                    default:
                        if(SanityManager.DEBUG){
                            SanityManager.THROWASSERT("unexpected value from  rf.getConstraintType(outRow) "
                                    +rf.getConstraintType(outRow));
                        }
                }

                assert subCD!=null:"subCD is expected to be non-null";
                cd=(ConstraintDescriptor)rf.buildDescriptor(outRow,subCD,this);

                /* If dList is null, then caller only wants a single descriptor - we're done
                 * else just add the current descriptor to the list.
                 */
                if(list==null){
                    break;
                }else{
                    list.add(cd);
                }
            }
        }finally{
            scanController.close();
        }
        return cd;
    }

    /**
     * Return a table descriptor corresponding to the TABLEID
     * field in SYSCONSTRAINTS where CONSTRAINTID matches
     * the constraintId passsed in.
     *
     * @param constraintId The id of the constraint
     * @return the corresponding table descriptor
     * @throws StandardException Thrown on error
     */
    @Override
    public TableDescriptor getConstraintTableDescriptor(UUID constraintId) throws StandardException{
        List slist=getConstraints(constraintId,
                SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX1_ID,
                SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_TABLEID);

        if(slist.isEmpty()){
            return null;
        }

        // get the table descriptor
        return getTableDescriptor((UUID)slist.get(0));
    }

    /**
     * Return a list of foreign keys constraints referencing
     * this constraint.  Returns both enabled and disabled
     * foreign keys.
     *
     * @param constraintId The id of the referenced constraint
     * @return list of constraints, empty of there are none
     * @throws StandardException Thrown on error
     */
    @Override
    public ConstraintDescriptorList getForeignKeys(UUID constraintId) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSFOREIGNKEYS_CATALOG_NUM);
        List<SubKeyConstraintDescriptor> fkList=newSList();

        // Use constraintIDOrderable in both start and stop positions for scan
        DataValueDescriptor constraintIDOrderable=getIDValueAsCHAR(constraintId);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,constraintIDOrderable);

        getDescriptorViaIndex(
                SYSFOREIGNKEYSRowFactory.SYSFOREIGNKEYS_INDEX2_ID,
                keyRow,
                null,
                ti,
                null,
                fkList,
                false);

        TableDescriptor td;
        ConstraintDescriptorList cdl=new ConstraintDescriptorList();

        for(SubKeyConstraintDescriptor cd : fkList){
            td=getConstraintTableDescriptor(cd.getUUID());
            cdl.add(getConstraintDescriptors(td).getConstraintDescriptorById(cd.getUUID()));
        }

        return cdl;
    }

    /**
     * Return an List which of the relevant column matching
     * the indexed criteria.  If nothing matches, returns an
     * empty List (never returns null).
     *
     * @param uuid      The id of the constraint
     * @param indexId   The index id in SYS.SYSCONSTRAINTS
     * @param columnNum The column to retrieve
     * @return a list of UUIDs in an List.
     * @throws StandardException Thrown on error
     */
    public List getConstraints(UUID uuid,int indexId,int columnNum) throws StandardException{
        ExecIndexRow indexRow1;
        ExecRow outRow;
        RowLocation baseRowLocation;
        ConglomerateController heapCC=null;
        ScanController scanController=null;
        TransactionController tc;
        TabInfoImpl ti=getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);
        SYSCONSTRAINTSRowFactory rf=(SYSCONSTRAINTSRowFactory)ti.getCatalogRowFactory();
        List<UUID> slist=newSList();

        assert indexId==SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX1_ID ||
                indexId==SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX3_ID:"bad index id: must be an index on a uuid";

        assert columnNum>0:"Invalid column number";
        assert columnNum<=SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_COLUMN_COUNT:" Invalid column number";

        try{
            /* Use tableIDOrderable in both start and stop positions for scan */
            DataValueDescriptor orderable=getIDValueAsCHAR(uuid);

            /* Set up the start/stop position for the scan */
            ExecIndexRow keyRow=exFactory.getIndexableRow(1);
            keyRow.setColumn(1,orderable);

            // Get the current transaction controller
            tc=getTransactionCompile();

            outRow=rf.makeEmptyRow();

            long heapConglomId=ti.getHeapConglomerate();
            int lockMode=TransactionController.MODE_RECORD;
            int isolationLevel=TransactionController.ISOLATION_REPEATABLE_READ;
            heapCC=tc.openConglomerate(heapConglomId,false,0,lockMode,isolationLevel);

            // create an index row template
            indexRow1=getIndexRowFromHeapRow(ti.getIndexRowGenerator(indexId),heapCC.newRowLocationTemplate(),outRow);

            // just interested in one column
            DataValueDescriptor[] rowTemplate=new DataValueDescriptor[SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_COLUMN_COUNT];
            FormatableBitSet columnToGetSet=new FormatableBitSet(SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_COLUMN_COUNT);
            columnToGetSet.set(columnNum-1);

            rowTemplate[columnNum-1]=new SQLChar();

            // Scan the index and go to the data pages for qualifying rows 
            scanController=tc.openScan(
                    ti.getIndexConglomerate(indexId),// conglomerate to open
                    false,                            // don't hold open across commit
                    0,                                // for read
                    lockMode,
                    isolationLevel,// RESOLVE: should be level 2
                    null,                 // all fields as objects
                    keyRow.getRowArray(),            // start position - exact key match.
                    ScanController.GE,                // startSearchOperation
                    null,                            // scanQualifier (none)
                    keyRow.getRowArray(),            // stop position - exact key match.
                    ScanController.GT);                // stopSearchOperation

            ValueRow row = new ValueRow();
            row.setRowArray(rowTemplate);

            while(scanController.fetchNext(indexRow1.getRowArray())){
                baseRowLocation=(RowLocation)indexRow1.getColumn(indexRow1.nColumns());

                // get the row and grab the uuid
                boolean base_row_exists=heapCC.fetch(baseRowLocation,row,columnToGetSet);

                assert base_row_exists:"base row not found"; //SI reads should see the row

                slist.add(uuidFactory.recreateUUID(rowTemplate[columnNum-1].getString()));
            }
        }finally{
            if(heapCC!=null){
                heapCC.close();
            }
            if(scanController!=null){
                scanController.close();
            }
        }
        return slist;
    }

    /**
     * Adds the given ConstraintDescriptor to the data dictionary,
     * associated with the given table and constraint type.
     *
     * @param descriptor The descriptor to add
     * @param tc         The transaction controller
     * @throws StandardException Thrown on error
     */
    @Override
    public void addConstraintDescriptor(ConstraintDescriptor descriptor,TransactionController tc) throws StandardException{
        int type=descriptor.getConstraintType();

        if(SanityManager.DEBUG){
            if(!(type==DataDictionary.PRIMARYKEY_CONSTRAINT ||
                    type==DataDictionary.FOREIGNKEY_CONSTRAINT ||
                    type==DataDictionary.UNIQUE_CONSTRAINT ||
                    type==DataDictionary.CHECK_CONSTRAINT)){
                SanityManager.THROWASSERT("constraint type ("+type+
                        ") is unexpected value");
            }
        }

        addDescriptor(descriptor,descriptor.getSchemaDescriptor(),SYSCONSTRAINTS_CATALOG_NUM,false,tc,false);

        switch(type){
            case DataDictionary.PRIMARYKEY_CONSTRAINT:
            case DataDictionary.FOREIGNKEY_CONSTRAINT:
            case DataDictionary.UNIQUE_CONSTRAINT:
                assert descriptor instanceof KeyConstraintDescriptor:"incorrect type: "+descriptor.getClass();

                addSubKeyConstraint((KeyConstraintDescriptor)descriptor,tc);
                break;

            case DataDictionary.CHECK_CONSTRAINT:
                assert descriptor instanceof CheckConstraintDescriptor:" incorrect type: "+descriptor.getClass();

                addDescriptor(descriptor,null,SYSCHECKS_CATALOG_NUM,true,tc,false);
                break;
            default:
                assert false; // That should never be reached as 
        }
    }

    /**
     * Update the constraint descriptor in question.  Updates
     * every row in the base conglomerate.
     *
     * @param cd         The Constraintescriptor
     * @param formerUUID The UUID for this column in SYSCONSTRAINTS,
     *                   may differ from what is in cd if this
     *                   is the column that is being set.
     * @param colsToSet  Array of ints of columns to be modified,
     *                   1 based.  May be null (all cols).
     * @param tc         The TransactionController to use
     * @throws StandardException Thrown on failure
     */
    @Override
    public void updateConstraintDescriptor(ConstraintDescriptor cd,
                                           UUID formerUUID,
                                           int[] colsToSet,
                                           TransactionController tc) throws StandardException{
        ExecIndexRow keyRow1;
        ExecRow row;
        DataValueDescriptor IDOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);
        SYSCONSTRAINTSRowFactory rf=(SYSCONSTRAINTSRowFactory)ti.getCatalogRowFactory();

        /* Use objectID/columnName in both start
         * and stop position for index 1 scan.
         */
        IDOrderable=getIDValueAsCHAR(formerUUID);

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(1);
        keyRow1.setColumn(1,IDOrderable);

        // build the row to be stuffed into SYSCONSTRAINTS. 
        row=rf.makeRow(cd,null);

        /*
        ** Figure out if the index in sysconstraints needs
        ** to be updated.
        */
        assert rf.getNumIndexes()==3:"Programmer error: there are more indexes on sysconstraints than expected";

        boolean[] bArray=new boolean[3];

        /*
        ** Do we need to update indexes?
        */
        if(colsToSet==null){
            bArray[0]=true;
            bArray[1]=true;
            bArray[2]=true;
        }else{
            /*
            ** Check the specific columns for indexed
            ** columns.
            */
            for(int colToSet : colsToSet){
                switch(colToSet){
                    case SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_CONSTRAINTID:
                        bArray[0]=true;
                        break;

                    case SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_CONSTRAINTNAME:
                    case SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_SCHEMAID:
                        bArray[1]=true;
                        break;

                    case SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_TABLEID:
                        bArray[2]=true;
                        break;
                    default:
                        // Do nothing
                }
            }
        }

        ti.updateRow(keyRow1,row,SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX1_ID,bArray,colsToSet,tc);
    }

    /**
     * Drops the given ConstraintDescriptor from the data dictionary.
     *
     * @param descriptor The descriptor to drop
     * @param tc         The TransactionController
     * @throws StandardException Thrown on error
     */
    @Override
    public void dropConstraintDescriptor(ConstraintDescriptor descriptor,
                                         TransactionController tc) throws StandardException{
        ExecIndexRow keyRow;
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor constraintNameOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

        switch(descriptor.getConstraintType()){
            case DataDictionary.PRIMARYKEY_CONSTRAINT:
            case DataDictionary.FOREIGNKEY_CONSTRAINT:
            case DataDictionary.UNIQUE_CONSTRAINT:
                dropSubKeyConstraint(descriptor,tc);
                break;

            case DataDictionary.CHECK_CONSTRAINT:
                dropSubCheckConstraint(descriptor.getUUID(),tc);
                break;
            default:
                assert false: "Dropping constraint of type " + descriptor.getConstraintType() + "not implemented";
        }

        /* Use constraintNameOrderable and schemaIdOrderable in both start
         * and stop position for index 2 scan.
         */
        constraintNameOrderable=new SQLVarchar(descriptor.getConstraintName());
        schemaIDOrderable=getIDValueAsCHAR(descriptor.getSchemaDescriptor().getUUID());

        /* Set up the start/stop position for the scan */
        keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,constraintNameOrderable);
        keyRow.setColumn(2,schemaIDOrderable);

        ti.deleteRow(tc,keyRow,SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX2_ID);
    }

    /**
     * Get a SubKeyConstraintDescriptor from syskeys or sysforeignkeys for
     * the specified constraint id.  For primary foreign and and unique
     * key constraints.
     *
     * @param constraintId The UUID for the constraint.
     * @param type         The type of the constraint
     *                     (e.g. DataDictionary.FOREIGNKEY_CONSTRAINT)
     * @return SubKeyConstraintDescriptor    The Sub descriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SubKeyConstraintDescriptor getSubKeyConstraint(UUID constraintId,int type) throws StandardException{
        DataValueDescriptor constraintIDOrderable;
        TabInfoImpl ti;
        int indexNum;
        int baseNum;

        switch(type){
            case DataDictionary.FOREIGNKEY_CONSTRAINT:
                baseNum=SYSFOREIGNKEYS_CATALOG_NUM;
                indexNum=SYSFOREIGNKEYSRowFactory.SYSFOREIGNKEYS_INDEX1_ID;
                break;
            case DataDictionary.PRIMARYKEY_CONSTRAINT:
                baseNum=SYSPRIMARYKEYS_CATALOG_NUM;
                indexNum=SYSPRIMARYKEYSRowFactory.SYSPRIMARYKEYS_INDEX1_ID;
                break;
            default:
                baseNum=SYSKEYS_CATALOG_NUM;
                indexNum=SYSKEYSRowFactory.SYSKEYS_INDEX1_ID;
        }
        ti=getNonCoreTI(baseNum);

        /* Use constraintIDOrderable in both start and stop positions for scan */
        constraintIDOrderable=getIDValueAsCHAR(constraintId);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,constraintIDOrderable);

        return (SubKeyConstraintDescriptor)
                getDescriptorViaIndex(
                        indexNum,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false);
    }

    /**
     * Add the matching row to syskeys when adding a unique or primary key constraint
     *
     * @param descriptor The KeyConstraintDescriptor for the constraint.
     * @param tc         The TransactionController
     * @throws StandardException Thrown on failure
     */
    protected abstract void addSubKeyConstraint(KeyConstraintDescriptor descriptor,
                                       TransactionController tc) throws StandardException;

    /**
     * Drop the matching row from syskeys when dropping a primary key
     * or unique constraint.
     *
     * @param constraint the constraint
     * @param tc         The TransactionController
     * @throws StandardException Thrown on failure
     */
    private void dropSubKeyConstraint(ConstraintDescriptor constraint,TransactionController tc) throws StandardException{
        ExecIndexRow keyRow1;
        DataValueDescriptor constraintIdOrderable;
        TabInfoImpl ti;
        int baseNum;
        int indexNum;

        if(constraint.getConstraintType()==DataDictionary.FOREIGNKEY_CONSTRAINT){
            baseNum=SYSFOREIGNKEYS_CATALOG_NUM;
            indexNum=SYSFOREIGNKEYSRowFactory.SYSFOREIGNKEYS_INDEX1_ID;

            /*
            ** If we have a foreign key, we need to decrement the
            ** reference count of the contraint that this FK references.
            ** We need to do this *before* we drop the foreign key
            ** because of the way FK.getReferencedConstraint() works.
            */
            ReferencedKeyConstraintDescriptor refDescriptor=(ReferencedKeyConstraintDescriptor)getConstraintDescriptor(
                    ((ForeignKeyConstraintDescriptor)constraint).
                            getReferencedConstraintId());

            if(refDescriptor!=null){
                refDescriptor.decrementReferenceCount();

                int[] colsToSet=new int[1];
                colsToSet[0]=SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_REFERENCECOUNT;

                updateConstraintDescriptor(refDescriptor,refDescriptor.getUUID(),colsToSet,tc);
            }
            ti = getNonCoreTI(baseNum);
        }else if(constraint.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT){
            ti=getPkTable();
            faultInTabInfo(ti);
            indexNum=0;
        } else{
            baseNum=SYSKEYS_CATALOG_NUM;
            indexNum=SYSKEYSRowFactory.SYSKEYS_INDEX1_ID;
            ti=getNonCoreTI(baseNum);
        }


        /* Use constraintIdOrderable in both start
         * and stop position for index 1 scan.
         */
        constraintIdOrderable=getIDValueAsCHAR(constraint.getUUID());

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(1);
        keyRow1.setColumn(1,constraintIdOrderable);

        ti.deleteRow(tc,keyRow1,indexNum);
    }

    protected abstract TabInfoImpl getPkTable() throws StandardException;

    /**
     * Get a SubCheckConstraintDescriptor from syschecks for
     * the specified constraint id.  (Useful for check constraints.)
     *
     * @param constraintId The UUID for the constraint.
     * @return SubCheckConstraintDescriptor    The Sub descriptor for the constraint.
     * @throws StandardException Thrown on failure
     */
    private SubCheckConstraintDescriptor getSubCheckConstraint(UUID constraintId) throws StandardException{
        DataValueDescriptor constraintIDOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSCHECKS_CATALOG_NUM);

        /* Use constraintIDOrderable in both start and stop positions for scan */
        constraintIDOrderable=getIDValueAsCHAR(constraintId);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,constraintIDOrderable);

        return (SubCheckConstraintDescriptor)
                getDescriptorViaIndex(SYSCHECKSRowFactory.SYSCHECKS_INDEX1_ID,keyRow,null,ti,null,null,false);
    }

    /**
     * Drop the matching row from syschecks when dropping a check constraint.
     *
     * @param constraintId The constraint id.
     * @param tc           The TransactionController
     * @throws StandardException Thrown on failure
     */
    private void dropSubCheckConstraint(UUID constraintId,TransactionController tc) throws StandardException{
        ExecIndexRow checkRow1;
        DataValueDescriptor constraintIdOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSCHECKS_CATALOG_NUM);

        /* Use constraintIdOrderable in both start
         * and stop position for index 1 scan.
         */
        constraintIdOrderable=getIDValueAsCHAR(constraintId);

        /* Set up the start/stop position for the scan */
        checkRow1=exFactory.getIndexableRow(1);
        checkRow1.setColumn(1,constraintIdOrderable);

        ti.deleteRow(tc,checkRow1,SYSCHECKSRowFactory.SYSCHECKS_INDEX1_ID);
    }

    /**
     * Get all of the ConglomerateDescriptors in the database and
     * hash them by conglomerate number.
     * This is useful as a performance optimization for the locking VTIs.
     * NOTE:  This method will scan SYS.SYSCONGLOMERATES at READ UNCOMMITTED.
     *
     * @param tc TransactionController for the transaction
     * @return A Hashtable with all of the ConglomerateDescriptors
     * in the database hashed by conglomerate number.
     * @throws StandardException Thrown on failure
     */
    @Override
    public Map<Long, ConglomerateDescriptor> hashAllConglomerateDescriptorsByNumber(TransactionController tc) throws StandardException{
        Map<Long, ConglomerateDescriptor> ht=new HashMap<>();
        ConglomerateDescriptor cd;
        ScanController scanController;
        ExecRow outRow;
        TabInfoImpl ti=coreInfo[SYSCONGLOMERATES_CORE_NUM];
        SYSCONGLOMERATESRowFactory rf=(SYSCONGLOMERATESRowFactory)ti.getCatalogRowFactory();

        outRow=rf.makeEmptyRow();
        scanController=tc.openScan(
                ti.getHeapConglomerate(),  // conglomerate to open
                false, // don't hold open across commit
                0, // for read
                TransactionController.MODE_RECORD,  // scans whole table.
                TransactionController.ISOLATION_READ_UNCOMMITTED,
                null, // all fields as objects
                null, //keyRow.getRowArray(),   // start position - first row
                ScanController.GE,      // startSearchOperation
                null,
                null, //keyRow.getRowArray(),   // stop position - through last row
                ScanController.GT);     // stopSearchOperation

        // it is important for read uncommitted scans to use fetchNext() rather
        // than fetch, so that the fetch happens while latch is held, otherwise
        // the next() might position the scan on a row, but the subsequent
        // fetch() may find the row deleted or purged from the table.
        while(scanController.fetchNext(outRow.getRowArray())){
            cd=(ConglomerateDescriptor)rf.buildDescriptor(
                    outRow,
                    null,
                    this);
            ht.put(cd.getConglomerateNumber(),cd);
        }

        scanController.close();

        return ht;
    }

    /**
     * Get all of the TableDescriptors in the database and hash them
     * by TableId This is useful as a performance optimization for the
     * locking VTIs.  NOTE: This method will scan SYS.SYSTABLES and
     * SYS.SYSSCHEMAS at READ UNCOMMITTED.
     *
     * @param tc TransactionController for the transaction
     * @return A Hashtable with all of the Table descriptors in the database
     * hashed by TableId
     * @throws StandardException Thrown on failure
     */
    @Override
    public Map<UUID, TableDescriptor> hashAllTableDescriptorsByTableId(TransactionController tc) throws StandardException{
        Map<UUID, TableDescriptor> ht=new HashMap<>();
        ScanController scanController;
        ExecRow outRow;
        TabInfoImpl ti=coreInfo[SYSTABLES_CORE_NUM];
        SYSTABLESRowFactory
                rf=(SYSTABLESRowFactory)ti.getCatalogRowFactory();

        outRow=rf.makeEmptyRow();

        scanController=tc.openScan(
                ti.getHeapConglomerate(),       // sys.systable
                false,                          // don't hold open across commit
                0,                              // for read
                TransactionController.MODE_RECORD,// scans whole table.
                TransactionController.ISOLATION_READ_UNCOMMITTED,
                null,                 // all fields as objects
                null,    // start position - first row
                ScanController.GE,              // startSearchOperation
                null,        //scanQualifier,
                null,    //stop position-through last row
                ScanController.GT);             // stopSearchOperation

        // it is important for read uncommitted scans to use fetchNext() rather
        // than fetch, so that the fetch happens while latch is held, otherwise
        // the next() might position the scan on a row, but the subsequent
        // fetch() may find the row deleted or purged from the table.
        while(scanController.fetchNext(outRow.getRowArray())){
            TableDescriptor td=(TableDescriptor)
                    rf.buildDescriptor(
                            outRow,
                            null,
                            this,
                            TransactionController.ISOLATION_READ_UNCOMMITTED);
            ht.put(td.getUUID(),td);
        }
        scanController.close();
        return ht;
    }

    /**
     * Get a ConglomerateDescriptor given its UUID.  If it is an index
     * conglomerate shared by at least another duplicate index, this returns
     * one of the ConglomerateDescriptors for those indexes.
     *
     * @param uuid The UUID
     * @return A ConglomerateDescriptor for the conglomerate.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConglomerateDescriptor getConglomerateDescriptor(UUID uuid) throws StandardException{
        ConglomerateDescriptor[] cds=getConglomerateDescriptors(uuid);
        if(cds.length==0)
            return null;
        return cds[0];
    }

    /**
     * Get an array of ConglomerateDescriptors given the UUID.  If it is a
     * heap conglomerate or an index conglomerate not shared by a duplicate
     * index, the size of the return array is 1. If the uuid argument is null, then
     * this method retrieves descriptors for all of the conglomerates in the database.
     *
     * @param uuid The UUID
     * @return An array of ConglomerateDescriptors for the conglomerate.
     * returns size 0 array if no such conglomerate.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConglomerateDescriptor[] getConglomerateDescriptors(UUID uuid) throws StandardException{
        DataValueDescriptor UUIDStringOrderable;
        TabInfoImpl ti=coreInfo[SYSCONGLOMERATES_CORE_NUM];

        List cdl=newSList();

        if(uuid!=null){
            /* Use UUIDStringOrderable in both start and stop positions for scan */
            UUIDStringOrderable=getIDValueAsCHAR(uuid);

            /* Set up the start/stop position for the scan */
            ExecIndexRow keyRow=exFactory.getIndexableRow(1);
            keyRow.setColumn(1,UUIDStringOrderable);

            getDescriptorViaIndex(SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX1_ID,
                    keyRow,
                    null,
                    ti,
                    null,
                    cdl,
                    false);
        }else{
            getDescriptorViaHeap(null,null,ti,null,cdl);
        }

        ConglomerateDescriptor[] cda=new ConglomerateDescriptor[cdl.size()];
        cdl.toArray(cda);
        return cda;

    }

    /**
     * Get a ConglomerateDescriptor given its conglomerate number.  If it is an
     * index conglomerate shared by at least another duplicate index, this
     * returns one of the ConglomerateDescriptors for those indexes.
     *
     * @param conglomerateNumber The conglomerate number.
     * @return A ConglomerateDescriptor for the conglomerate.  Returns NULL if
     * no such conglomerate.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConglomerateDescriptor getConglomerateDescriptor(long conglomerateNumber) throws StandardException{
        ConglomerateDescriptor result = dataDictionaryCache.conglomerateDescriptorCacheFind(conglomerateNumber);
        if (result != null) {
            return result;
        }
        ConglomerateDescriptor[] cds=getConglomerateDescriptors(conglomerateNumber);
        if(cds.length==0)
            return null;
        result = cds[0];
        dataDictionaryCache.conglomerateDescriptorCacheAdd(conglomerateNumber, result);
        return result;
    }

    /**
     * Get an array of conglomerate descriptors for the given conglomerate
     * number.  If it is a heap conglomerate or an index conglomerate not
     * shared by a duplicate index, the size of the return array is 1.
     *
     * @param conglomerateNumber The number for the conglomerate
     *                           we're interested in
     * @return An array of ConglomerateDescriptors that share the requested
     * conglomerate. Returns size 0 array if no such conglomerate.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConglomerateDescriptor[] getConglomerateDescriptors(long conglomerateNumber) throws StandardException{
        DataValueDescriptor conglomNumberOrderable;
        TabInfoImpl ti=coreInfo[SYSCONGLOMERATES_CORE_NUM];

        conglomNumberOrderable=new SQLLongint(conglomerateNumber);

        ScanQualifier[][] scanQualifier=exFactory.getScanQualifier(1);
        scanQualifier[0][0].setQualifier(
                SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_CONGLOMERATENUMBER-1,    /* column number */
                conglomNumberOrderable,
                Orderable.ORDER_OP_EQUALS,
                false,
                false,
                false);

        ConglomerateDescriptorList cdl=new ConglomerateDescriptorList();
        getDescriptorViaHeap(null,scanQualifier,ti,null,cdl);

        int size=cdl.size();
        ConglomerateDescriptor[] cda=new ConglomerateDescriptor[size];
        for(int index=0;index<size;index++)
            cda[index]=cdl.get(index);

        return cda;
    }


    /**
     * Populate the ConglomerateDescriptorList for the
     * specified TableDescriptor by scanning sysconglomerates.
     * <p/>
     * MT synchronization: it is assumed that the caller has synchronized
     * on the CDL in the given TD.
     *
     * @param td The TableDescriptor.
     * @throws StandardException Thrown on failure
     */
    private void getConglomerateDescriptorsScan(TableDescriptor td) throws StandardException{
        ConglomerateDescriptorList cdl=td.getConglomerateDescriptorList();

        ExecIndexRow keyRow3;
        DataValueDescriptor tableIDOrderable;
        TabInfoImpl ti=coreInfo[SYSCONGLOMERATES_CORE_NUM];

        /* Use tableIDOrderable in both start and stop positions for scan */
        tableIDOrderable=getIDValueAsCHAR(td.getUUID());

        /* Set up the start/stop position for the scan */
        keyRow3=exFactory.getIndexableRow(1);
        keyRow3.setColumn(1,tableIDOrderable);

        getDescriptorViaIndex(
                SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX3_ID,
                keyRow3,
                null,
                ti,
                null,
                cdl,
                false);
    }

    /**
     * Gets a conglomerate descriptor for the named index in the given schema,
     * getting an exclusive row lock on the matching row in
     * sys.sysconglomerates (for DDL concurrency) if requested.
     *
     * @param indexName The name of the index we're looking for
     * @param sd        The schema descriptor
     * @param forUpdate Whether or not to get an exclusive row
     *                  lock on the row in sys.sysconglomerates.
     * @return A ConglomerateDescriptor describing the requested
     * conglomerate. Returns NULL if no such conglomerate.
     * @throws StandardException Thrown on failure
     */
    @Override
    public ConglomerateDescriptor getConglomerateDescriptor(String indexName,
                                                            SchemaDescriptor sd,
                                                            boolean forUpdate) throws StandardException{
        ExecIndexRow keyRow2;
        DataValueDescriptor nameOrderable;
        DataValueDescriptor schemaIDOrderable;
        TabInfoImpl ti=coreInfo[SYSCONGLOMERATES_CORE_NUM];

        nameOrderable=new SQLVarchar(indexName);
        schemaIDOrderable=getIDValueAsCHAR(sd.getUUID());

        /* Set up the start/stop position for the scan */
        keyRow2=exFactory.getIndexableRow(2);
        keyRow2.setColumn(1,nameOrderable);
        keyRow2.setColumn(2,schemaIDOrderable);

        return (ConglomerateDescriptor)getDescriptorViaIndex(SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX2_ID,
                keyRow2,
                null,
                ti,
                null,
                null,
                forUpdate);
    }

    /**
     * Drops a conglomerate descriptor
     *
     * @param conglomerate The ConglomerateDescriptor for the conglomerate
     * @param tc           TransactionController for the transaction
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropConglomerateDescriptor(ConglomerateDescriptor conglomerate,
                                           TransactionController tc) throws StandardException{
        ExecIndexRow keyRow2;
        DataValueDescriptor nameOrderable;
        DataValueDescriptor schemaIDOrderable;
        TabInfoImpl ti=coreInfo[SYSCONGLOMERATES_CORE_NUM];

        nameOrderable=new SQLVarchar(conglomerate.getConglomerateName());
        schemaIDOrderable=getIDValueAsCHAR(conglomerate.getSchemaID());

        /* Set up the start/stop position for the scan */
        keyRow2=exFactory.getIndexableRow(2);
        keyRow2.setColumn(1,nameOrderable);
        keyRow2.setColumn(2,schemaIDOrderable);

        ti.deleteRow(tc,keyRow2,SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX2_ID);

        tc.markConglomerateDropped(conglomerate.getConglomerateNumber());
    }

    /**
     * Drops all conglomerates associated with a table.
     *
     * @param td The TableDescriptor of the table
     * @param tc TransactionController for the transaction
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropAllConglomerateDescriptors(TableDescriptor td,TransactionController tc) throws StandardException{
        ExecIndexRow keyRow3;
        DataValueDescriptor tableIDOrderable;
        TabInfoImpl ti=coreInfo[SYSCONGLOMERATES_CORE_NUM];

        /* Use tableIDOrderable in both start
         * and stop position for index 3 scan.
         */
        tableIDOrderable=getIDValueAsCHAR(td.getUUID());

        /* Set up the start/stop position for the scan */
        keyRow3=exFactory.getIndexableRow(1);
        keyRow3.setColumn(1,tableIDOrderable);

        ConglomerateDescriptorList cds = td.getConglomerateDescriptorList();

        ti.deleteRow(tc,keyRow3,SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX3_ID);

        if (!td.isExternal()) {
            for (ConglomerateDescriptor cd : cds) {
                tc.markConglomerateDropped(cd.getConglomerateNumber());
            }
        }
    }

    /**
     * Update the conglomerateNumber for a ConglomerateDescriptor.
     * This is useful, in 1.3, when doing a bulkInsert into an
     * empty table where we insert into a new conglomerate.
     * (This will go away in 1.4.)
     *
     * @param cd                 The ConglomerateDescriptor
     * @param conglomerateNumber The new conglomerate number
     * @param tc                 The TransactionController to use
     * @throws StandardException Thrown on failure
     */
    @Override
    public void updateConglomerateDescriptor(ConglomerateDescriptor cd,
                                             long conglomerateNumber,
                                             TransactionController tc) throws StandardException{
        ConglomerateDescriptor[] cds=new ConglomerateDescriptor[1];
        cds[0]=cd;
        updateConglomerateDescriptor(cds,conglomerateNumber,tc);
    }

    /**
     * Update all system schemas to have new authorizationId. This is needed
     * while upgrading pre-10.2 databases to 10.2 or later versions. From 10.2,
     * all system schemas would be owned by database owner's authorizationId.
     *
     * @param aid AuthorizationID of Database Owner
     * @param tc  TransactionController to use
     * @throws StandardException Thrown on failure
     */
    @Override
    public void updateSystemSchemaAuthorization(String aid,TransactionController tc) throws StandardException{
        updateSchemaAuth(SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME,aid,tc);
        updateSchemaAuth(SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME,aid,tc);
        updateSchemaAuth(SchemaDescriptor.IBM_SYSTEM_ADM_SCHEMA_NAME,aid,tc);

        updateSchemaAuth(SchemaDescriptor.IBM_SYSTEM_CAT_SCHEMA_NAME,aid,tc);
        updateSchemaAuth(SchemaDescriptor.IBM_SYSTEM_FUN_SCHEMA_NAME,aid,tc);
        updateSchemaAuth(SchemaDescriptor.IBM_SYSTEM_PROC_SCHEMA_NAME,aid,tc);
        updateSchemaAuth(SchemaDescriptor.IBM_SYSTEM_STAT_SCHEMA_NAME,aid,tc);
        updateSchemaAuth(SchemaDescriptor.IBM_SYSTEM_NULLID_SCHEMA_NAME,aid,tc);

        updateSchemaAuth(SchemaDescriptor.STD_SQLJ_SCHEMA_NAME,aid,tc);
        updateSchemaAuth(SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME,aid,tc);
        updateSchemaAuth(SchemaDescriptor.STD_SYSTEM_UTIL_SCHEMA_NAME,aid,tc);

        // now reset our understanding of who owns the database
        resetDatabaseOwner(tc);
    }

    /**
     * Update authorizationId of specified schemaName
     *
     * @param schemaName      Schema Name of system schema
     * @param authorizationId authorizationId of new schema owner
     * @param tc              The TransactionController to use
     * @throws StandardException Thrown on failure
     */
    public void updateSchemaAuth(String schemaName,String authorizationId,TransactionController tc) throws StandardException{
        ExecIndexRow keyRow;
        DataValueDescriptor schemaNameOrderable;
        TabInfoImpl ti=coreInfo[SYSSCHEMAS_CORE_NUM];

        /* Use schemaNameOrderable in both start
         * and stop position for index 1 scan.
         */
        schemaNameOrderable=new SQLVarchar(schemaName);

        /* Set up the start/stop position for the scan */
        keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,schemaNameOrderable);

        SYSSCHEMASRowFactory rf=(SYSSCHEMASRowFactory)ti.getCatalogRowFactory();
        ExecRow row=rf.makeEmptyRow();

        row.setColumn(SYSSCHEMASRowFactory.SYSSCHEMAS_SCHEMAAID,new SQLVarchar(authorizationId));

        boolean[] bArray={false,false};

        int[] colsToUpdate={SYSSCHEMASRowFactory.SYSSCHEMAS_SCHEMAAID};

        ti.updateRow(keyRow,row,SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID,bArray,colsToUpdate,tc);
    }

    /**
     * Update the conglomerateNumber for an array of ConglomerateDescriptors.
     * In case of more than one ConglomerateDescriptor, each descriptor
     * should be updated separately, conglomerate id is not same for all
     * the descriptors. Even when indexes are sharing the same
     * conglomerate(conglomerate number), conglomerate ids are unique.
     * <p/>
     * This is useful, in 1.3, when doing a bulkInsert into an
     * empty table where we insert into a new conglomerate.
     * (This will go away in 1.4.)
     *
     * @param cds                The array of ConglomerateDescriptors
     * @param conglomerateNumber The new conglomerate number
     * @param tc                 The TransactionController to use
     * @throws StandardException Thrown on failure
     */
    @Override
    public void updateConglomerateDescriptor(ConglomerateDescriptor[] cds,
                                             long conglomerateNumber,
                                             TransactionController tc) throws StandardException{
        ExecIndexRow keyRow1;
        ExecRow row;
        DataValueDescriptor conglomIDOrderable;
        TabInfoImpl ti=coreInfo[SYSCONGLOMERATES_CORE_NUM];
        SYSCONGLOMERATESRowFactory rf=(SYSCONGLOMERATESRowFactory)ti.getCatalogRowFactory();
        boolean[] bArray={false,false,false};

        for(ConglomerateDescriptor cd : cds){
            /* Use conglomIDOrderable in both start
             * and stop position for index 1 scan.
             */
            conglomIDOrderable=getIDValueAsCHAR(cd.getUUID());

            tc.markConglomerateDropped(cd.getConglomerateNumber());

            /* Set up the start/stop position for the scan */
            keyRow1=exFactory.getIndexableRow(1);
            keyRow1.setColumn(1,conglomIDOrderable);

            cd.setConglomerateNumber(conglomerateNumber);
            // build the row to be stuffed into SYSCONGLOMERATES.
            row=rf.makeRow(cd,null);

            // update row in catalog (no indexes)
            ti.updateRow(keyRow1,row,SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX1_ID,bArray,null,tc);
        }
    }


    /**
     * Gets a list of the dependency descriptors for the given dependent's id.
     *
     * @param dependentID The ID of the dependent we're interested in
     * @return List            Returns a list of DependencyDescriptors.
     * Returns an empty List if no stored dependencies for the
     * dependent's ID.
     * @throws StandardException Thrown on failure
     */
    @Override
    public List<DependencyDescriptor> getDependentsDescriptorList(String dependentID) throws StandardException{
        List<DependencyDescriptor> ddlList=newSList();
        DataValueDescriptor dependentIDOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSDEPENDS_CATALOG_NUM);

        /* Use dependentIDOrderable in both start and stop positions for scan */
        dependentIDOrderable=new SQLChar(dependentID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,dependentIDOrderable);

        getDescriptorViaIndex(SYSDEPENDSRowFactory.SYSDEPENDS_INDEX1_ID,keyRow,null,ti,null,ddlList,false);

        return ddlList;
    }

    /**
     * Gets a list of the dependency descriptors for the given provider's id.
     *
     * @param providerID The ID of the provider we're interested in
     * @return List            Returns a list of DependencyDescriptors.
     * Returns an empty List if no stored dependencies for the
     * provider's ID.
     * @throws StandardException Thrown on failure
     */
    @Override
    public List<DependencyDescriptor> getProvidersDescriptorList(String providerID) throws StandardException{
        List<DependencyDescriptor> ddlList=newSList();
        DataValueDescriptor providerIDOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSDEPENDS_CATALOG_NUM);

        /* Use providerIDOrderable in both start and stop positions for scan */
        providerIDOrderable=new SQLChar(providerID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,providerIDOrderable);

        getDescriptorViaIndex(SYSDEPENDSRowFactory.SYSDEPENDS_INDEX2_ID,keyRow,null,ti,null,ddlList,false);
        return ddlList;
    }

    /**
     * Build and return an List with DependencyDescriptors for
     * all of the stored dependencies.
     * This is useful for consistency checking.
     *
     * @return List        List of all DependencyDescriptors.
     * @throws StandardException Thrown on failure
     */
    @Override
    public List<DependencyDescriptor> getAllDependencyDescriptorsList() throws StandardException{
        ScanController scanController;
        TransactionController tc;
        ExecRow outRow;
        List<DependencyDescriptor> ddl=newSList();
        TabInfoImpl ti=getNonCoreTI(SYSDEPENDS_CATALOG_NUM);
        SYSDEPENDSRowFactory rf=(SYSDEPENDSRowFactory)ti.getCatalogRowFactory();


        // Get the current transaction controller
        tc=getTransactionCompile();

        outRow=rf.makeEmptyRow();

        scanController=tc.openScan(
                ti.getHeapConglomerate(),  // conglomerate to open
                false, // don't hold open across commit
                0, // for read
                TransactionController.MODE_TABLE,   // scans entire table.
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,                     // all fields as objects
                null,   // start position - first row
                ScanController.GE,      // startSearchOperation
                null,
                null,   // stop position - through last row
                ScanController.GT);     // stopSearchOperation

        while(scanController.fetchNext(outRow.getRowArray())){
            DependencyDescriptor dependencyDescriptor;

            dependencyDescriptor=(DependencyDescriptor)rf.buildDescriptor(outRow,null,this);

            ddl.add(dependencyDescriptor);
        }

        scanController.close();

        return ddl;
    }

    /**
     * Drop a single dependency from the data dictionary.
     *
     * @param dd The DependencyDescriptor.
     * @param tc TransactionController for the transaction
     * @throws StandardException Thrown on failure
     */
    public void dropStoredDependency(DependencyDescriptor dd,TransactionController tc) throws StandardException{
        if (!tc.isElevated()) {
            StandardException.plainWrapException(new IOException("dropStoredDependency: not writeable"));
        }
        ExecIndexRow keyRow1;
        UUID dependentID=dd.getUUID();
        UUID providerID=dd.getProviderID();
        DataValueDescriptor dependentIDOrderable=getIDValueAsCHAR(dependentID);
        TabInfoImpl ti=getNonCoreTI(SYSDEPENDS_CATALOG_NUM);

        /* Use dependentIDOrderable in both start
         * and stop position for index 1 scan.
         */
        keyRow1=exFactory.getIndexableRow(1);
        keyRow1.setColumn(1,dependentIDOrderable);

        // only drop the rows which have this providerID
        TupleFilter filter=new DropDependencyFilter(providerID);

        ti.deleteRows(tc,
                keyRow1,                // start row
                ScanController.GE,
                null,                //qualifier
                filter,                // filter on base row
                keyRow1,                // stop row
                ScanController.GT,
                SYSDEPENDSRowFactory.SYSDEPENDS_INDEX1_ID);

    }


    /**
     * Remove all of the stored dependencies for a given dependent's ID
     * from the data dictionary.
     *
     * @param dependentsUUID Dependent's uuid
     * @param tc             TransactionController for the transaction
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropDependentsStoredDependencies(UUID dependentsUUID,TransactionController tc) throws StandardException{
        if (!tc.isElevated()) {
            StandardException.plainWrapException(new IOException("addStoreDependency: not writeable"));
        }

        dropDependentsStoredDependencies(dependentsUUID,tc,true);
    }

    @Override
    public void dropDependentsStoredDependencies(UUID dependentsUUID,
                                                 TransactionController tc,
                                                 boolean wait) throws StandardException{
        ExecIndexRow keyRow1;
        DataValueDescriptor dependentIDOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSDEPENDS_CATALOG_NUM);

        /* Use dependentIDOrderable in both start
         * and stop position for index 1 scan.
         */
        dependentIDOrderable=getIDValueAsCHAR(dependentsUUID);

        /* Set up the start/stop position for the scan */
        keyRow1=exFactory.getIndexableRow(1);
        keyRow1.setColumn(1,dependentIDOrderable);

        ti.deleteRow(tc,keyRow1,SYSDEPENDSRowFactory.SYSDEPENDS_INDEX1_ID,wait);
    }

    /**
     * Get the UUID Factory.  (No need to make the UUIDFactory a module.)
     *
     * @return UUIDFactory    The UUID Factory for this DataDictionary.
     */
    @Override
    public UUIDFactory getUUIDFactory(){
        return uuidFactory;
    }

    /**
     * Get the alias descriptor for an ANSI UDT.
     *
     * @param tc  The transaction to use: if null, use the compilation transaction
     * @param dtd The UDT's type descriptor
     * @return The UDT's alias descriptor if it is an ANSI UDT; null otherwise.
     */
    @Override
    public AliasDescriptor getAliasDescriptorForUDT(TransactionController tc,
                                                    DataTypeDescriptor dtd) throws StandardException{
        if(tc==null){
            tc=getTransactionCompile();
        }

        if(dtd==null){
            return null;
        }

        BaseTypeIdImpl btii=dtd.getTypeId().getBaseTypeId();
        if(!btii.isAnsiUDT()){
            return null;
        }

        SchemaDescriptor sd=getSchemaDescriptor(btii.getSchemaName(),tc,true);

        return getAliasDescriptor(sd.getUUID().toString(),btii.getUnqualifiedName(),AliasInfo.ALIAS_NAME_SPACE_UDT_AS_CHAR);
    }

    /**
     * Get a AliasDescriptor given its UUID.
     *
     * @param uuid The UUID
     * @return The AliasDescriptor for the alias.
     * @throws StandardException Thrown on failure
     */
    @Override
    public AliasDescriptor getAliasDescriptor(UUID uuid) throws StandardException{
        DataValueDescriptor UUIDStringOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSALIASES_CATALOG_NUM);

        /* Use UUIDStringOrderable in both start and stop positions for scan */
        UUIDStringOrderable=getIDValueAsCHAR(uuid);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,UUIDStringOrderable);

        return (AliasDescriptor)getDescriptorViaIndex(SYSALIASESRowFactory.SYSALIASES_INDEX2_ID,
                keyRow,
                null,
                ti,
                null,
                null,
                false);
    }

    /**
     * Get a AliasDescriptor by alias name and name space.
     * NOTE: caller responsible for handling no match.
     *
     * @param schemaId  schema identifier
     * @param aliasName The alias name.
     * @param nameSpace The alias type.
     * @return AliasDescriptor    AliasDescriptor for the alias name and name space
     * @throws StandardException Thrown on failure
     */
    @Override
    public AliasDescriptor getAliasDescriptor(String schemaId,String aliasName,char nameSpace) throws StandardException{
        DataValueDescriptor aliasNameOrderable;
        DataValueDescriptor nameSpaceOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSALIASES_CATALOG_NUM);

        /* Use aliasNameOrderable and aliasTypeOrderable in both start
         * and stop position for scan.
         */
        aliasNameOrderable=new SQLVarchar(aliasName);
        char[] charArray=new char[1];
        charArray[0]=nameSpace;
        nameSpaceOrderable=new SQLChar(new String(charArray));

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(3);
        keyRow.setColumn(1,new SQLChar(schemaId));
        keyRow.setColumn(2,aliasNameOrderable);
        keyRow.setColumn(3,nameSpaceOrderable);

        return (AliasDescriptor)getDescriptorViaIndex(SYSALIASESRowFactory.SYSALIASES_INDEX1_ID,
                keyRow,
                null,
                ti,
                null,
                null,
                false);
    }

    /**
     * Get the list of routines matching the schema and routine name.
     * While we only support a single alias for a given name,namespace just
     * return a list of zero or one item.
     * If the schema is SYSFUN then do not use the system catalogs,
     * but instead look up the routines from the in-meomry table driven
     * by the contents of SYSFUN_FUNCTIONS.
     */
    @Override
    public List<AliasDescriptor> getRoutineList(String schemaID,String routineName,char nameSpace) throws StandardException{

        // Special in-memory table lookup for SYSFUN
        if(schemaID.equals(SchemaDescriptor.SYSFUN_SCHEMA_UUID) && nameSpace==AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR){
            // We expect to find just a single function, since we currently
            // don't support multiple routines with the same name, but use a
            // list to support future extension.
            List<AliasDescriptor> list=new ArrayList<>(1);

            for(int f=0;f<DataDictionaryImpl.SYSFUN_FUNCTIONS.length;f++){
                String[] details=DataDictionaryImpl.SYSFUN_FUNCTIONS[f];
                String name=details[0];
                if(!name.equals(routineName))
                    continue;

                AliasDescriptor ad=sysfunDescriptors[f];
                if(ad==null){
                    // details[1] Return type
                    TypeDescriptor rt=
                            DataTypeDescriptor.getBuiltInDataTypeDescriptor(details[1]).getCatalogType();

                    boolean isDeterministic=Boolean.valueOf(details[SYSFUN_DETERMINISTIC_INDEX]);

                    // Determine the number of arguments (could be zero).
                    int paramCount=details.length-SYSFUN_FIRST_PARAMETER_INDEX;
                    TypeDescriptor[] pt=new TypeDescriptor[paramCount];
                    String[] paramNames=new String[paramCount];
                    int[] paramModes=new int[paramCount];
                    for(int i=0;i<paramCount;i++){
                        pt[i]=DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                                details[SYSFUN_FIRST_PARAMETER_INDEX+i]).getCatalogType();
                        paramNames[i]="P"+(i+1); // Dummy names
                        // All parameters must be IN.
                        paramModes[i]=JDBC30Translation.PARAMETER_MODE_IN;
                    }

                    // details[3] = java method
                    RoutineAliasInfo ai=new RoutineAliasInfo(details[3],
                            "JAVA", paramCount, paramNames,
                            pt,paramModes,0,
                            RoutineAliasInfo.PS_JAVA,RoutineAliasInfo.NO_SQL,isDeterministic,
                            false, /* hasDefinersRights */
                            false,rt, null);

                    // details[2] = class name
                    ad=new AliasDescriptor(this,uuidFactory.createUUID(),name,
                            uuidFactory.recreateUUID(schemaID),
                            details[2],AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR,
                            AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR,
                            true,ai,null);

                    sysfunDescriptors[f]=ad;
                }
                list.add(ad);
            }
            return list;
        }

        AliasDescriptor ad=getAliasDescriptor(schemaID,routineName,nameSpace);
        return ad==null?Collections.<AliasDescriptor>emptyList():Collections.singletonList(ad);
    }

    /**
     * Drop a AliasDescriptor from the DataDictionary
     *
     * @param ad The AliasDescriptor to drop
     * @param tc The TransactionController
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropAliasDescriptor(AliasDescriptor ad,TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSALIASES_CATALOG_NUM);

        /* Use aliasNameOrderable and nameSpaceOrderable in both start
         * and stop position for index 1 scan.
         */

        char[] charArray=new char[1];
        charArray[0]=ad.getNameSpace();

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow1=exFactory.getIndexableRow(3);
        keyRow1.setColumn(1,getIDValueAsCHAR(ad.getSchemaUUID()));
        keyRow1.setColumn(2,new SQLVarchar(ad.getDescriptorName()));
        keyRow1.setColumn(3,new SQLChar(new String(charArray)));

        ti.deleteRow(tc,keyRow1,SYSALIASESRowFactory.SYSALIASES_INDEX1_ID);

    }

    @Override
    public void updateUser(UserDescriptor newDescriptor,TransactionController tc) throws StandardException{
        ExecIndexRow keyRow;
        TabInfoImpl ti=getNonCoreTI(SYSUSERS_CATALOG_NUM);

        /* Set up the start/stop position for the scan */
        keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,new SQLVarchar(newDescriptor.getUserName()));

        // this zeroes out the password in the UserDescriptor
        ExecRow row=ti.getCatalogRowFactory().makeRow(newDescriptor,null);

        boolean[] bArray={false};

        int[] colsToUpdate={
                SYSUSERSRowFactory.HASHINGSCHEME_COL_NUM,
                SYSUSERSRowFactory.PASSWORD_COL_NUM,
                SYSUSERSRowFactory.LASTMODIFIED_COL_NUM,
        };

        ti.updateRow(keyRow,row,SYSUSERSRowFactory.SYSUSERS_INDEX1_ID,bArray,colsToUpdate,tc);
    }

    @Override
    public UserDescriptor getUser(String userName) throws StandardException{
        //
        // No sense looking for the SYSUSERS congomerate until the database
        // is hard-upgraded to 10.9 or later.
        //
        dictionaryVersion.checkVersion(DD_VERSION_DERBY_10_9,"NATIVE AUTHENTICATION");

        ExecIndexRow keyRow;
        TabInfoImpl ti=getNonCoreTI(SYSUSERS_CATALOG_NUM);
        /* Set up the start/stop position for the scan */
        keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,new SQLVarchar(userName));
        return (UserDescriptor)getDescriptorViaIndex(SYSUSERSRowFactory.SYSUSERS_INDEX1_ID,keyRow,null,ti,null,null,false);
    }

    @Override
    public void dropUser(String userName,TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSUSERS_CATALOG_NUM);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow1=exFactory.getIndexableRow(1);
        keyRow1.setColumn(1,new SQLVarchar(userName));

        ti.deleteRow(tc,keyRow1,SYSUSERSRowFactory.SYSUSERS_INDEX1_ID);
    }

    //
    // class implementation
    //

    /**
     * Initialize system catalogs. This is where we perform upgrade. It is our
     * pious hope that we won't ever have to upgrade the core catalogs, other than
     * to add fields inside Formatable columns in these catalogs.
     * <p/>
     * If we do have to upgrade the core catalogs, then we may need to move the
     * loadCatalog calls into the upgrade machinery. It's do-able, just not pretty.
     *
     * @param tc TransactionController
     * @throws StandardException Thrown on error
     */
    protected void loadDictionaryTables(TransactionController tc,Properties startParams) throws StandardException{
        // load the core catalogs first
        loadCatalogs(coreInfo);

        dictionaryVersion=(DD_Version)tc.getProperty(DataDictionary.CORE_DATA_DICTIONARY_VERSION);

        // NATIVE authentication allowed if the database is at least at level 10.9
        boolean nativeAuthenticationEnabled=PropertyUtil.nativeAuthenticationEnabled(startParams);
        if(nativeAuthenticationEnabled){
            dictionaryVersion.checkVersion(DD_VERSION_DERBY_10_9,"NATIVE AUTHENTICATION");
        }

        resetDatabaseOwner(tc);

        softwareVersion.upgradeIfNeeded(dictionaryVersion,tc,startParams);
    }

    /**
     * Initialize system procedures.
     * This is where Derby updates (reloads) the system stored procedures
     * when the <code>db.language.updateSystemProcs</code> system property is set to true.
     *
     * @param tc TransactionController
     * @throws StandardException Thrown on error
     */
    protected void updateSystemProcedures(TransactionController tc) throws StandardException{
        // Update (or create) the system stored procedures if requested.
        if(updateSystemProcs){
            createOrUpdateAllSystemProcedures(tc);
        }
        // Only update the system procedures once.  Otherwise, each time an ij session is created, the system procedures will be dropped/created again.
        // It would be better if it was possible to detect when the database is being booted during server startup versus the database being booted during ij startup.
        updateSystemProcs=false;
    }

    /**
     * Reset the database owner according to what is stored in the catalogs.
     * This can change at upgrade time so we have factored this logic into
     * a separately callable method.
     *
     * @param tc TransactionController
     * @throws StandardException Thrown on error
     */
    public void resetDatabaseOwner(TransactionController tc) throws StandardException{
        SchemaDescriptor sd=locateSchemaRow(SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME,tc);
        authorizationDatabaseOwner=sd.getAuthorizationId();

        systemSchemaDesc.setAuthorizationId(authorizationDatabaseOwner);
        sysIBMSchemaDesc.setAuthorizationId(authorizationDatabaseOwner);
        sysIBMADMSchemaDesc.setAuthorizationId(authorizationDatabaseOwner);
        systemUtilSchemaDesc.setAuthorizationId(authorizationDatabaseOwner);
    }

    /**
     * Initialize indices for an array of catalogs
     *
     * @throws StandardException Thrown on error
     */
    public void loadCatalogs(TabInfoImpl[] catalogArray) throws StandardException{
        int ictr;
        int numIndexes;
        int indexCtr;
        TabInfoImpl catalog;
        int catalogCount=catalogArray.length;

        /* Initialize the various variables associated with index scans of these catalogs */
        for(ictr=0;ictr<catalogCount;ictr++){
            // NOTE: This only works for core catalogs, which are initialized
            // up front.
            catalog=catalogArray[ictr];

            numIndexes=catalog.getNumberOfIndexes();

            if(numIndexes>0){
                for(indexCtr=0;indexCtr<numIndexes;indexCtr++){
                    initSystemIndexVariables(catalog,indexCtr);
                }
            }
        }

    }

    /*
    ** Methods related to create
    */

    /**
     * Create all the required dictionary tables. Any classes that extend this class
     * and need to create new tables should override this method, and then
     * call this method as the first action in the new method, e.g.
     * <PRE>
     * protected Configuration createDictionaryTables(Configuration cfg, TransactionController tc,
     * DataDescriptorGenerator ddg)
     * throws StandardException
     * {
     * super.createDictionaryTables(params, tc, ddg);
     * <p/>
     * ...
     * }
     * </PRE>
     */
    protected class NonCoreCreation implements Runnable{
        private int noncoreCtr;
        private TransactionController tc;
        private ExecutionContext ec;

        public NonCoreCreation(int noncoreCtr,TransactionController tc,ExecutionContext ec){
            this.noncoreCtr=noncoreCtr;
            this.tc=tc;
            this.ec=ec;
        }

        @Override
        public void run(){
            try{
                ContextManager cm=ContextService.getFactory().newContextManager();
                cm.pushContext(ec);
                ContextService.getFactory().setCurrentContextManager(cm);
                int catalogNumber=noncoreCtr+NUM_CORE;
                boolean isDummy=(catalogNumber==SYSDUMMY1_CATALOG_NUM);
                SchemaDescriptor sd = systemSchemaDesc;
                if (catalogNumber == SYSMONGETCONNECTION_CATALOG_NUM)
                    sd = sysIBMADMSchemaDesc;
                else if (isDummy)
                    sd = sysIBMSchemaDesc;
                TabInfoImpl ti=getNonCoreTIByNumber(catalogNumber);
                String version = catalogVersions.get(catalogNumber);
                if (ti != null) {
                    makeCatalog(ti, sd, tc, version);
                }
                if(isDummy)
                    populateSYSDUMMY1(tc);
                // Clear the table entry for this non-core table,
                // to allow it to be garbage-collected. The idea
                // is that a running database might never need to
                // reference a non-core table after it was created.
                clearNoncoreTable(noncoreCtr);
            }catch(Exception e){
                e.printStackTrace();
                SanityManager.THROWASSERT("Dictionary Table Failure - exiting");
            }
        }
    }


    protected void createDictionaryTables(Properties params,
                                          TransactionController tc,
                                          DataDescriptorGenerator ddg) throws StandardException{
        /*
        ** Create a new schema descriptor -- with no args
        ** creates the system schema descriptor in which
        ** all tables reside (SYS)
        */
        systemSchemaDesc=newSystemSchemaDesc(SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME,SchemaDescriptor.SYSTEM_SCHEMA_UUID);

        /* Create the core tables and generate the UUIDs for their
         * heaps (before creating the indexes).
         * RESOLVE - This loop will eventually drive all of the
         * work for creating the core tables.
         */
        ExecutionContext ec=(ExecutionContext)ContextService.getContext(ExecutionContext.CONTEXT_ID);

        ExecutorService executor=Executors.newFixedThreadPool(4);
        for(int coreCtr=0;coreCtr<NUM_CORE;coreCtr++){
            executor.execute(new CoreCreation(coreCtr,tc,ec));
        }
        executor.shutdown();
        // Wait until all threads are finish
        while(!executor.isTerminated()){
            try{
                executor.awaitTermination(Long.MAX_VALUE,TimeUnit.SECONDS);
            }catch(InterruptedException e){
                System.out.println("Interrupted");
            }
        }
        for(int coreCtr=0;coreCtr<NUM_CORE;coreCtr++){
            // bootstrap indexes on core tables before bootstraping the tables themselves
            if(coreInfo[coreCtr].getNumberOfIndexes()>0){
                TabInfoImpl ti=coreInfo[coreCtr];
                bootStrapSystemIndexes(systemSchemaDesc,tc,ddg,ti);
            }
        }

        // bootstrap the core tables into the data dictionary
        for(int ictr=0;ictr<NUM_CORE;ictr++){
            /* RESOLVE - need to do something with COLUMNTYPE in following table creating code */
            TabInfoImpl ti=coreInfo[ictr];
            addSystemTableToDictionary(ti,systemSchemaDesc,tc,ddg);
        }

        // Add the bootstrap information to the configuration
        params.put(CFG_SYSTABLES_ID,Long.toString(coreInfo[SYSTABLES_CORE_NUM].getHeapConglomerate()));
        params.put(CFG_SYSTABLES_INDEX1_ID,Long.toString(
                coreInfo[SYSTABLES_CORE_NUM].getIndexConglomerate(
                        SYSTABLESRowFactory.SYSTABLES_INDEX1_ID)));
        params.put(CFG_SYSTABLES_INDEX2_ID,
                Long.toString(
                        coreInfo[SYSTABLES_CORE_NUM].getIndexConglomerate(
                                SYSTABLESRowFactory.SYSTABLES_INDEX2_ID)));

        params.put(CFG_SYSCOLUMNS_ID,Long.toString(coreInfo[SYSCOLUMNS_CORE_NUM].getHeapConglomerate()));
        params.put(CFG_SYSCOLUMNS_INDEX1_ID,
                Long.toString(
                        coreInfo[SYSCOLUMNS_CORE_NUM].getIndexConglomerate(
                                SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID)));
        params.put(CFG_SYSCOLUMNS_INDEX2_ID,
                Long.toString(
                        coreInfo[SYSCOLUMNS_CORE_NUM].getIndexConglomerate(
                                SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX2_ID)));

        params.put(CFG_SYSCONGLOMERATES_ID,Long.toString(coreInfo[SYSCONGLOMERATES_CORE_NUM].getHeapConglomerate()));
        params.put(CFG_SYSCONGLOMERATES_INDEX1_ID,
                Long.toString(
                        coreInfo[SYSCONGLOMERATES_CORE_NUM].getIndexConglomerate(
                                SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX1_ID)));
        params.put(CFG_SYSCONGLOMERATES_INDEX2_ID,
                Long.toString(
                        coreInfo[SYSCONGLOMERATES_CORE_NUM].getIndexConglomerate(
                                SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX2_ID)));
        params.put(CFG_SYSCONGLOMERATES_INDEX3_ID,
                Long.toString(
                        coreInfo[SYSCONGLOMERATES_CORE_NUM].getIndexConglomerate(
                                SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX3_ID)));

        params.put(CFG_SYSSCHEMAS_ID,Long.toString(coreInfo[SYSSCHEMAS_CORE_NUM].getHeapConglomerate()));
        params.put(CFG_SYSSCHEMAS_INDEX1_ID,
                Long.toString(
                        coreInfo[SYSSCHEMAS_CORE_NUM].getIndexConglomerate(
                                SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID)));
        params.put(CFG_SYSSCHEMAS_INDEX2_ID,
                Long.toString(
                        coreInfo[SYSSCHEMAS_CORE_NUM].getIndexConglomerate(
                                SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX2_ID)));

        //Add the SYSIBM Schema
        sysIBMSchemaDesc=addSystemSchema(SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME,SchemaDescriptor.SYSIBM_SCHEMA_UUID,tc);

        //Add the SYSIBMADM Schema
        sysIBMADMSchemaDesc=addSystemSchema(SchemaDescriptor.IBM_SYSTEM_ADM_SCHEMA_NAME,SchemaDescriptor.SYSIBMADM_SCHEMA_UUID,tc);

        /* Create the non-core tables and generate the UUIDs for their
         * heaps (before creating the indexes).
         * RESOLVE - This loop will eventually drive all of the
         * work for creating the non-core tables.
         */
        ExecutorService nonCoreExecutor=Executors.newFixedThreadPool(10);
        for(int noncoreCtr=0;noncoreCtr<NUM_NONCORE;noncoreCtr++){
            nonCoreExecutor.execute(new NonCoreCreation(noncoreCtr,tc,ec));
        }

        nonCoreExecutor.shutdown();
        while(!nonCoreExecutor.isTerminated()){
            try{
                nonCoreExecutor.awaitTermination(Long.MAX_VALUE,TimeUnit.SECONDS);
            }catch(InterruptedException ie){
                Thread.interrupted();
            }
        }

        //Add ths System Schema
        addDescriptor(systemSchemaDesc,null,SYSSCHEMAS_CATALOG_NUM,false,tc,false);


        // Add the following system Schema's to be compatible with DB2, 
        // currently Derby does not use them, but by creating them as
        // system schema's it will insure applications can't create them,
        // drop them, or create objects in them.  This set includes:
        //     SYSCAT
        //     SYSFUN
        //     SYSPROC
        //     SYSSTAT
        //     NULLID

        //Add the SYSCAT Schema 
        addSystemSchema(SchemaDescriptor.IBM_SYSTEM_CAT_SCHEMA_NAME,SchemaDescriptor.SYSCAT_SCHEMA_UUID,tc);

        //Add the SYSFUN Schema
        addSystemSchema(SchemaDescriptor.IBM_SYSTEM_FUN_SCHEMA_NAME,SchemaDescriptor.SYSFUN_SCHEMA_UUID,tc);

        //Add the SYSPROC Schema
        addSystemSchema(SchemaDescriptor.IBM_SYSTEM_PROC_SCHEMA_NAME,SchemaDescriptor.SYSPROC_SCHEMA_UUID,tc);

        //Add the SYSSTAT Schema
        addSystemSchema(SchemaDescriptor.IBM_SYSTEM_STAT_SCHEMA_NAME,SchemaDescriptor.SYSSTAT_SCHEMA_UUID,tc);

        //Add the NULLID Schema
        addSystemSchema(SchemaDescriptor.IBM_SYSTEM_NULLID_SCHEMA_NAME,SchemaDescriptor.NULLID_SCHEMA_UUID,tc);

        //Add the SQLJ Schema
        addSystemSchema(SchemaDescriptor.STD_SQLJ_SCHEMA_NAME,SchemaDescriptor.SQLJ_SCHEMA_UUID,tc);

        //Add the SYSCS_DIAG Schema
        addSystemSchema(SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME,SchemaDescriptor.SYSCS_DIAG_SCHEMA_UUID,tc);

        //Add the SYSCS_UTIL Schema
        addSystemSchema(SchemaDescriptor.STD_SYSTEM_UTIL_SCHEMA_NAME,SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,tc);

        //Add the SPLICE schema
        SchemaDescriptor appSchemaDesc=new SchemaDescriptor(this,
                SchemaDescriptor.STD_DEFAULT_SCHEMA_NAME,
                SchemaDescriptor.DEFAULT_USER_NAME,
                uuidFactory.recreateUUID(SchemaDescriptor.DEFAULT_SCHEMA_UUID),
                false);

        addDescriptor(appSchemaDesc,null,SYSSCHEMAS_CATALOG_NUM,false,tc,false);

        //Add the SYSVW schema
        sysViewSchemaDesc = addSystemSchema(SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME,SchemaDescriptor.SYSVW_SCHEMA_UUID,tc);
    }

    /**
     * Add a system schema to the database.
     * <p/>
     *
     * @param schema_name name of the schema to add.
     * @throws StandardException Standard exception policy.
     */
    protected SchemaDescriptor addSystemSchema(String schema_name,
                                             String schema_uuid,
                                             TransactionController tc) throws StandardException{
        // create the descriptor
        UUID oid=uuidFactory.recreateUUID(schema_uuid);
        SchemaDescriptor schema_desc=new SchemaDescriptor(this,schema_name,authorizationDatabaseOwner,oid,true);

        // add it to the catalog.
        addDescriptor(schema_desc,null,SYSSCHEMAS_CATALOG_NUM,false,tc,false);

        return (schema_desc);
    }

    /**
     * called by the upgrade code (dd_xena etc) to add a new system catalog.
     *
     * @param tc            TransactionController to use.
     * @param catalogNumber catalogNumber
     */
    protected void upgradeMakeCatalog(TransactionController tc,int catalogNumber) throws StandardException{
        TabInfoImpl ti;
        if(catalogNumber>=NUM_CORE)
            ti=getNonCoreTIByNumber(catalogNumber);
        else
            ti=coreInfo[catalogNumber];

        String version = catalogVersions.get(catalogNumber);
        makeCatalog(ti,(catalogNumber==SYSDUMMY1_CATALOG_NUM)?getSysIBMSchemaDescriptor():getSystemSchemaDescriptor(),
                tc, version);
    }


    /**
     * Called by the upgrade code to upgrade the way we store jar files in the
     * database.<p/>
     * We now use UUID as part of the file name to avoid problems with path
     * delimiters. Also, we henceforth use no schema subdirectories since there
     * is no chance of name collision with the UUID.
     *
     * @param tc TransactionController to use.
     */
    protected void upgradeJarStorage(TransactionController tc) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSFILES_CATALOG_NUM);
        SYSFILESRowFactory rf=(SYSFILESRowFactory)ti.getCatalogRowFactory();

        ExecRow outRow=rf.makeEmptyRow();

        /*
        ** Table scan
        */

        Set<String> schemas=new HashSet<>();

        try(ScanController scanController=tc.openScan(
                ti.getHeapConglomerate(),     // conglomerate to open
                false,                        // don't hold open across commit
                0,                            // for read
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,      // all fields as objects
                null, // start position - first row
                0,                            // startSearchOperation - none
                null,         // scanQualifier,
                null, // stop position -through last row
                0)){
            while(scanController.fetchNext(outRow.getRowArray())){
                FileInfoDescriptor fid=(FileInfoDescriptor)rf.buildDescriptor(outRow,null,this);
                schemas.add(fid.getSchemaDescriptor().getSchemaName());
                JarUtil.upgradeJar(tc,fid);
            }
        }

        Iterator i=schemas.iterator();
        FileResource fh=tc.getFileHandler();

        // remove those directories with their contents
        while(i.hasNext()){
            fh.removeJarDir(FileResource.JAR_DIRECTORY_NAME+File.separatorChar+i.next());
        }
    }

    /**
     * The dirty work of creating a catalog.
     *
     * @param ti TabInfoImpl describing catalog to create.
     * @param sd Schema to create catalogs in.
     * @param tc Transaction context.
     * @throws StandardException Standard Derby error policy
     */
    public void makeCatalog(TabInfoImpl ti,SchemaDescriptor sd,TransactionController tc, String version) throws StandardException{
        makeCatalog(ti,sd,tc,null, version);
    }

    public void makeCatalog(TabInfoImpl ti,SchemaDescriptor sd,
                            TransactionController tc,ColumnOrdering[] columnOrder, String version) throws StandardException{
        DataDescriptorGenerator ddg=getDataDescriptorGenerator();

        Properties heapProperties=new Properties();
        heapProperties.setProperty("tableDisplayName", ti.getTableName());
        if (version != null) {
            heapProperties.setProperty("catalogVersion", version);
        }
        ti.setHeapConglomerate(
                createConglomerate(
                        tc,
                        ti.getCatalogRowFactory().makeEmptyRowForLatestVersion(),
                        heapProperties,
                        columnOrder
                )
        );

        // bootstrap indexes on core tables before bootstrapping the tables themselves
        if(ti.getNumberOfIndexes()>0){
            bootStrapSystemIndexes(sd,tc,ddg,ti);
        }

        addSystemTableToDictionary(ti,sd,tc,ddg);
    }

    /**
     * Upgrade an existing system catalog column's definition
     * by setting it to the value it would have in a newly
     * created database. This is only used to for a couple
     * of columns that had incorrectly nullability. Other
     * uses (e.g. changing column type) might require more work.
     *
     * @param columnNumber The column to change
     * @param tc           Transaction controller
     * @throws StandardException Standard Derby error policy
     */
    public void upgradeFixSystemColumnDefinition(CatalogRowFactory rowFactory,
                                                 int columnNumber,
                                                 TransactionController tc) throws StandardException{
        SystemColumn theColumn;
        SystemColumn[] columns=rowFactory.buildColumnList();
        SchemaDescriptor sd=getSystemSchemaDescriptor();

        TableDescriptor td=getTableDescriptor(rowFactory.getCatalogName(),sd,tc);

        theColumn=columns[columnNumber-1];    // from 1 to 0 based
        ColumnDescriptor cd=makeColumnDescriptor(theColumn,columnNumber,td);
        String columnName=cd.getColumnName();
        int[] columnNameColArray=new int[1];
        columnNameColArray[0]=SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDATATYPE;
        updateColumnDescriptor(cd,td.getUUID(),columnName,columnNameColArray,tc);
    }

    /**
     * Get the UUID for the specified system table.  Prior
     * to Plato, system tables did not have canonical UUIDs, so
     * we need to scan systables to get the UUID when we
     * are updating the core tables.
     *
     * @param tableName        Name of the table
     * @param schemaUUID    UUID of schema
     * @param tc            TransactionController to user
     *
     * @return UUID    The UUID of the core table.
     *
     * @exception StandardException        Thrown on failure
     */
    private UUID getUUIDForCoreTable(String tableName,
                                     String schemaUUID,
                                     TransactionController tc)
            throws StandardException {
        ConglomerateController heapCC;
        ExecRow row;
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor tableNameOrderable;
        ScanController scanController;
        TabInfoImpl ti = coreInfo[SYSTABLES_CORE_NUM];
        SYSTABLESRowFactory rf = (SYSTABLESRowFactory) ti.getCatalogRowFactory();

        // We only want the 1st column from the heap
        row = exFactory.getValueRow(1);

        /* Use tableNameOrderable and schemaIdOrderable in both start
         * and stop position for scan.
         */
        tableNameOrderable = new SQLVarchar(tableName);
        schemaIDOrderable = new SQLChar(schemaUUID);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow = exFactory.getIndexableRow(2);
        keyRow.setColumn(1, tableNameOrderable);
        keyRow.setColumn(2, schemaIDOrderable);

        heapCC = tc.openConglomerate(
                ti.getHeapConglomerate(), false, 0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

        ExecRow indexTemplateRow = rf.buildEmptyIndexRow(SYSTABLESRowFactory.SYSTABLES_INDEX1_ID, heapCC.newRowLocationTemplate());

        /* Scan the index and go to the data pages for qualifying rows to
         * build the column descriptor.
         */
        scanController = tc.openScan(
                ti.getIndexConglomerate(SYSTABLESRowFactory.SYSTABLES_INDEX1_ID),  // conglomerate to open
                false, // don't hold open across commit
                0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                (FormatableBitSet) null,         // all fields as objects
                keyRow.getRowArray(),   // start position - first row
                ScanController.GE,      // startSearchOperation
                (ScanQualifier[][]) null, //scanQualifier,
                keyRow.getRowArray(),   // stop position - through last row
                ScanController.GT);     // stopSearchOperation

        /* OK to fetch into the template row,
         * since we won't be doing a next.
         */
        if (scanController.fetchNext(indexTemplateRow.getRowArray())) {
            RowLocation baseRowLocation;


            baseRowLocation = (RowLocation) indexTemplateRow.getColumn(indexTemplateRow.nColumns());

            /* 1st column is TABLEID (UUID - char(36)) */
            row.setColumn(SYSTABLESRowFactory.SYSTABLES_TABLEID, new SQLChar());
            FormatableBitSet bi = new FormatableBitSet(1);
            bi.set(0);
            boolean base_row_exists =
                    heapCC.fetch(
                            baseRowLocation, row, (FormatableBitSet) null);

            if (SanityManager.DEBUG) {
                // it can not be possible for heap row to disappear while
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                SanityManager.ASSERT(base_row_exists, "base row not found");
            }
        }

        scanController.close();
        heapCC.close();

        return uuidFactory.recreateUUID(row.getColumn(1).toString());
    }
    /**
      *    Adds columns to the conglomerate underlying a system table.
      *
      *    @param    templateRow            Ultimate shape of base row of table
      *    @param    newColumnIDs            Array of 1-based column ids
      *    @param    conglomID            heap id
      *    @param    tc                Transaction controller
      *
      *    @exception StandardException Standard Derby error policy
      */
    private    void    widenConglomerate
    (
            ExecRow                    templateRow,
            int[]                    newColumnIDs,
            long                    conglomID,
            TransactionController                    tc
    )
            throws StandardException
    {
            int    columnCount = newColumnIDs.length;

            for ( int ix = 0; ix < columnCount; ix++ )
            {
                    int columnID = newColumnIDs[ix];
                    int storablePosition = columnID - 1;  // from 1 to 0 based

        // system catalog columns always have UCS_BASIC collation.

        tc.addColumnToConglomerate(
            conglomID,
            storablePosition,
            templateRow.getColumn( columnID),
            StringDataValue.COLLATION_TYPE_UCS_BASIC);
            }

    }

    /**
      *    Upgrade an existing catalog by adding columns.
      *
      *    @param    rowFactory     Associated with this catalog.
      *    @param    newColumnIDs   Array of 1-based column ids.
      *    @param    tc             Transaction controller
      *
      *    @exception StandardException Standard Derby error policy
      */
        public void upgrade_addColumns( CatalogRowFactory rowFactory,
                                        int[] newColumnIDs,
                                        TransactionController tc )
                    throws StandardException
    {
        int            columnID;
        SystemColumn        currentColumn;

        SystemColumn[]        columns = rowFactory.buildColumnList();
                ExecRow templateRow = rowFactory.makeEmptyRowForLatestVersion();
        int            columnCount = newColumnIDs.length;
        SchemaDescriptor    sd = getSystemSchemaDescriptor();
        TableDescriptor        td;
        long                conglomID;

        // Special case when adding a column to systables or syscolumns,
        // since we can't go to systables/syscolumns to get the
        // table/column descriptor until after we add and populate the new column.
        if (rowFactory instanceof SYSTABLESRowFactory)
        {
                    td = dataDescriptorGenerator.newTableDescriptor(
                                            "SYSTABLES",
                                            sd,
                                            TableDescriptor.BASE_TABLE_TYPE,
                                            TableDescriptor.ROW_LOCK_GRANULARITY,
                                            -1,null,null,null,
                                            null,null,null,false,false,null);
                    td.setUUID(getUUIDForCoreTable("SYSTABLES", sd.getUUID().toString(), tc));
                    conglomID = coreInfo[SYSTABLES_CORE_NUM].getHeapConglomerate();
        }
        else if (rowFactory instanceof SYSCOLUMNSRowFactory)
        {
                    td = dataDescriptorGenerator.newTableDescriptor(
                                            "SYSCOLUMNS",
                                            sd,
                                            TableDescriptor.BASE_TABLE_TYPE,
                                            TableDescriptor.ROW_LOCK_GRANULARITY,
                                            -1,null,null,null,
                                            null,null,null,false,false,null);
                    td.setUUID(getUUIDForCoreTable("SYSCOLUMNS", sd.getUUID().toString(), tc));
                    conglomID = coreInfo[SYSCOLUMNS_CORE_NUM].getHeapConglomerate();
        }
        else
        {
                    td = getTableDescriptor( rowFactory.getCatalogName(), sd, tc );
                    conglomID = td.getHeapConglomerateId();
        }

        widenConglomerate( templateRow, newColumnIDs, conglomID, tc );


        ColumnDescriptor[] cdArray = new ColumnDescriptor[columnCount];
        for ( int ix = 0; ix < columnCount; ix++ )
        {
                    columnID = newColumnIDs[ix];
                    currentColumn = columns[ columnID - 1 ];    // from 1 to 0 based

                    cdArray[ix] = makeColumnDescriptor(currentColumn, columnID, td);
        }
        addDescriptorArray(cdArray, td, SYSCOLUMNS_CATALOG_NUM, false, tc);
    }

    /*
     *******************************************************************************
    *
    *    See RepBasicDataDictionary for sample code on how to create a system
    *    table.
    *
    *    What follows here is special code for the core catalogs. These are catalogs
    *    which have to exist before any other system tables are created.
    *
    *    Creating a core catalog consists of two steps: 1) creating all the infrastructure
    *    needed to make generic systemTableCreation work, 2) actually populating the
    *    Data Dictionary and core conglomerates with tuples.
    *
     *******************************************************************************
    */


    /**
     * Infrastructure work for indexes on catalogs.
     *
     * @throws StandardException Standard Derby error policy
     */
    protected void bootStrapSystemIndexes(SchemaDescriptor sd,
                                          TransactionController tc,
                                          DataDescriptorGenerator ddg,
                                          TabInfoImpl ti) throws StandardException{
        ConglomerateDescriptor[] cgd=new ConglomerateDescriptor[ti.getNumberOfIndexes()];

        /* Ordering problem with sysconglomerates.  We need to create
         * all of the conglomerates first before adding rows to
         * sysconglomerates.  (All of the conglomerates for sysconglomerates
         * must be there before we can add to them.)
         *
         */
        for(int indexCtr=0;indexCtr<ti.getNumberOfIndexes();indexCtr++){
            cgd[indexCtr]=bootstrapOneIndex(sd,tc,ddg,ti,indexCtr,ti.getHeapConglomerate());
        }

        for(int indexCtr=0;indexCtr<ti.getNumberOfIndexes();indexCtr++){
            addDescriptor(cgd[indexCtr],sd,SYSCONGLOMERATES_CATALOG_NUM,false,tc,false);
        }
    }

    @Override
    public RowLocation[] computeAutoincRowLocations(TransactionController tc,
                                                    TableDescriptor td) throws StandardException{
        int size;
        if(!(td.tableHasAutoincrement()))
            return null;

        size=td.getNumberOfColumns();
        RowLocation[] rla=new RowLocation[size];

        for(int i=0;i<size;i++){
            ColumnDescriptor cd=td.getColumnDescriptor(i+1);
            if(cd.isAutoincrement())
                rla[i]=computeRowLocation(tc,td,cd.getColumnName());
        }
        return rla;
    }

    @Override
    public NumberDataValue getSetAutoincrementValue(RowLocation rl,
                                                    TransactionController tc,
                                                    boolean doUpdate,
                                                    NumberDataValue newValue,
                                                    boolean wait) throws StandardException{
        int columnNum=SYSCOLUMNSRowFactory.SYSCOLUMNS_AUTOINCREMENTVALUE;
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];
        ConglomerateController heapCC=null;
        SYSCOLUMNSRowFactory rf=(SYSCOLUMNSRowFactory)ti.getCatalogRowFactory();
        ExecRow row=rf.makeEmptyRow();

        FormatableBitSet columnToRead=new FormatableBitSet(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMN_COUNT);

        // FormatableBitSet is 0 based.
        columnToRead.set(columnNum-1); // current value.
        columnToRead.set(columnNum);     // start value.
        columnToRead.set(columnNum+1); // increment value.

        try{
            /* if wait is true then we need to do a wait while trying to
               open/fetch from the conglomerate. note we use wait both to
               open as well as fetch from the conglomerate.
            */
            heapCC=tc.openConglomerate(ti.getHeapConglomerate(),false,
                    (TransactionController.OPENMODE_FORUPDATE|((wait)?0:TransactionController.OPENMODE_LOCK_NOWAIT)),
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

            // fetch the current value
            boolean baseRowExists=heapCC.fetch(rl,row,columnToRead,wait);
            // We're not prepared for a non-existing base row.
            assert baseRowExists:"base row not found";

            // while the Row interface is 1 based.
            NumberDataValue currentAI=(NumberDataValue)row.getColumn(columnNum);
            long currentAIValue=currentAI.getLong();

            if(doUpdate){
                // increment the value
                NumberDataValue increment=(NumberDataValue)row.getColumn(columnNum+2);
                currentAI=currentAI.plus(currentAI,increment,currentAI);
                row.setColumn(columnNum,currentAI);

                // store the new value in SYSCOLUMNS
                FormatableBitSet columnToUpdate=new FormatableBitSet(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMN_COUNT);
                columnToUpdate.set(columnNum-1); // current value.
                heapCC.replace(rl,row.getRowArray(),columnToUpdate);
            }

            // but we return the "currentAIValue"-- i.e the value before
            // incrementing it. 
            if(newValue!=null){
                // user has passed in an object; set the current value in there and
                // return it.
                newValue.setValue(currentAIValue);
                return newValue;
            }else{
                // reuse the object read from row.
                currentAI.setValue(currentAIValue);
                return currentAI;
            }
        }finally{
            if(heapCC!=null)
                heapCC.close();
        }
    }

    protected ConglomerateDescriptor bootstrapOneIndex(SchemaDescriptor sd,
                                                     TransactionController tc,
                                                     DataDescriptorGenerator ddg,
                                                     TabInfoImpl ti,
                                                     int indexNumber,
                                                     long heapConglomerateNumber) throws StandardException{
        boolean isUnique;
        ConglomerateController cc;
        ExecRow baseRow;
        ExecIndexRow indexableRow;
        int numColumns;
        long conglomId;
        RowLocation rl;
        CatalogRowFactory rf=ti.getCatalogRowFactory();
        IndexRowGenerator irg;
        ConglomerateDescriptor conglomerateDescriptor;

        initSystemIndexVariables(ti,indexNumber);

        irg=ti.getIndexRowGenerator(indexNumber);

        numColumns=ti.getIndexColumnCount(indexNumber);

        /* Is the index unique */
        isUnique=ti.isIndexUnique(indexNumber);

        // create an index row template
        indexableRow=irg.getIndexRowTemplate();

        baseRow=rf.makeEmptyRowForLatestVersion();

        // Get a RowLocation template
        cc=tc.openConglomerate(heapConglomerateNumber,false,0,TransactionController.MODE_RECORD,TransactionController.ISOLATION_REPEATABLE_READ);

        rl=cc.newRowLocationTemplate();
        cc.close();

        // Get an index row based on the base row
        irg.getIndexRow(baseRow,rl,indexableRow,null);

        // Describe the properties of the index to the store using Properties
        // RESOLVE: The following properties assume a BTREE index.
        Properties indexProperties=new Properties();

        // Tell it the conglomerate id of the base table
        indexProperties.put("baseConglomerateId",Long.toString(heapConglomerateNumber));

        // All indexes are unique because they contain the RowLocation.
        // The number of uniqueness columns must include the RowLocation
        // if the user did not specify a unique index.
        indexProperties.put("nUniqueColumns",Integer.toString(isUnique?numColumns:numColumns+1));

        // By convention, the row location column is the last column
        indexProperties.put("rowLocationColumn",Integer.toString(numColumns));

        // For now, all columns are key fields, including the RowLocation
        indexProperties.put("nKeyFields",Integer.toString(numColumns+1));

        // Cache human readable display name for table and index,
        // which will make it to the relevant descriptor if necessary.
        // These attr names need to match SpliceConstants.TABLE_DISPLAY_NAME_ATTR
        // and SpliceConstants.INDEX_DISPLAY_NAME_ATTR, respectively.
        indexProperties.setProperty("schemaDisplayName", sd.getSchemaName());
        indexProperties.setProperty("tableDisplayName", ti.getTableName());
        indexProperties.setProperty("indexDisplayName", ti.getIndexName(indexNumber));
        /* Create and add the conglomerate (index) */
        conglomId=tc.createConglomerate(false,
                "BTREE", // we're requesting an index conglomerate
                indexableRow.getRowArray(),
                null, //default sort order
                null, //default collation id's for collumns in all system congloms
                indexProperties, // default properties
                TransactionController.IS_DEFAULT); // not temporary

        conglomerateDescriptor=
                ddg.newConglomerateDescriptor(conglomId,
                        rf.getIndexName(indexNumber),
                        true,
                        irg,
                        false,
                        rf.getCanonicalIndexUUID(indexNumber),
                        rf.getCanonicalTableUUID(),
                        sd.getUUID());
        ti.setIndexConglomerate(conglomerateDescriptor);

        return conglomerateDescriptor;
    }

    public void initSystemIndexVariables(TabInfoImpl ti,int indexNumber) throws StandardException{
        int numCols=ti.getIndexColumnCount(indexNumber);
        int[] baseColumnPositions=new int[numCols];

        for(int colCtr=0;colCtr<numCols;colCtr++){
            baseColumnPositions[colCtr]=ti.getBaseColumnPosition(indexNumber,colCtr);
        }

        int baseColumnLength=baseColumnPositions.length;
        boolean[] isAscending=new boolean[baseColumnLength];
        for(int i=0;i<baseColumnLength;i++)
            isAscending[i]=true;


        boolean isUnique=ti.isIndexUnique(indexNumber);
        //Splice is always higher than 10.4
        IndexRowGenerator irg=new IndexRowGenerator("DENSE",isUnique,false,baseColumnPositions,isAscending,baseColumnLength,false,false);

        // For now, assume that all index columns are ordered columns
        ti.setIndexRowGenerator(indexNumber,irg);
    }

    /**
     * Flush sequence caches to disk so that we don't leak unused, pre-allocated numbers.
     */
    @Override
    public void clearSequenceCaches() throws StandardException{
        dataDictionaryCache.sequenceGeneratorCacheClearAll();
    }


    /**
     * Add the required entries to the data dictionary for a System table.
     */

    private void addSystemTableToDictionary(TabInfoImpl ti,
                                            SchemaDescriptor sd,
                                            TransactionController tc,
                                            DataDescriptorGenerator ddg) throws StandardException{
        CatalogRowFactory crf=ti.getCatalogRowFactory();

        String name=ti.getTableName();
        long conglomId=ti.getHeapConglomerate();
        SystemColumn[] columnList=crf.buildColumnList();
        UUID heapUUID=crf.getCanonicalHeapUUID();
        String heapName=crf.getCanonicalHeapName();
        TableDescriptor td;
        UUID toid;
        int columnCount;
        SystemColumn column;

        // add table to the data dictionary

        columnCount=columnList.length;
        td=ddg.newTableDescriptor(name,sd,TableDescriptor.SYSTEM_TABLE_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY,-1,
                null,null,null,null,null,null,false,false,null);
        td.setUUID(crf.getCanonicalTableUUID());
        addDescriptor(td,sd,SYSTABLES_CATALOG_NUM,false,tc,false);
        toid=td.getUUID();

        /* Add the conglomerate for the heap */
        ConglomerateDescriptor cgd=ddg.newConglomerateDescriptor(conglomId,
                heapName,
                false,
                null,
                false,
                heapUUID,
                toid,
                sd.getUUID());

        addDescriptor(cgd,sd,SYSCONGLOMERATES_CATALOG_NUM,false,tc,false);

        /* Create the columns */
        ColumnDescriptor[] cdlArray=new ColumnDescriptor[columnCount];

        for(int columnNumber=0;columnNumber<columnCount;columnNumber++){
            column=columnList[columnNumber];

            assert column!=null:"column "+columnNumber+" for table "+ti.getTableName()+" is null";
            cdlArray[columnNumber]=makeColumnDescriptor(column,columnNumber+1,td);
        }
        addDescriptorArray(cdlArray,td,SYSCOLUMNS_CATALOG_NUM,false,tc);

        // now add the columns to the cdl of the table.
        ColumnDescriptorList cdl=td.getColumnDescriptorList();
        //noinspection ManualArrayToCollectionCopy
        for(int i=0;i<columnCount;i++)
            cdl.add(cdlArray[i]);
    }

    /**
     * Converts a SystemColumn to a ColumnDescriptor.
     *
     * @param columnPosition Position of the column in the table, one based.
     * @param column         a SystemColumn
     * @param td             descriptor for table that column lives in
     * @return a ColumnDes*criptor
     * @throws StandardException Standard Derby error policy
     */
    protected ColumnDescriptor makeColumnDescriptor(SystemColumn column,
                                                    int columnPosition,
                                                    TableDescriptor td) throws StandardException{
        //RESOLVEAUTOINCREMENT
        // No defaults yet for system columns
        return new ColumnDescriptor(column.getName(),columnPosition,columnPosition,column.getType(),null,null,td,null,0,0,columnPosition);
    }


    public String getCatalogVersion(long conglomerateNumber) throws  StandardException{
        Optional<String> version = dataDictionaryCache.catalogVersionCacheFind(conglomerateNumber);
        if (version != null)
            return version.orNull();

        TransactionController tc = getTransactionCompile();
        String v = tc.getCatalogVersion(conglomerateNumber);
        dataDictionaryCache.catalogVersionCacheAdd(conglomerateNumber, v == null ? Optional.absent() : Optional.of(v));
        return v;
    }

    /**
     * Create a conglomerate for a system table
     *
     * @param tc          Transaction context.
     * @param rowTemplate Template for rows for the new table
     * @param properties  Properties for createConglomerate
     * @return Conglomerate id.
     * @throws StandardException Standard Derby error policy.
     */
    protected long createConglomerate(TransactionController tc,
                                      ExecRow rowTemplate,
                                      Properties properties) throws StandardException{
        return createConglomerate(tc,rowTemplate,properties,null);
    }

    protected long createConglomerate(TransactionController tc,
                                      ExecRow rowTemplate,
                                      Properties properties,
                                      ColumnOrdering[] columnOrdering) throws StandardException{
        long conglomId;

        conglomId=tc.createConglomerate(false,
                "heap", // we're requesting a heap conglomerate
                rowTemplate.getRowArray(), // row template
                columnOrdering,
                null, // default collation ids
                properties, // default properties
                TransactionController.IS_DEFAULT); // not temporary
        return conglomId;
    }

    /**
     * Converts a UUID to an DataValueDescriptor.
     *
     * @return the UUID converted to an DataValueDescriptor
     */
    protected static SQLChar getIDValueAsCHAR(UUID uuid){
        String uuidString=uuid.toString();
        return new SQLChar(uuidString);
    }

    /**
     * Initialize catalog information. This method is overridden by children.
     *
     * @throws StandardException Thrown on error
     */
    public void initializeCatalogInfo() throws StandardException{
        initializeCoreInfo();
        initializeNoncoreInfo();
    }

    /**
     * Initialized the core info array.
     */
    private void initializeCoreInfo() throws StandardException{
        TabInfoImpl[] lcoreInfo=coreInfo=new TabInfoImpl[NUM_CORE];
        UUIDFactory luuidFactory=uuidFactory;
        lcoreInfo[SYSTABLES_CORE_NUM]=new TabInfoImpl(new SYSTABLESRowFactory(luuidFactory,exFactory,dvf, this));
        lcoreInfo[SYSCOLUMNS_CORE_NUM]=new TabInfoImpl(new SYSCOLUMNSRowFactory(luuidFactory,exFactory,dvf,this));
        lcoreInfo[SYSCONGLOMERATES_CORE_NUM]=new TabInfoImpl(new SYSCONGLOMERATESRowFactory(luuidFactory,exFactory,dvf, this));
        lcoreInfo[SYSSCHEMAS_CORE_NUM]=new TabInfoImpl(new SYSSCHEMASRowFactory(luuidFactory,exFactory,dvf, this));
    }

    /**
     * Initialized the noncore info array.
     */
    private void initializeNoncoreInfo(){
        noncoreInfo=new TabInfoImpl[NUM_NONCORE];
    }

    /**
     * Get the TransactionController to use, when not
     * passed in as a parameter.  (This hides logic about
     * whether or not we're at boot time in a single
     * place.  NOTE:  There's no LCC at boot time.)
     * NOTE: All <get> methods in the DD should call this method.
     *
     * @return TransactionController    The TC to use.
     * @throws StandardException Thrown on error
     */
    public TransactionController getTransactionCompile() {
        if(bootingTC!=null){
            assert booting:"booting is expected to be true";
            return bootingTC;
        }else{
            assert !booting:"booting is expected to be false";

            LanguageConnectionContext lcc=getLCC();
            return lcc.getTransactionCompile();

        }
    }


    /**
     * Get the TransactionController to use, when not
     * passed in as a parameter.  (This hides logic about
     * whether or not we're at boot time in a single
     * place.  NOTE:  There's no LCC at boot time.)
     * NOTE: All <get> methods in the DD should call this method.
     *
     * @return TransactionController    The TC to use.
     * @throws StandardException Thrown on error
     */
    public TransactionController getTransactionExecute() throws StandardException{
        if(bootingTC!=null){
            assert booting:"booting is expected to be true";
            return bootingTC;
        }else{
            assert !booting:"booting is expected to be false";
            LanguageConnectionContext lcc=getLCC();
            return lcc.getTransactionExecute();
        }
    }

    /**
     * Return a (single or list of) catalog row descriptor(s) from a
     * system table where the access is from the index to the heap.
     *
     * @param indexId               The id of the index (0 to # of indexes on table) to use
     * @param keyRow                The supplied ExecIndexRow for search
     * @param ti                    The TabInfoImpl to use
     * @param parentTupleDescriptor The parentDescriptor, if applicable.
     * @param list                  The list to build, if supplied.  If null, then
     *                              caller expects a single descriptor
     * @param forUpdate             Whether or not to open the index for update.
     * @return The last matching descriptor
     * @throws StandardException Thrown on error
     */
    public final TupleDescriptor getDescriptorViaIndex(int indexId,
                                                       ExecIndexRow keyRow,
                                                       ScanQualifier[][] scanQualifiers,
                                                       TabInfoImpl ti,
                                                       TupleDescriptor parentTupleDescriptor,
                                                       List list,
                                                       boolean forUpdate) throws StandardException{
        // Get the current transaction controller
        TransactionController tc=getTransactionCompile();

        return getDescriptorViaIndexMinion(
                indexId,
                keyRow,
                scanQualifiers,
                ti,
                parentTupleDescriptor,
                list,
                forUpdate,
                TransactionController.ISOLATION_REPEATABLE_READ,
                tc);
    }

    /**
     * Return a (single or list of) catalog row descriptor(s) from a
     * system table where the access is from the index to the heap.
     * <p/>
     * This overload variant takes an explicit tc, in contrast to the normal
     * one which uses the one returned by getTransactionCompile.
     *
     * @param indexId               The id of the index (0 to # of indexes on table) to use
     * @param keyRow                The supplied ExecIndexRow for search
     * @param ti                    The TabInfoImpl to use
     * @param parentTupleDescriptor The parentDescriptor, if applicable.
     * @param list                  The list to build, if supplied.  If null, then
     *                              caller expects a single descriptor
     * @param forUpdate             Whether or not to open the index for update.
     * @param isolationLevel        Use this explicit isolation level. Only
     *                              ISOLATION_REPEATABLE_READ (normal usage) or
     *                              ISOLATION_READ_UNCOMMITTED (corner cases)
     *                              supported for now.
     * @param tc                    Transaction controller
     * @return The last matching descriptor. If isolationLevel is
     * ISOLATION_READ_UNCOMMITTED, the base row may be gone by the
     * time we access it via the index; in such a case a null is
     * returned.
     * @throws StandardException Thrown on error.
     */
    public final TupleDescriptor getDescriptorViaIndex(int indexId,
                                                       ExecIndexRow keyRow,
                                                       ScanQualifier[][] scanQualifiers,
                                                       TabInfoImpl ti,
                                                       TupleDescriptor parentTupleDescriptor,
                                                       List list,
                                                       boolean forUpdate,
                                                       int isolationLevel,
                                                       TransactionController tc) throws StandardException{
        if(tc==null){
            tc=getTransactionCompile();
        }

        return getDescriptorViaIndexMinion(indexId,
                keyRow,
                scanQualifiers,
                ti,
                parentTupleDescriptor,
                list,
                forUpdate,
                isolationLevel,
                tc);
    }


    public final <T extends TupleDescriptor> T getDescriptorViaIndexMinion(int indexId,
                                                                           ExecIndexRow keyRow,
                                                                           ScanQualifier[][] scanQualifiers,
                                                                           TabInfoImpl ti,
                                                                           TupleDescriptor parentTupleDescriptor,
                                                                           List<T> list,
                                                                           boolean forUpdate,
                                                                           int isolationLevel,
                                                                           TransactionController tc) throws StandardException{
        CatalogRowFactory rf=ti.getCatalogRowFactory();
        ConglomerateController heapCC;
        ExecIndexRow indexRow1 = null;
        ExecRow outRow;
        ScanController scanController;
        T td=null;

        if(SanityManager.DEBUG){
            SanityManager.ASSERT
                    (isolationLevel==TransactionController.ISOLATION_REPEATABLE_READ ||
                            isolationLevel==TransactionController.ISOLATION_READ_UNCOMMITTED);
        }
        heapCC=tc.openConglomerate(ti.getHeapConglomerate(),false,0,TransactionController.MODE_RECORD,isolationLevel);

        /* Scan the index and go to the data pages for qualifying rows to
         * build the column descriptor.
         */
        scanController=tc.openScan(
                ti.getIndexConglomerate(indexId),  // conglomerate to open
                false, // don't hold open across commit
                (forUpdate)?TransactionController.OPENMODE_FORUPDATE:0,
                TransactionController.MODE_RECORD,
                isolationLevel,
                null,         // all fields as objects
                keyRow.getRowArray(),   // start position - first row
                ScanController.GE,      // startSearchOperation
                scanQualifiers,         //scanQualifier,
                keyRow.getRowArray(),   // stop position - through last row
                ScanController.GT);     // stopSearchOperation
        List<RowLocation> rowLocations = new ArrayList();

        List<ExecRow> outRows = new ArrayList();
        int i = 0;
        while(true) {
            if (list==null && i==1) // List = null means the caller wants only one record.
                break;
            // create an index row template
            outRow=rf.makeEmptyRow().getClone();
            indexRow1 = getIndexRowFromHeapRow(ti.getIndexRowGenerator(indexId), heapCC.newRowLocationTemplate(), outRow);
            // It is important for read uncommitted scans to use fetchNext()
            // rather than fetch, so that the fetch happens while latch is
            // held, otherwise the next() might position the scan on a row,
            // but the subsequent fetch() may find the row deleted or purged
            // from the table.
            if (!scanController.fetchNext(indexRow1.getRowArray())) {
                break;
            }
            i++;

            outRows.add(outRow);
            rowLocations.add((RowLocation) indexRow1.getColumn(
                    indexRow1.nColumns()).cloneValue(true));
        }
        if (!heapCC.batchFetch(rowLocations,outRows,null)) {
            if(SanityManager.DEBUG){
                // it can not be possible for heap row to disappear while
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                if((isolationLevel==TransactionController.ISOLATION_REPEATABLE_READ)){
                    StringBuilder strbuf=new StringBuilder("Error retrieving base row in table "+ti.getTableName());
                    strbuf.append(": could not locate a row matching index row ").append(indexRow1)
                            .append(" from index ").append(ti.getIndexName(indexId)).append(", conglom number ")
                            .append(ti.getIndexConglomerate(indexId));
                    debugGenerateInfo(strbuf,tc,heapCC,ti,indexId);
                    SanityManager.THROWASSERT(strbuf.toString());
                }
            }
        }

        for (ExecRow retrievedRow: outRows) {
            td=(T)rf.buildDescriptor(retrievedRow,parentTupleDescriptor,this);
            if (td!=null && list !=null)
                list.add(td);
        }
        scanController.close();
        heapCC.close();
        return td;
    }


    private void debugGenerateInfo(StringBuilder strbuf,TransactionController tc,ConglomerateController heapCC,TabInfoImpl ti,int indexId){
        if(SanityManager.DEBUG){
            try{
                strbuf.append("\nadditional information: ");

                // print the lock table
                // will get a NullPointerException if lcc doesn't yet exist e.g. at boot time
                LanguageConnectionContext lcc=(LanguageConnectionContext)
                        ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
                if(lcc!=null){
                    long currentTime=System.currentTimeMillis();
                    //EXCLUDE-START-lockdiag-
                    Enumeration lockTable=lockFactory.makeVirtualLockTable();
                    String lockTableString=Timeout.buildString(lockTable,currentTime);
                    strbuf.append("lock table at time of failure\n\n");
                    strbuf.append(lockTableString);
                    //EXCLUDE-END-lockdiag-
                }

                // consistency checking etc.
                ConglomerateController btreeCC=tc.openConglomerate(ti.getIndexConglomerate(indexId),
                        false,
                        0,TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_REPEATABLE_READ);

                btreeCC.debugConglomerate();
                heapCC.debugConglomerate();
                heapCC.checkConsistency();
                strbuf.append("\nheapCC.checkConsistency() = true");
                ConglomerateController indexCC=tc.openConglomerate(
                        ti.getIndexConglomerate(indexId),
                        false,
                        0,
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_REPEATABLE_READ);
                indexCC.checkConsistency();
                strbuf.append("\nindexCC.checkConsistency() = true");

                System.err.println("ASSERT FAILURE: "+strbuf.toString());
                System.out.println("ASSERT FAILURE: "+strbuf.toString());
                SanityManager.DEBUG_PRINT("ASSERT FAILURE",strbuf.toString());
            }catch(StandardException se){
                strbuf.append("\ngot the following error when doing extra consistency checks:\n").append(se.toString());
            }
        }
    }


    /**
     * Return a (single or list of) catalog row descriptor(s) from a
     * system table where the access a heap scan
     *
     * @param columns               which columns to fetch from the system
     *                              table, or null to fetch all columns
     * @param scanQualifiers        qualifiers
     * @param ti                    The TabInfoImpl to use
     * @param parentTupleDescriptor The parentDescriptor, if applicable.
     * @param list                  The list to build, if supplied.
     *                              If null, then caller expects a single descriptor
     * @return The last matching descriptor
     * @throws StandardException Thrown on error
     */
    protected <T extends TupleDescriptor> T getDescriptorViaHeap(FormatableBitSet columns,
                                                                 ScanQualifier[][] scanQualifiers,
                                                                 TabInfoImpl ti,
                                                                 TupleDescriptor parentTupleDescriptor,
                                                                 List<T> list) throws StandardException{
        CatalogRowFactory rf=ti.getCatalogRowFactory();
        ExecRow outRow;
        ScanController scanController;
        TransactionController tc;
        TupleDescriptor td=null;

        // Get the current transaction controller
        tc=getTransactionCompile();

        outRow=rf.makeEmptyRow();

        /*
        ** Table scan
        */
        scanController=tc.openScan(
                ti.getHeapConglomerate(),    // conglomerate to open
                false,                        // don't hold open across commit
                0,                            // for read
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
                columns,
                null,        // start position - first row
                0,                    // startSearchOperation - none
                scanQualifiers,        // scanQualifier,
                null,        // stop position - through last row
                0);                    // stopSearchOperation - none

        while(scanController.fetchNext(outRow.getRowArray())){
            td=rf.buildDescriptor(outRow,parentTupleDescriptor,this);

            /* If dList is null, then caller only wants a single descriptor - we're done
             * else just add the current descriptor to the list.
             */
            if(list==null){
                break;
            }else{
                //noinspection unchecked
                list.add((T)td);
            }
        }
        scanController.close();
        //noinspection unchecked
        return (T)td;
    }

    /**
     * Get a TabInfoImpl for a non-core table.
     * (We fault in information about non-core tables as needed.)
     *
     * @param catalogNumber The index into noncoreTable[].
     * @throws StandardException Thrown on error
     */
    protected TabInfoImpl getNonCoreTI(int catalogNumber) throws StandardException{
        TabInfoImpl ti=getNonCoreTIByNumber(catalogNumber);

        faultInTabInfo(ti);

        return ti;
    }

    /**
     * returns the tabinfo for a non core system catalog. Input is a
     * catalogNumber (defined in DataDictionary).
     */
    public TabInfoImpl getNonCoreTIByNumber(int catalogNumber) throws StandardException{
        int nonCoreNum=catalogNumber-NUM_CORE;

        // Look up the TabInfoImpl in the array. This does not have to be
        // synchronized, because getting a reference is atomic.

        TabInfoImpl retval=noncoreInfo[nonCoreNum];

        if(retval==null){
            // If we did not find the TabInfoImpl, get the right one and
            // load it into the array. There is a small chance that
            // two threads will do this at the same time. The code will
            // work properly in that case, since storing a reference
            // is atomic (although we could get extra object instantiation
            // if two threads come through here at the same time.
            UUIDFactory luuidFactory=uuidFactory;

            switch(catalogNumber){
                case SYSCONSTRAINTS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSCONSTRAINTSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSKEYS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSKEYSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSPRIMARYKEYS_CATALOG_NUM:
                    retval = new TabInfoImpl(new SYSPRIMARYKEYSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSDEPENDS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSDEPENDSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSVIEWS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSVIEWSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSCHECKS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSCHECKSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSFOREIGNKEYS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSFOREIGNKEYSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSSTATEMENTS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSSTATEMENTSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSFILES_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSFILESRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSALIASES_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSALIASESRowFactory(luuidFactory,exFactory,dvf, this));
                    break;
                case SYSTRIGGERS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSTRIGGERSRowFactory(this, luuidFactory,exFactory,dvf));
                    break;
                case SYSSCHEMAPERMS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSSCHEMAPERMSRowFactory(luuidFactory,exFactory,dvf, this));
                    break;
                case SYSTABLEPERMS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSTABLEPERMSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSCOLPERMS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSCOLPERMSRowFactory(luuidFactory,exFactory,dvf, this));
                    break;
                case SYSROUTINEPERMS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSROUTINEPERMSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSROLES_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSROLESRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSSEQUENCES_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSSEQUENCESRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSPERMS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSPERMSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSUSERS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSUSERSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSBACKUP_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSBACKUPRowFactory(luuidFactory,exFactory,dvf, this));
                    break;
                case SYSBACKUPITEMS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSBACKUPITEMSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSCOLUMNSTATS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSCOLUMNSTATISTICSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSPHYSICALSTATS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSPHYSICALSTATISTICSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSTABLESTATS_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSTABLESTATISTICSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSSOURCECODE_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSSOURCECODERowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSDUMMY1_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSDUMMY1RowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSSNAPSHOT_NUM:
                    retval=new TabInfoImpl(new SYSSNAPSHOTSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSTOKENS_NUM:
                    retval=new TabInfoImpl(new SYSTOKENSRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSREPLICATION_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSREPLICATIONRowFactory(luuidFactory,exFactory,dvf,this));
                    break;
                case SYSMONGETCONNECTION_CATALOG_NUM:
                    retval=new TabInfoImpl(new SYSMONGETCONNECTIONRowFactory(luuidFactory,exFactory,dvf, this));
                    break;
                default:
                    retval=null;
                    break;
            }
            if (retval != null) {
                initSystemIndexVariables(retval);
                noncoreInfo[nonCoreNum] = retval;
            }
        }

        return retval;
    }

    protected void initSystemIndexVariables(TabInfoImpl ti) throws StandardException{
        int numIndexes=ti.getNumberOfIndexes();

        if(numIndexes>0){

            for(int indexCtr=0;indexCtr<numIndexes;indexCtr++){
                initSystemIndexVariables(ti,indexCtr);
            }
        }
    }

    /**
     * Populate SYSDUMMY1 table with a single row.
     *
     * @throws StandardException Standard Derby error policy
     */
    protected void populateSYSDUMMY1(TransactionController tc) throws StandardException{
        TabInfoImpl    ti = getNonCoreTI(SYSDUMMY1_CATALOG_NUM);
        ExecRow row = ti.getCatalogRowFactory().makeRow(null, null);

        // ignore return value because sysdummy1 does not have indexes
        ti.insertRow(row, tc);
    }

    // Expected to be called only during boot time, so no synchronization.
    protected void clearNoncoreTable(int nonCoreNum){
        noncoreInfo[nonCoreNum]=null;
    }

    /**
     * Finishes building a TabInfoImpl if it hasn't already been faulted in.
     * NOP if TabInfoImpl has already been faulted in.
     *
     * @param ti TabInfoImpl to fault in.
     * @throws StandardException Thrown on error
     */
    public void faultInTabInfo(TabInfoImpl ti) throws StandardException{
        int numIndexes;

        /* Most of the time, the noncoreInfo will be complete.
         * It's okay to do an unsynchronized check and return
         * if it is complete, since it never becomes "un-complete".
         * If we change the code, for some reason, to allow
         * it to become "un-complete" after being complete,
         * then we will have to do a synchronized check here
         * as well.
         */
        if(ti.isComplete()){
            return;
        }

        /* The completing of the noncoreInfo entry must be synchronized.
         * NOTE: We are assuming that we will not access a different
         * noncoreInfo in the course of completing of this noncoreInfo,
         * otherwise a deadlock could occur.
         */
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized(ti){
            /* Now that we can run, the 1st thing that we must do
             * is to verify that we still need to complete the
             * object.  (We may have been blocked on another user
             * doing the same.)
             */
            if(ti.isComplete()){
                return;
            }

            TableDescriptor td=getTableDescriptor(ti.getTableName(),getSystemSchemaDescriptor(),null);

            // It's possible that the system table is not there right
            // now. This can happen, for example, if we're in the
            // process of upgrading a source or target to Xena, in 
            // which case SYSSYNCINSTANTS is dropped and re-created.
            // Just return in this case, so we don't get a null pointer
            // exception.
            if(td==null){
                // look up from SYSIBM schema
                td=getTableDescriptor(ti.getTableName(),getSysIBMSchemaDescriptor(),null);
                if ( td == null) {
                    // look up from SYSIBMADM schema
                    td = getTableDescriptor(ti.getTableName(), sysIBMADMSchemaDesc, null);
                    if (td == null) {
                        return;
                    }
                }
            }

            ConglomerateDescriptor cd=null;
            ConglomerateDescriptor[] cds=td.getConglomerateDescriptors();

            /* Init the heap conglomerate here */
            for(ConglomerateDescriptor cd1 : cds){
                cd=cd1;

                if(!cd.isIndex()){
                    ti.setHeapConglomerate(cd.getConglomerateNumber());
                    break;
                }
            }

            assert cd!=null:"No heap conglomerate found for "+ti.getTableName();

            /* Initialize the index conglomerates */
            numIndexes=ti.getCatalogRowFactory().getNumIndexes();
            if(numIndexes==0){
                return;
            }

            /* For each index, we get its id from the CDL */
            ConglomerateDescriptor icd;
            int indexCount=0;

            for(ConglomerateDescriptor cd1 : cds){
                icd=cd1;

                if(icd.isIndex()){
                    ti.setIndexConglomerate(icd);
                    indexCount++;
                }
            }

            assert indexCount==ti.getCatalogRowFactory().getNumIndexes():
                    "Number of indexes found does not match the number expected";
        }
    }


    /**
     * Get an index row based on a row from the heap.
     *
     * @param irg     IndexRowGenerator to use
     * @param rl      RowLocation for heap
     * @param heapRow Row from the heap
     * @return ExecIndexRow    Index row.
     * @throws StandardException Thrown on error
     */
    public static ExecIndexRow getIndexRowFromHeapRow(IndexRowGenerator irg,
                                                      RowLocation rl,
                                                      ExecRow heapRow) throws StandardException{
        ExecIndexRow indexRow;

        indexRow=irg.getIndexRowTemplate();
        // Get an index row based on the base row
        irg.getIndexRow(heapRow,rl,indexRow,null);

        return indexRow;
    }

    /**
     * Get the heap conglomerate number for SYS.SYSCOLUMNS.
     * (Useful for adding new index to the table.)
     *
     * @return The heap conglomerate number for SYS.SYSCOLUMNS.
     */
    //used in Splice
    @SuppressWarnings("unused")
    public long getSYSCOLUMNSHeapConglomerateNumber(){
        return coreInfo[SYSCOLUMNS_CORE_NUM].getHeapConglomerate();
    }

    private long getBootParameter(Properties startParams,String key,boolean required) throws StandardException{

        String value=startParams.getProperty(key);
        if(value==null){
            if(!required){
                return -1;
            }
            throw StandardException.newException(SQLState.PROPERTY_MISSING,key);
        }

        try{
            return Long.parseLong(value);
        }catch(NumberFormatException nfe){
            throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE,key,value);
        }
    }

    /**
     * Returns a unique system generated name of the form SQLyymmddhhmmssxxn
     * yy - year, mm - month, dd - day of month, hh - hour, mm - minute, ss - second,
     * xx - the first 2 digits of millisec because we don't have enough space to keep the exact millisec value,
     * n - number between 0-9
     * <p/>
     * The number at the end is to handle more than one system generated name request came at the same time.
     * In that case, the timestamp will remain the same, we will just increment n at the end of the name.
     * <p/>
     * Following is how we get around the problem of more than 10 system generated name requestes at the same time:
     * When the database boots up, we start a counter with value -1 for the last digit in the generated name.
     * We also keep the time in millisec to keep track of when the last system name was generated. At the
     * boot time, it will be default to 0L. In addition, we have a calendar object for the time in millisec
     * That calendar object is used to fetch yy, mm, dd, etc for the string SQLyymmddhhmmssxxn
     * <p/>
     * When the first request for the system generated name comes, time of last system generated name will be less than
     * the current time. We initialize the counter to 0, set the time of last system generated name to the
     * current time truncated off to lower 10ms time. The first name request is the only time we know for sure the
     * time of last system generated name will be less than the current time. After this first request, the next request
     * could be at any time. We go through the following algorithm for every generated name request.
     * <p/>
     * First check if the current time(truncated off to lower 10ms) is greater than the timestamp for last system generated name
     * <p/>
     * If yes, then we change the timestamp for system generated name to the current timestamp and reset the counter to 0
     * and generate the name using the current timestamp and 0 as the number at the end of the generated name.
     * <p/>
     * If no, then it means this request for generated name has come at the same time as last one.
     * Or it may come at a time less than the last generated name request. This could be because of seasonal time change
     * or somebody manually changing the time on the computer. In any case,
     * if the counter is less than 10(meaning this is not yet our 11th request for generated name at a given time),
     * we use that in the generated name. But if the counter has reached 10(which means, this is the 11th name request
     * at the same time), then we increment the system generated name timestamp by 10ms and reset the counter to 0
     * (notice, at this point, the timestamp for system generated names is not in sync with the real current time, but we
     * need to have this mechanism to get around the problem of more than 10 generated name requests at a same physical time).
     *
     * @return system generated unique name
     */
    @Override
    public String getSystemSQLName(){
        StringBuilder generatedSystemSQLName=new StringBuilder("SQL");
        synchronized(this){
            //get the current timestamp
            long timeNow=(System.currentTimeMillis()/10L)*10L;

            //if the current timestamp is greater than last constraint name generation time, then we reset the counter and
            //record the new timestamp
            if(timeNow>timeForLastSystemSQLName){
                systemSQLNameNumber=0;
                calendarForLastSystemSQLName.setTimeInMillis(timeNow);
                timeForLastSystemSQLName=timeNow;
            }else{
                //the request has come at the same time as the last generated name request
                //or it has come at a time less than the time the last generated name request. This can happen
                //because of seasonal time change or manual update of computer time.

                //get the number that was last used for the last digit of generated name and increment it by 1.
                systemSQLNameNumber++;
                if(systemSQLNameNumber==10){ //we have already generated 10 names at the last system generated timestamp value
                    //so reset the counter
                    systemSQLNameNumber=0;
                    timeForLastSystemSQLName=timeForLastSystemSQLName+10L;
                    //increment the timestamp for system generated names by 10ms
                    calendarForLastSystemSQLName.setTimeInMillis(timeForLastSystemSQLName);
                }
            }

            generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.YEAR)));
            //have to add 1 to the month value returned because the method give 0-January, 1-February and so on and so forth
            generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.MONTH)+1));
            generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.DAY_OF_MONTH)));
            generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.HOUR_OF_DAY)));
            generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.MINUTE)));
            generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.SECOND)));
            //because we don't have enough space to store the entire millisec value, just store the higher 2 digits.
            generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.MILLISECOND)/10));
            generatedSystemSQLName.append(systemSQLNameNumber);
        }
        return generatedSystemSQLName.toString();
    }

    private static String twoDigits(int val){
        String retval;

        if(val<10){
            retval="0"+val;
        }else{
            int retvalLength=Integer.toString(val).length();
            retval=Integer.toString(val).substring(retvalLength-2);
        }

        return retval;
    }

    /**
     * sets a new value in SYSCOLUMNS for a particular
     * autoincrement column.
     *
     * @param tc              Transaction Controller to use.
     * @param columnName      Name of the column.
     * @param aiValue         Value to write to SYSCOLUMNS.
     * @param incrementNeeded whether to increment the value passed in by the
     *                        user (aiValue) or not before writing it to SYSCOLUMNS.
     */
    @Override
    public void setAutoincrementValue(TransactionController tc,
                                      UUID tableUUID,
                                      String columnName,
                                      long aiValue,
                                      boolean incrementNeeded) throws StandardException{
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];
        ExecIndexRow keyRow;

        keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,getIDValueAsCHAR(tableUUID));
        keyRow.setColumn(2,new SQLChar(columnName));

        SYSCOLUMNSRowFactory rf=(SYSCOLUMNSRowFactory)ti.getCatalogRowFactory();
        ExecRow row=rf.makeEmptyRow();

        boolean[] bArray=new boolean[2];
        for(int index=0;index<2;index++){
            bArray[index]=false;
        }

        int[] colsToUpdate=new int[1];

        colsToUpdate[0]=SYSCOLUMNSRowFactory.SYSCOLUMNS_AUTOINCREMENTVALUE;

        if(incrementNeeded){
            ExecRow readRow=ti.getRow(tc,keyRow,SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID);
            NumberDataValue increment=(NumberDataValue)readRow.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_AUTOINCREMENTINC);
            aiValue+=increment.getLong();
        }
        row.setColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_AUTOINCREMENTVALUE,new SQLLongint(aiValue));

        ti.updateRow(keyRow,row,SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,bArray,colsToUpdate,tc);
    }

    /**
     * sets whether to collect stats for a column
     *
     * @param tc              Transaction Controller to use.
     * @param columnName      Name of the column.
     * @param collectStats         Value to write to SYSCOLUMNS.
     */
    @Override
    public void setCollectStats(TransactionController tc,
                                      UUID tableUUID,
                                      String columnName,
                                      boolean collectStats) throws StandardException{
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];
        ExecIndexRow keyRow;

        keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,getIDValueAsCHAR(tableUUID));
        keyRow.setColumn(2,new SQLChar(columnName));

        SYSCOLUMNSRowFactory rf=(SYSCOLUMNSRowFactory)ti.getCatalogRowFactory();
        ExecRow row=rf.makeEmptyRow();

        boolean[] bArray=new boolean[2];
        for(int index=0;index<2;index++){
            bArray[index]=false;
        }
        int[] colsToUpdate=new int[1];
        colsToUpdate[0]=SYSCOLUMNSRowFactory.SYSCOLUMNS_COLLECTSTATS;
        row.setColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLLECTSTATS,new SQLBoolean(collectStats));
        ti.updateRow(keyRow,row,SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,bArray,colsToUpdate,tc);
    }

    /**
     * sets whether extrapolation is allowed for a column
     *
     * @param tc              Transaction Controller to use.
     * @param tableUUID
     * @param columnName      Name of the column.
     * @param useExtrapolation   Value to write to SYSCOLUMNS.
     */
    @Override
    public void setUseExtrapolation(TransactionController tc,
                                UUID tableUUID,
                                String columnName,
                                byte useExtrapolation) throws StandardException{
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];
        ExecIndexRow keyRow;

        keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,getIDValueAsCHAR(tableUUID));
        keyRow.setColumn(2,new SQLChar(columnName));

        SYSCOLUMNSRowFactory rf=(SYSCOLUMNSRowFactory)ti.getCatalogRowFactory();
        ExecRow row=rf.makeEmptyRow();

        boolean[] bArray=new boolean[2];
        for(int index=0;index<2;index++){
            bArray[index]=false;
        }
        int[] colsToUpdate=new int[1];
        colsToUpdate[0]=SYSCOLUMNSRowFactory.SYSCOLUMNS_USEEXTRAPOLATION;
        row.setColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_USEEXTRAPOLATION,new SQLTinyint(useExtrapolation));
        ti.updateRow(keyRow,row,SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,bArray,colsToUpdate,tc);
    }

    /**
     * Computes the RowLocation in SYSCOLUMNS for a particular
     * autoincrement column.
     *
     * @param tc         Transaction Controller to use.
     * @param td         Table Descriptor.
     * @param columnName Name of column which has autoincrement column.
     * @throws StandardException thrown on failure.
     */
    private RowLocation computeRowLocation(TransactionController tc,
                                           TableDescriptor td,
                                           String columnName) throws StandardException{
        TabInfoImpl ti=coreInfo[SYSCOLUMNS_CORE_NUM];
        ExecIndexRow keyRow;
        UUID tableUUID=td.getUUID();

        keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,getIDValueAsCHAR(tableUUID));
        keyRow.setColumn(2,new SQLChar(columnName));
        return ti.getRowLocation(tc,keyRow,SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID);
    }

    /**
     * Computes the RowLocation in SYSSEQUENCES for a particular sequence. Also
     * constructs the sequence descriptor.
     *
     * @param tc                 Transaction Controller to use.
     * @param sequenceIDstring   UUID of the sequence as a string
     * @param rowLocation        OUTPUT param for returing the row location
     * @param sequenceDescriptor OUTPUT param for return the sequence descriptor
     * @throws StandardException thrown on failure.
     */
    protected void computeSequenceRowLocation(TransactionController tc,
                                              String sequenceIDstring,
                                              RowLocation[] rowLocation,
                                              SequenceDescriptor[] sequenceDescriptor) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(SYSSEQUENCES_CATALOG_NUM);
        ExecIndexRow keyRow;

        keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,new SQLChar(sequenceIDstring));

        rowLocation[0]=ti.getRowLocation(tc,keyRow,SYSSEQUENCESRowFactory.SYSSEQUENCES_INDEX1_ID);

        sequenceDescriptor[0]=(SequenceDescriptor)getDescriptorViaIndex(SYSSEQUENCESRowFactory.SYSSEQUENCES_INDEX1_ID,
                keyRow,
                null,
                ti,
                null,
                null,
                false,
                TransactionController.ISOLATION_REPEATABLE_READ,
                tc);
    }

    /**
     * Set the current value of an ANSI/ISO sequence. This method does not perform
     * any sanity checking but assumes that the caller knows what they are doing. If the
     * old value on disk is not what we expect it to be, then we are in a race with another
     * session. They won and we don't update the value on disk. However, if the old value
     * is null, that is a signal to us that we should update the value on disk anyway.
     *
     * @param tc          Transaction Controller to use.
     * @param rowLocation Row in SYSSEQUENCES to update.
     * @param wait        True if we should wait for locks
     * @param oldValue    What we expect to find in the CURRENTVALUE column.
     * @param newValue    What to stuff into the CURRENTVALUE column.
     * @return Returns true if the value was successfully updated, false if we lost a race with another session.
     * @throws StandardException thrown on failure.
     */
    boolean updateCurrentSequenceValue(TransactionController tc,
                                       RowLocation rowLocation,
                                       boolean wait,
                                       Long oldValue,
                                       Long newValue) throws StandardException{
        int columnNum=SYSSEQUENCESRowFactory.SYSSEQUENCES_CURRENT_VALUE;
        FormatableBitSet columnToUpdate=new FormatableBitSet(SYSSEQUENCESRowFactory.SYSSEQUENCES_COLUMN_COUNT);
        TabInfoImpl ti=getNonCoreTI(SYSSEQUENCES_CATALOG_NUM);
        ConglomerateController heapCC=null;
        SYSSEQUENCESRowFactory rf=(SYSSEQUENCESRowFactory)ti.getCatalogRowFactory();
        ExecRow row=rf.makeEmptyRow();

        // FormatableBitSet is 0 based.
        columnToUpdate.set(columnNum-1); // current value.

        try{
            /* if wait is true then we need to do a wait while trying to
               open/fetch from the conglomerate. note we use wait both to
               open as well as fetch from the conglomerate.
            */
            heapCC=tc.openConglomerate(ti.getHeapConglomerate(),
                    false,
                    (TransactionController.OPENMODE_FORUPDATE|((wait)?0:TransactionController.OPENMODE_LOCK_NOWAIT)),
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

            boolean baseRowExists=heapCC.fetch(rowLocation,row,columnToUpdate,wait);
            // We're not prepared for a non-existing base row.
            assert baseRowExists:"base row not found";

            NumberDataValue oldValueOnDisk=(NumberDataValue)row.getColumn(columnNum);

            SQLLongint expectedOldValue;
            if(oldValue==null){
                expectedOldValue=new SQLLongint();
            }else{
                expectedOldValue=new SQLLongint(oldValue.longValue());
            }

            // only update value if what's on disk is what we expected
            if((oldValue==null) || (expectedOldValue.compare(oldValueOnDisk)==0)){
                SQLLongint newValueOnDisk;
                if(newValue==null){
                    newValueOnDisk=new SQLLongint();
                }else{
                    newValueOnDisk=new SQLLongint(newValue.longValue());
                }

                row.setColumn(columnNum,newValueOnDisk);
                heapCC.replace(rowLocation,row.getRowArray(),columnToUpdate);

                return true;
            }else{
                return false;
            }
        }finally{
            if(heapCC!=null){
                heapCC.close();
            }
        }
    }

    @Override
    public abstract void getCurrentValueAndAdvance(String sequenceUUIDstring,NumberDataValue returnValue, boolean useBatch) throws StandardException;

    @Override
    public abstract Long peekAtSequence(String schemaName,String sequenceName) throws StandardException;

    @Override
    public RowLocation getRowLocationTemplate(LanguageConnectionContext lcc,TableDescriptor td) throws StandardException{
        RowLocation rl;
        ConglomerateController heapCC;

        TransactionController tc=lcc.getTransactionCompile();

        long tableId=td.getHeapConglomerateId();
        heapCC=tc.openConglomerate(tableId,false,0,TransactionController.MODE_RECORD,TransactionController.ISOLATION_READ_COMMITTED);
        try{
            rl=heapCC.newRowLocationTemplate();
        }finally{
            heapCC.close();
        }

        return rl;
    }

    private static LanguageConnectionContext getLCC(){
        return (LanguageConnectionContext)ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
    }

    private SchemaDescriptor newSystemSchemaDesc(String name,String uuid){
        return new SchemaDescriptor(this,name,authorizationDatabaseOwner,uuidFactory.recreateUUID(uuid),true);
    }

    private SchemaDescriptor newDeclaredGlobalTemporaryTablesSchemaDesc(String name){
        return new SchemaDescriptor(this,name,authorizationDatabaseOwner,uuidFactory.createUUID(),false);
    }

    /**
     * Check to see if a database has been upgraded to the required
     * level in order to use a language feature.
     *
     * @param requiredMajorVersion Data Dictionary major version
     * @param feature              Non-null to throw an error, null to return the state of the version match.
     * @return True if the database has been upgraded to the required level, false otherwise.
     */
    @Override
    public boolean checkVersion(int requiredMajorVersion,String feature) throws StandardException{
        if(requiredMajorVersion==DataDictionary.DD_VERSION_CURRENT){
            requiredMajorVersion=softwareVersion.majorVersionNumber;
        }

        return dictionaryVersion.checkVersion(requiredMajorVersion,feature);
    }

    @Override
    public boolean isReadOnlyUpgrade(){
        return readOnlyUpgrade;
    }

    /**
     * Mark this database as a read only database whose stored prepared
     * statements are invalid because some kind of upgrade is needed.
     */
    void setReadOnlyUpgrade(){
        readOnlyUpgrade=true;
    }

    /**
     * * Create system built-in metadata stored prepared statements.
     */
    void createSystemSps(TransactionController tc) throws StandardException{
        // DatabaseMetadata stored plans
        createSPSSet(tc,false,getSystemSchemaDescriptor().getUUID());

        // network server stored plans
        createSPSSet(tc,true,getSysIBMSchemaDescriptor().getUUID());
    }

    /**
     * Create a set of stored prepared statements from a properties file.
     * Key is the statement name, value is the SQL statement.
     */
    protected void createSPSSet(TransactionController tc,boolean net,UUID schemaID) throws StandardException{
        Properties p=getQueryDescriptions(net);
        Enumeration e=p.keys();
        //statement will get compiled on first execution
        //Note: Don't change this to FALSE LCC is not available for compiling

        while(e.hasMoreElements()){
            String spsName=(String)e.nextElement();
            String spsText=p.getProperty(spsName);
            SPSDescriptor spsd=new SPSDescriptor(this,spsName,
                    getUUIDFactory().createUUID(),
                    schemaID,
                    schemaID,
                    SPSDescriptor.SPS_TYPE_REGULAR,
                    false,        // it is valid, unless nocompile
                    spsText, //sps text
                    false);
//            if (LOG.isTraceEnabled()) LOG.trace(String.format("Creating metadata SPS: %s.%s", spsd.getSchemaDescriptor().getSchemaName(), spsd.getDescriptorName()));
            addSPSDescriptor(spsd,tc);
        }
    }


    /**
     * Generic create procedure routine.
     * <p/>
     * Takes the input procedure and inserts it into the appropriate
     * catalog.
     * <p/>
     * Assumes all arguments are "IN" type.
     *
     * @param routine_name         name of the routine in java and the SQL
     *                             procedure name.
     * @param arg_names            String array of procedure argument names in order.
     * @param arg_types            Internal SQL types of the arguments
     * @param routine_sql_control  One of the RoutineAliasInfo constants:
     *                             MODIFIES_SQL_DATA
     *                             READS_SQL_DATA
     *                             CONTAINS_SQL
     *                             NO_SQL
     * @param isDeterministic      True if the procedure/function is DETERMINISTIC
     * @param return_type          null for procedure.  For functions the return type
     *                             of the function.
     * @param newlyCreatedRoutines evolving set of routines, some of which may need permissions later on
     * @param tc                   an instance of the TransactionController
     * @param procClass            the fully qualified name of the class that contains
     *                             java definitions for the stored procedures
     * @return UUID        UUID of system routine that got created.
     * @throws StandardException Standard exception policy.
     */
    private UUID createSystemProcedureOrFunction(
            String routine_name,
            UUID schema_uuid,
            String[] arg_names,
            TypeDescriptor[] arg_types,
            int num_out_param,
            int num_result_sets,
            short routine_sql_control,
            boolean isDeterministic,
            TypeDescriptor return_type,
            Set<String> newlyCreatedRoutines,
            TransactionController tc,
            String procClass)
            throws StandardException{
        int num_args=0;
        if(arg_names!=null)
            num_args=arg_names.length;

        if(num_args!=0){
            assert arg_types!=null;
            assert arg_names.length==arg_types.length;
        }


        // all args are only "in" arguments
        int[] arg_modes=null;
        if(num_args!=0){
            arg_modes=new int[num_args];
            int num_in_param=num_args-num_out_param;
            for(int i=0;i<num_in_param;i++)
                arg_modes[i]=JDBC30Translation.PARAMETER_MODE_IN;
            for(int i=0;i<num_out_param;i++)
                arg_modes[num_in_param+i]=JDBC30Translation.PARAMETER_MODE_OUT;
        }

        RoutineAliasInfo routine_alias_info=
                new RoutineAliasInfo(
                        routine_name,                       // name of routine
                        "JAVA",                    // language
                        num_args,                           // number of params
                        arg_names,                          // names of params
                        arg_types,                          // types of params
                        arg_modes,                          // all "IN" params
                        num_result_sets,                    // number of result sets
                        RoutineAliasInfo.PS_JAVA,           // link to java routine
                        routine_sql_control,                // one of:
                        //  MODIFIES_SQL_DATA
                        //  READS_SQL_DATA
                        //  CONTAINS_SQL
                        //  NO_SQL
                        isDeterministic,             // whether the procedure/function is DETERMINISTIC
                        false,                              // not definer's rights
                        true,                               // true - calledOnNullInput
                        return_type,
                        null);

        UUID routine_uuid=getUUIDFactory().createUUID();
        AliasDescriptor ads=new AliasDescriptor(
                this,
                routine_uuid,
                routine_name,
                schema_uuid,
                procClass,
                (return_type==null)?
                        AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
                        AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR,
                (return_type==null)?
                        AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR:
                        AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR,
                false,
                routine_alias_info,null);

        addDescriptor(ads,null,DataDictionary.SYSALIASES_CATALOG_NUM,false,tc,false);

        newlyCreatedRoutines.add(routine_name);

        return routine_uuid;
    }

    /**
     * Generic create procedure routine.
     * Takes the input procedure and inserts it into the appropriate
     * catalog.
     * <p/>
     * Assumes all arguments are "IN" type.
     *
     * @param routine_name         name of the routine in java and the SQL
     *                             procedure name.
     * @param arg_names            String array of procedure argument names in order.
     * @param arg_types            Internal SQL types of the arguments
     * @param routine_sql_control  One of the RoutineAliasInfo constants:
     *                             MODIFIES_SQL_DATA
     *                             READS_SQL_DATA
     *                             CONTAINS_SQL
     *                             NO_SQL
     * @param isDeterministic      True if the procedure/function is DETERMINISTIC
     * @param return_type          null for procedure.  For functions the return type
     *                             of the function.
     * @param newlyCreatedRoutines evolving set of routines, some of which may need permissions later on
     * @param tc                   an instance of the TransactionController
     * @return UUID         UUID of system routine that got created.
     * @throws StandardException Standard exception policy.
     */
    private UUID createSystemProcedureOrFunction(
            String routine_name,
            UUID schema_uuid,
            String[] arg_names,
            TypeDescriptor[] arg_types,
            int num_out_param,
            int num_result_sets,
            short routine_sql_control,
            boolean isDeterministic,
            TypeDescriptor return_type,
            Set<String> newlyCreatedRoutines,
            TransactionController tc)
            throws StandardException{
        return createSystemProcedureOrFunction(routine_name,
                schema_uuid,arg_names,arg_types,
                num_out_param,num_result_sets,routine_sql_control,isDeterministic,
                return_type,newlyCreatedRoutines,tc,"com.splicemachine.db.catalog.SystemProcedures");
    }

    /**
     * Grant PUBLIC access to specific system routines. Currently, this is
     * done for some routines in SYSCS_UTIL schema. We grant access to routines
     * which we have just added. Doing it this way lets us declare these
     * routines in one place and re-use this logic during database creation and
     * during upgrade.
     *
     * @param tc              TransactionController to use
     * @param authorizationID authorization ID of the permission grantor
     * @throws StandardException Standard exception policy.
     */
    public void grantPublicAccessToSystemRoutines(HashSet newlyCreatedRoutines,
                                                  TransactionController tc,
                                                  String authorizationID) throws StandardException{

        // Get schema ID for SYSCS_UTIL schema
        String schemaID=getSystemUtilSchemaDescriptor().getUUID().toString();

        for(String routineName : sysUtilProceduresWithPublicAccess){

            if(!newlyCreatedRoutines.contains(routineName)){
                continue;
            }

            grantPublicAccessToSystemRoutine(schemaID,routineName,AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR,tc,authorizationID);
        }
    }


    /**
     * Grant PUBLIC access to a system routine. This method should be used only
     * for granting access to a system routine (other than routines in SYSFUN
     * schema). It expects the routine to be present in SYSALIASES catalog.
     *
     * @param schemaID        Schema ID
     * @param routineName     Routine Name
     * @param nameSpace       Indicates whether the routine is a function/procedure.
     * @param tc              TransactionController to use
     * @param authorizationID authorization ID of the permission grantor
     * @throws StandardException Standard exception policy.
     */
    private void grantPublicAccessToSystemRoutine(String schemaID,
                                                  String routineName,
                                                  char nameSpace,
                                                  TransactionController tc,
                                                  String authorizationID) throws StandardException{
        // For system routines, a valid alias descriptor will be returned.
        AliasDescriptor ad=getAliasDescriptor(schemaID,routineName,
                nameSpace);
        //
        // When upgrading from 10.1, it can happen that we haven't yet created
        // all public procedures. We forgive that possibility here and just return.
        //
        if(ad==null){
            return;
        }

        UUID routineUUID=ad.getUUID();
        createRoutinePermPublicDescriptor(routineUUID,tc,authorizationID);
    }


    /**
     * Create RoutinePermDescriptor to grant access to PUBLIC for
     * this system routine using the grantor specified in authorizationID.
     *
     * @param routineUUID     uuid of the routine
     * @param tc              TransactionController to use
     * @param authorizationID authorization ID of the permission grantor
     * @throws StandardException Standard exception policy.
     */
    void createRoutinePermPublicDescriptor(UUID routineUUID,
                                           TransactionController tc,
                                           String authorizationID) throws StandardException{
        RoutinePermsDescriptor routinePermDesc=
                new RoutinePermsDescriptor(this,"PUBLIC",authorizationID,routineUUID);
        // add if this permission has not been granted before
        if (getUncachedRoutinePermsDescriptor(routinePermDesc) == null)
            addDescriptor(routinePermDesc,null,DataDictionary.SYSROUTINEPERMS_CATALOG_NUM,false,tc,false);
    }

    /**
     * Create system procedures added in version 10.1.
     * <p/>
     * Create 10.1 system procedures, called by either code creating new
     * database, or code doing hard upgrade from previous version.
     * <p/>
     *
     * @param tc                   booting transaction
     * @param newlyCreatedRoutines set of routines we are creating (used to add permissions later on)
     * @param sysUtilUUID          uuid of the SYSUTIL schema.
     * @throws StandardException Standard exception policy.
     */
    void create_10_1_system_procedures(TransactionController tc,
                                       Set<String> newlyCreatedRoutines,
                                       UUID sysUtilUUID) throws StandardException{
        // void SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(
        //     IN SCHEMANAME        VARCHAR(128), 
        //     IN TABLENAME         VARCHAR(128),
        //     IN PURGE_ROWS        SMALLINT,
        //     IN DEFRAGMENT_ROWS   SMALLINT,
        //     IN TRUNCATE_END      SMALLINT
        //     )
        // procedure argument names
        String[] arg_names={
                "SCHEMANAME",
                "TABLENAME",
                "PURGE_ROWS",
                "DEFRAGMENT_ROWS",
                "TRUNCATE_END"};

        // procedure argument types
        TypeDescriptor[] arg_types={
                CATALOG_TYPE_SYSTEM_IDENTIFIER,
                CATALOG_TYPE_SYSTEM_IDENTIFIER,
                TypeDescriptor.SMALLINT,
                TypeDescriptor.SMALLINT,
                TypeDescriptor.SMALLINT
        };

        createSystemProcedureOrFunction(
                "SYSCS_INPLACE_COMPRESS_TABLE",
                sysUtilUUID,
                arg_names,
                arg_types,
                0,
                0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                false,
                null,
                newlyCreatedRoutines,
                tc);
    }


    /**
     * Create system procedures added in version 10.2.
     * <p/>
     * Create 10.2 system procedures, called by either code creating new
     * database, or code doing hard upgrade from previous version.
     * <p/>
     *
     * @param tc                   booting transaction
     * @param newlyCreatedRoutines set of routines we are creating (used to add permissions later on)
     * @param sysUtilUUID          uuid of the SYSUTIL schema.
     * @throws StandardException Standard exception policy.
     */
    void create_10_2_system_procedures(TransactionController tc,
                                       Set<String> newlyCreatedRoutines,
                                       UUID sysUtilUUID) throws StandardException{

        // void SYSCS_UTIL.SYSCS_BACKUP_DATABASE_NOWAIT(
        //     IN BACKUPDIR        VARCHAR(Limits.DB2_VARCHAR_MAXWIDTH)
        //     )

        {
            // procedure argument names
            String[] arg_names={"BACKUPDIR"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR,Limits.DB2_VARCHAR_MAXWIDTH)
            };

            createSystemProcedureOrFunction(
                    "SYSCS_BACKUP_DATABASE_NOWAIT",
                    sysUtilUUID,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.MODIFIES_SQL_DATA,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc);
        }

        {
            // procedure argument names
            String[] arg_names={"BACKUPDIR","DELETE_ARCHIVED_LOG_FILES"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR,Limits.DB2_VARCHAR_MAXWIDTH),
                    TypeDescriptor.SMALLINT
            };

            createSystemProcedureOrFunction(
                    "SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT",
                    sysUtilUUID,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.MODIFIES_SQL_DATA,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc);
        }

        // SYSIBM.SQLFUNCTIONS(VARCHAR(128), VARCHAR(128), VARCHAR(128), 
        // VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names={
                    "CATALOGNAME",
                    "SCHEMANAME",
                    "FUNCNAME",
                    "OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR,4000)};

            createSystemProcedureOrFunction(
                    "SQLFUNCTIONS",
                    getSysIBMSchemaDescriptor().getUUID(),
                    arg_names,
                    arg_types,
                    0,
                    1,
                    RoutineAliasInfo.READS_SQL_DATA,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc);
        }

        // SYSIBM.SQLFUNCTIONPARAMS(VARCHAR(128), VARCHAR(128),
        // VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names={
                    "CATALOGNAME",
                    "SCHEMANAME",
                    "FUNCNAME",
                    "PARAMNAME",
                    "OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR,4000)};

            createSystemProcedureOrFunction(
                    "SQLFUNCTIONPARAMS",
                    getSysIBMSchemaDescriptor().getUUID(),
                    arg_names,
                    arg_types,
                    0,
                    1,
                    RoutineAliasInfo.READS_SQL_DATA,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc);
        }
    }

    /**
     * Create system procedures added in version 10.3.
     * Create 10.3 system procedures related to the LOB Methods ,
     * called by either code creating new
     * database, or code doing hard upgrade from previous version.
     *
     * @param tc                   an instance of the TransactionController class.
     * @param newlyCreatedRoutines set of routines we are creating (used to add permissions later on)
     * @throws StandardException Standard exception policy.
     */
    private void create_10_3_system_procedures_SYSIBM(TransactionController tc,
                                                      Set<String> newlyCreatedRoutines) throws StandardException{
        //create 10.3 functions used by LOB methods.
        UUID schema_uuid=getSysIBMSchemaDescriptor().getUUID();
        String procClass=LOBStoredProcedure.class.getCanonicalName();
        {
            createSystemProcedureOrFunction(
                    "CLOBCREATELOCATOR",
                    schema_uuid,
                    null,
                    null,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    TypeDescriptor.INTEGER,
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR"};

            TypeDescriptor[] arg_types={TypeDescriptor.INTEGER};

            createSystemProcedureOrFunction(
                    "CLOBRELEASELOCATOR",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","SEARCHSTR","POS"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR),
                    DataTypeDescriptor.getCatalogType(Types.BIGINT)
            };
            createSystemProcedureOrFunction(
                    "CLOBGETPOSITIONFROMSTRING",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    DataTypeDescriptor.getCatalogType(
                            Types.BIGINT),
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","SEARCHLOCATOR","POS"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT)
            };
            createSystemProcedureOrFunction(
                    "CLOBGETPOSITIONFROMLOCATOR",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT),
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR"};

            // procedure argument types
            TypeDescriptor[] arg_types={TypeDescriptor.INTEGER};
            createSystemProcedureOrFunction(
                    "CLOBGETLENGTH",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT),
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","POS","LEN"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(
                            Types.BIGINT),
                    TypeDescriptor.INTEGER
            };
            createSystemProcedureOrFunction(
                    "CLOBGETSUBSTRING",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR,Limits.MAX_CLOB_RETURN_LEN),
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","POS","LEN","REPLACESTR"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT),
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR)
            };
            createSystemProcedureOrFunction(
                    "CLOBSETSTRING",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","LEN"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT)
            };
            createSystemProcedureOrFunction(
                    "CLOBTRUNCATE",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }

        //Now create the Stored procedures required for BLOB
        {

            createSystemProcedureOrFunction(
                    "BLOBCREATELOCATOR",
                    schema_uuid,
                    null,
                    null,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    TypeDescriptor.INTEGER,
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR"};

            TypeDescriptor[] arg_types={TypeDescriptor.INTEGER};

            createSystemProcedureOrFunction(
                    "BLOBRELEASELOCATOR",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","SEARCHBYTES","POS"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.VARBINARY),
                    DataTypeDescriptor.getCatalogType(Types.BIGINT)
            };
            createSystemProcedureOrFunction(
                    "BLOBGETPOSITIONFROMBYTES",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    DataTypeDescriptor.getCatalogType(
                            Types.BIGINT),
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","SEARCHLOCATOR","POS"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT)
            };
            createSystemProcedureOrFunction(
                    "BLOBGETPOSITIONFROMLOCATOR",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT),
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER
            };
            createSystemProcedureOrFunction(
                    "BLOBGETLENGTH",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT),
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","POS","LEN"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT),
                    TypeDescriptor.INTEGER
            };
            createSystemProcedureOrFunction(
                    "BLOBGETBYTES",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    DataTypeDescriptor.getCatalogType(Types.VARBINARY,Limits.MAX_BLOB_RETURN_LEN),
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","POS","LEN","REPLACEBYTES"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT),
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.VARBINARY)
            };
            createSystemProcedureOrFunction(
                    "BLOBSETBYTES",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
        {
            String[] arg_names={"LOCATOR","LEN"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT)
            };
            createSystemProcedureOrFunction(
                    "BLOBTRUNCATE",
                    schema_uuid,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc,
                    procClass);
        }
    }

    /**
     * Create the System procedures that are added to 10.5.
     *
     * @param tc                   an instance of the TransactionController.
     * @param newlyCreatedRoutines set of routines we are creating (used to add permissions later on)
     * @throws StandardException Standard exception policy.
     */
    void create_10_5_system_procedures(TransactionController tc,Set<String> newlyCreatedRoutines)
            throws StandardException{
        // Create the procedures in the SYSCS_UTIL schema.
        UUID sysUtilUUID=getSystemUtilSchemaDescriptor().getUUID();

        // void SYSCS_UTIL.SYSCS_UPDATE_STATISTICS(varchar(128), varchar(128), varchar(128))
        {
            // procedure argument names
            String[] arg_names={"SCHEMANAME","TABLENAME","INDEXNAME"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    CATALOG_TYPE_SYSTEM_IDENTIFIER

            };

            createSystemProcedureOrFunction(
                    "SYSCS_UPDATE_STATISTICS",
                    sysUtilUUID,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.MODIFIES_SQL_DATA,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc);
        }
    }

    /**
     * Create the System procedures that are added to 10.6.
     *
     * @param tc an instance of the TransactionController.
     * @throws StandardException Standard exception policy.
     */
    void create_10_6_system_procedures(TransactionController tc,Set<String> newlyCreatedRoutines) throws StandardException{
        // Create the procedures in the SYSCS_UTIL schema.
        UUID sysUtilUUID=getSystemUtilSchemaDescriptor().getUUID();
        // void SYSCS_UTIL.SYSCS_SET_XPLAIN_MODE(smallint mode)
        {
            // procedure argument names
            String[] arg_names={"ENABLE"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    TypeDescriptor.INTEGER,
            };

            createSystemProcedureOrFunction(
                    "SYSCS_SET_XPLAIN_MODE",
                    sysUtilUUID,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.CONTAINS_SQL,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc);
        }

        // SMALLINT SYSCS_UTIL.SYSCS_GET_XPLAIN_MODE()
        {

            createSystemProcedureOrFunction(
                    "SYSCS_GET_XPLAIN_MODE",
                    sysUtilUUID,
                    null,
                    null,
                    0,
                    0,
                    RoutineAliasInfo.READS_SQL_DATA,
                    false,
                    TypeDescriptor.INTEGER,
                    newlyCreatedRoutines,
                    tc);
        }


        // void SYSCS_UTIL.SYSCS_SET_XPLAIN_SCHEMA(String schemaName)
        {
            // procedure argument names
            String[] arg_names={"SCHEMANAME"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
            };

            createSystemProcedureOrFunction(
                    "SYSCS_SET_XPLAIN_SCHEMA",
                    sysUtilUUID,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.MODIFIES_SQL_DATA,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc);
        }

        // STRING SYSCS_UTIL.SYSCS_GET_XPLAIN_SCHEMA()
        {
            createSystemProcedureOrFunction(
                    "SYSCS_GET_XPLAIN_SCHEMA",
                    sysUtilUUID,
                    null,
                    null,
                    0,
                    0,
                    RoutineAliasInfo.READS_SQL_DATA,
                    false,
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    newlyCreatedRoutines,
                    tc);
        }
    }

    /**
     * Create the System procedures that are added in 10.3.
     *
     * @param tc                   an instance of the TransactionController.
     * @param newlyCreatedRoutines set of routines we are creating (used to add permissions later on)
     * @throws StandardException Standard exception policy.
     */
    void create_10_3_system_procedures(TransactionController tc,Set<String> newlyCreatedRoutines) throws StandardException{
        // Create the procedures in the SYSCS_UTIL schema.
        create_10_3_system_procedures_SYSCS_UTIL(tc,newlyCreatedRoutines);
        //create the procedures in the SYSIBM schema
        create_10_3_system_procedures_SYSIBM(tc,newlyCreatedRoutines);
    }

    /**
     * Create system procedures that are part of the
     * SYSCS_UTIL schema added in version 10.3.
     * <p/>
     * Create 10.3 system procedures, called by either code creating new
     * database, or code doing hard upgrade from previous version.
     * <p/>
     *
     * @param tc                   an instance of the Transaction Controller.
     * @param newlyCreatedRoutines set of routines we are creating (used to add permissions later on)
     * @throws StandardException Standard exception policy.
     */
    void create_10_3_system_procedures_SYSCS_UTIL(TransactionController tc,
                                                  Set<String> newlyCreatedRoutines) throws StandardException{
        UUID sysUtilUUID=getSystemUtilSchemaDescriptor().getUUID();

        // void SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE()
        {
            createSystemProcedureOrFunction(
                    "SYSCS_EMPTY_STATEMENT_CACHE",
                    sysUtilUUID,
                    null,
                    null,
                    0,
                    0,
                    RoutineAliasInfo.NO_SQL,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc);
        }

    }

    /**
     * <p>
     * Create system procedures that are part of the
     * SYSCS_UTIL schema added in version 10.9. These include the procedures for managing NATIVE credentials.
     * See DERBY-866.
     * </p>
     *
     * @param tc                   an instance of the Transaction Controller.
     * @param newlyCreatedRoutines set of routines we are creating (used to add permissions later on)
     */
    void create_10_9_system_procedures(TransactionController tc,Set<String> newlyCreatedRoutines) throws StandardException{
        UUID sysUtilUUID=getSystemUtilSchemaDescriptor().getUUID();

        //
        // SYSCS_CREATE_USER( IN USERNAME  VARCHAR(128), IN PASSWORD VARCHAR(32672) )
        //

        {
            // procedure argument names
            String[] arg_names={"userName","password"};

            // procedure argument types
            TypeDescriptor[] arg_types={CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR,32672)
            };

            createSystemProcedureOrFunction
                    (
                            "SYSCS_CREATE_USER",
                            sysUtilUUID,
                            arg_names,
                            arg_types,
                            0,
                            0,
                            RoutineAliasInfo.MODIFIES_SQL_DATA,
                            false,
                            null,
                            newlyCreatedRoutines,
                            tc
                    );
        }

        //
        // SYSCS_RESET_PASSWORD( IN USERNAME  VARCHAR(128), IN PASSWORD VARCHAR(32672) )
        //

        {
            // procedure argument names
            String[] arg_names={"userName","password"};

            // procedure argument types
            TypeDescriptor[] arg_types={
                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
                    DataTypeDescriptor.getCatalogType(Types.VARCHAR,32672)
            };

            createSystemProcedureOrFunction
                    (
                            "SYSCS_RESET_PASSWORD",
                            sysUtilUUID,
                            arg_names,
                            arg_types,
                            0,
                            0,
                            RoutineAliasInfo.MODIFIES_SQL_DATA,
                            false,
                            null,
                            newlyCreatedRoutines,
                            tc
                    );
        }

        //
        // SYSCS_MODIFY_PASSWORD( IN PASSWORD VARCHAR(32672) )
        //

        {
            // procedure argument names
            String[] arg_names={"password"};

            // procedure argument types
            TypeDescriptor[] arg_types={DataTypeDescriptor.getCatalogType(Types.VARCHAR,32672)};

            createSystemProcedureOrFunction(
                    "SYSCS_MODIFY_PASSWORD",
                    sysUtilUUID,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.MODIFIES_SQL_DATA,
                    false,
                    null,
                    newlyCreatedRoutines,
                    tc
            );
        }

        //
        // SYSCS_DROP_USER( IN USERNAME  VARCHAR(128) )
        //

        {
            // procedure argument names
            String[] arg_names={"userName"};

            // procedure argument types
            TypeDescriptor[] arg_types={CATALOG_TYPE_SYSTEM_IDENTIFIER};

            createSystemProcedureOrFunction
                    (
                            "SYSCS_DROP_USER",
                            sysUtilUUID,
                            arg_names,
                            arg_types,
                            0,
                            0,
                            RoutineAliasInfo.MODIFIES_SQL_DATA,
                            false,
                            null,
                            newlyCreatedRoutines,
                            tc
                    );
        }

        // BIGINT
        // SYSCS_UTIL.SYSCS_PEEK_AT_SEQUENCE( VARCHAR(128), VARCHAR(128) )

        {
            // procedure argument names
            String[] arg_names={"schemaName","sequenceName"};

            // procedure argument types
            TypeDescriptor[] arg_types={CATALOG_TYPE_SYSTEM_IDENTIFIER,CATALOG_TYPE_SYSTEM_IDENTIFIER};

            createSystemProcedureOrFunction(
                    "SYSCS_PEEK_AT_SEQUENCE",
                    sysUtilUUID,
                    arg_names,
                    arg_types,
                    0,
                    0,
                    RoutineAliasInfo.READS_SQL_DATA,
                    false,
                    DataTypeDescriptor.getCatalogType(Types.BIGINT),
                    newlyCreatedRoutines,
                    tc);
        }

        // void SYSCS_UTIL.SYSCS_DROP_STATISTICS(varchar(128), varchar(128), varchar(128))

        // SYSCS_DROP_STATISTICS works in Derby but is not valid for Splice.
        // Leave it out of dictionary until we implement it properly.
        // If it is invoked from a system where this is already present,
        // it will throw an unsupported operation exception.
//        {
//            // procedure argument names
//            String[] arg_names={"SCHEMANAME","TABLENAME","INDEXNAME"};
//
//            // procedure argument types
//            TypeDescriptor[] arg_types={
//                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
//                    CATALOG_TYPE_SYSTEM_IDENTIFIER,
//                    CATALOG_TYPE_SYSTEM_IDENTIFIER
//
//            };
//
//            createSystemProcedureOrFunction(
//                    "SYSCS_DROP_STATISTICS",
//                    sysUtilUUID,
//                    arg_names,
//                    arg_types,
//                    0,
//                    0,
//                    RoutineAliasInfo.MODIFIES_SQL_DATA,
//                    false,
//                    null,
//                    newlyCreatedRoutines,
//                    tc);
//        }
    }


    /*
    ** Priv block code to load net work server meta data queries.
    */

    private String spsSet;

    private synchronized Properties getQueryDescriptions(boolean net){
        spsSet=net?"/com/splicemachine/db/impl/sql/catalog/metadata_net.properties":"/com/splicemachine/db/impl/jdbc/metadata.properties";
        return (Properties)java.security.AccessController.doPrivileged(this);
    }

    @Override
    public final Object run(){
        // SECURITY PERMISSION - IP3
        Properties p=new Properties();
        try {

            // SECURITY PERMISSION - IP3
            try (InputStream is=getClass().getResourceAsStream(spsSet)) {
                p.load(is);
            }
        } catch(IOException ignored){
        }
        return p;
    }


    private static <T> List<T> newSList(){
        return Collections.synchronizedList(new LinkedList<T>());
    }

    /**
     * Get one user's privileges on a table
     *
     * @param tableUUID       the uuid of the table
     * @param authorizationId The user name
     * @return a TablePermsDescriptor or null if the user has no permissions on the table.
     * @throws StandardException
     */
    @Override
    public TablePermsDescriptor getTablePermissions(UUID tableUUID,String authorizationId) throws StandardException{
        TablePermsDescriptor key=new TablePermsDescriptor(this,authorizationId,null,tableUUID);
        return (TablePermsDescriptor)getPermissions(key);
    } // end of getTablePermissions

    @Override
    public TablePermsDescriptor getTablePermissions(UUID tablePermsUUID) throws StandardException{
        TablePermsDescriptor key=new TablePermsDescriptor(this,tablePermsUUID);
        return getUncachedTablePermsDescriptor(key);
    }

    /**
     * GEt Schema Permission for a specific authorizationId
     * @param schemaPermsUUID
     * @param authorizationId The user name
     * @return
     * @throws StandardException
     */

    @Override
    public SchemaPermsDescriptor getSchemaPermissions(UUID schemaPermsUUID, String authorizationId) throws StandardException{
        SchemaPermsDescriptor key=new SchemaPermsDescriptor(this,authorizationId,null,schemaPermsUUID);
        return (SchemaPermsDescriptor)getPermissions(key);
    }

    @Override
    public SchemaPermsDescriptor getSchemaPermissions(UUID schemaPermsUUID) throws StandardException{
        SchemaPermsDescriptor key=new SchemaPermsDescriptor(this,schemaPermsUUID);
        return getUncachedSchemaPermsDescriptor(key);
    }

    public Object getPermissions(PermissionsDescriptor key) throws StandardException {
        return getPermissions(key, true);
    }

    public Object getPermissions(PermissionsDescriptor key, boolean metadataAccessRestrictionEnabled) throws StandardException{

        Optional<PermissionsDescriptor> optional = dataDictionaryCache.permissionCacheFind(key);
        if (optional != null)
            return optional.orNull();

        PermissionsDescriptor permissions = null;

        if( key instanceof TablePermsDescriptor) {

            TablePermsDescriptor tablePermsKey = (TablePermsDescriptor) key;
            permissions = getUncachedTablePermsDescriptor(tablePermsKey);
            if( permissions == null)
            {
                // The owner has all privileges unless they have been revoked.
                TableDescriptor td = getTableDescriptor(tablePermsKey.getTableUUID());
                SchemaDescriptor sd = td.getSchemaDescriptor();

                if( metadataAccessRestrictionEnabled && sd.isSystemViewSchema() ||
                        !metadataAccessRestrictionEnabled && sd.isSystemSchema())
                {
                    // make sytem tables or views publicly accessible and readable depending on metadataAccessRestritionEnabled or not
                    permissions = new TablePermsDescriptor( this,
                            tablePermsKey.getGrantee(),
                            (String) null,
                            tablePermsKey.getTableUUID(),
                            "Y", "N", "N", "N", "N", "N");
                    // give the permission the same UUID as the system table
                    ((TablePermsDescriptor) permissions).setUUID( tablePermsKey.getTableUUID() );
                } else if( tablePermsKey.getGrantee().equals( sd.getAuthorizationId()))
                {
                    permissions = new TablePermsDescriptor( this,
                            tablePermsKey.getGrantee(),
                            Authorizer.SYSTEM_AUTHORIZATION_ID,
                            tablePermsKey.getTableUUID(),
                            "Y", "Y", "Y", "Y", "Y", "Y");
                }
            }
        }
        else if( key instanceof ColPermsDescriptor)
        {
            ColPermsDescriptor colPermsKey = (ColPermsDescriptor) key;
            permissions = getUncachedColPermsDescriptor(colPermsKey );
            if( permissions == null)
                permissions = new ColPermsDescriptor( this,
                        colPermsKey.getGrantee(),
                        (String) null,
                        colPermsKey.getTableUUID(),
                        colPermsKey.getType(),
                        (FormatableBitSet) null);
        }
        else if( key instanceof RoutinePermsDescriptor)
        {
            RoutinePermsDescriptor routinePermsKey = (RoutinePermsDescriptor) key;
            permissions = getUncachedRoutinePermsDescriptor( routinePermsKey);
            if( permissions == null)
            {
                // The owner has all privileges unless they have been revoked.
                try
                {
                    AliasDescriptor ad = getAliasDescriptor( routinePermsKey.getRoutineUUID());
                    SchemaDescriptor sd = getSchemaDescriptor( ad.getSchemaUUID(),
                            ConnectionUtil.getCurrentLCC().getTransactionExecute());
                    if (sd.isSystemSchema() && !sd.isSchemaWithGrantableRoutines())
                        permissions = new RoutinePermsDescriptor( this,
                                routinePermsKey.getGrantee(),
                                (String) null,
                                routinePermsKey.getRoutineUUID(),
                                true);
                    else if( routinePermsKey.getGrantee().equals( sd.getAuthorizationId()))
                        permissions = new RoutinePermsDescriptor( this,
                                routinePermsKey.getGrantee(),
                                Authorizer.SYSTEM_AUTHORIZATION_ID,
                                routinePermsKey.getRoutineUUID(),
                                true);
                }
                catch( java.sql.SQLException sqle)
                {
                    throw StandardException.plainWrapException( sqle);
                }
            }
        }
        else if (key instanceof SchemaPermsDescriptor) {
            SchemaPermsDescriptor schemaPermsKey = (SchemaPermsDescriptor) key;
            permissions = getUncachedSchemaPermsDescriptor(schemaPermsKey);

            if( permissions == null)
            {
                try {
                    // The owner has all privileges unless they have been revoked.
                    SchemaDescriptor sd = getSchemaDescriptor(schemaPermsKey.getSchemaUUID(), ConnectionUtil.getCurrentLCC().getTransactionExecute());

                    if (metadataAccessRestrictionEnabled && sd.isSystemViewSchema() ||
                            !metadataAccessRestrictionEnabled && sd.isSystemSchema()) {
                        // allow read access to public
                        permissions = new SchemaPermsDescriptor(this,
                                schemaPermsKey.getGrantee(),
                                (String) null,
                                schemaPermsKey.getSchemaUUID(),
                                "Y", "N", "N", "N", "N", "N", "N", "Y");
                    } else if (schemaPermsKey.getGrantee().equals(sd.getAuthorizationId())) {
                        permissions = new SchemaPermsDescriptor(this,
                                schemaPermsKey.getGrantee(),
                                Authorizer.SYSTEM_AUTHORIZATION_ID,
                                schemaPermsKey.getSchemaUUID(),
                                "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y" );
                    }
                } catch( java.sql.SQLException sqle) {
                    throw StandardException.plainWrapException( sqle);
                }
            }
        }
        else if( key instanceof PermDescriptor)
        {
            PermDescriptor permKey = (PermDescriptor) key;
            permissions = getUncachedGenericPermDescriptor( permKey);
            if( permissions == null)
            {
                // The owner has all privileges unless they have been revoked.
                String objectType = permKey.getObjectType();
                String privilege = permKey.getPermission();
                UUID protectedObjectsID = permKey.getPermObjectId();


                PrivilegedSQLObject pso = PermDescriptor.getProtectedObject( this, protectedObjectsID, objectType );
                SchemaDescriptor sd = pso.getSchemaDescriptor();
                if( permKey.getGrantee().equals( sd.getAuthorizationId()))
                {
                    permissions = new PermDescriptor
                            (
                                    this,
                                    null,
                                    objectType,
                                    pso.getUUID(),
                                    privilege,
                                    Authorizer.SYSTEM_AUTHORIZATION_ID,
                                    permKey.getGrantee(),
                                    true
                            );
                }
            }
        }

        dataDictionaryCache.permissionCacheAdd(key, Optional.fromNullable(permissions));

        return permissions;
    }

    @Override
    public ColPermsDescriptor getColumnPermissions(UUID colPermsUUID) throws StandardException{
        return null; // Col Permissions not implemented
    }

    /**
     * Get one user's column privileges for a table.
     *
     * @param tableUUID       the uuid of the table of interest
     * @param privType        (as int) Authorizer.SELECT_PRIV, Authorizer.UPDATE_PRIV, or Authorizer.REFERENCES_PRIV
     * @param forGrant        whether or not we are looking for grant priviledges
     * @param authorizationId The user name
     * @return a ColPermsDescriptor or null if the user has no separate column
     * permissions of the specified type on the table. Note that the user may have been granted
     * permission on all the columns of the table (no column list), in which case this routine
     * will return null. You must also call getTablePermissions to see if the user has permission
     * on a set of columns.
     * @throws StandardException
     */
    @Override
    public ColPermsDescriptor getColumnPermissions(UUID tableUUID,
                                                   int privType,
                                                   boolean forGrant,
                                                   String authorizationId) throws StandardException{
        return null; // Col Permissions not implemented
    } // end of getColumnPermissions

    /**
     * Get one user's permissions for a routine (function or procedure).
     *
     * @param routineUUID     the routine's uuid
     * @param authorizationId The user's name
     * @return The descriptor of the users permissions for the routine.
     * @throws StandardException
     */
    public RoutinePermsDescriptor getRoutinePermissions(UUID routineUUID,String authorizationId) throws StandardException{
        RoutinePermsDescriptor key=new RoutinePermsDescriptor(this,authorizationId,null,routineUUID);

        return (RoutinePermsDescriptor)getPermissions(key);
    } // end of getRoutinePermissions

    @Override
    public RoutinePermsDescriptor getRoutinePermissions(UUID routinePermsUUID) throws StandardException{
        RoutinePermsDescriptor key=new RoutinePermsDescriptor(this,routinePermsUUID);
        return getUncachedRoutinePermsDescriptor(key);
    }

    @Override
    public RoutinePermsDescriptor getRoutinePermissions(UUID routineUUID,
                                                 List<RoutinePermsDescriptor> permsList) throws StandardException{
        RoutinePermsDescriptor key=new RoutinePermsDescriptor(this,null,null,routineUUID);
        TabInfoImpl ti=getNonCoreTI(SYSROUTINEPERMS_CATALOG_NUM);
        PermissionsCatalogRowFactory rowFactory=(PermissionsCatalogRowFactory)ti.getCatalogRowFactory();
        ExecIndexRow keyRow=rowFactory.buildIndexKeyRow(SYSROUTINEPERMSRowFactory.ALIASID_INDEX_NUM,key);
        return (RoutinePermsDescriptor)getDescriptorViaIndex(SYSROUTINEPERMSRowFactory.ALIASID_INDEX_NUM,keyRow,null,ti,null,permsList,false);
    } // end of getRoutinePermissions
    /**
     * Add or remove a permission to/from the permission database.
     *
     * @param add     if true then the permission is added, if false the permission is removed
     * @param perm    the permission to deal with
     * @param grantee who the permission is granted to
     * @param tc      the transaction to use
     * @return 3 values: 0: no change, 1: add permission; 2: remove permission
     *  hence the caller of this method know whether to send invalidation
     * actions to PermssionDescriptor's dependents and dictionary cache.
     */
    @Override
    public PermissionOperation addRemovePermissionsDescriptor(boolean add,
                                                  PermissionsDescriptor perm,
                                                  String grantee,
                                                  TransactionController tc) throws StandardException{
        int catalogNumber=perm.getCatalogNumber();

        // It is possible for grant statements to look like following
        //   grant execute on function f_abs to mamata2, mamata3;
        //   grant all privileges on t11 to mamata2, mamata3;
        // This means that dd.addRemovePermissionsDescriptor will be called
        // twice for TablePermsDescriptor and twice for RoutinePermsDescriptor, 
        // once for each grantee.
        // First it's called for mamta2. When a row is inserted for mamta2 
        // into the correct system table for the permission descriptor, the 
        // permission descriptor's uuid gets populated with the uuid of 
        // the row that just got inserted into the system table for mamta2
        // Now, when dd.addRemovePermissionsDescriptor gets called again for
        // mamta3, the permission descriptor's uuid will still be set to
        // the uuid that was used for mamta2. If we do not reset the
        // uuid to null, we will think that there is a duplicate row getting
        // inserted for the same uuid. In order to get around this, we should 
        // reset the UUID of passed PermissionDescriptor everytime this method 
        // is called. This way, there will be no leftover values from previous
        // call of this method.
        perm.setUUID(null);
        perm.setGrantee(grantee);
        TabInfoImpl ti=getNonCoreTI(catalogNumber);
        PermissionsCatalogRowFactory rf=(PermissionsCatalogRowFactory)ti.getCatalogRowFactory();
        int primaryIndexNumber=rf.getPrimaryKeyIndexNumber();

        ExecIndexRow key=rf.buildIndexKeyRow(primaryIndexNumber,perm);
        ExecRow existingRow=ti.getRow(tc,key,primaryIndexNumber);
        if(existingRow==null){
            if(!add){
                //we didn't find an entry in system catalog and this is revoke
                //so that means there is nothing to revoke. Simply return.
                //No need to reset permission descriptor's uuid because
                //no row was ever found in system catalog for the given
                //permission and hence uuid can't be non-null
                return PermissionOperation.NOCHANGE;
            }else{
                //We didn't find an entry in system catalog and this is grant so 
                //so that means we have to enter a new row in system catalog for
                //this grant.
                ExecRow row=ti.getCatalogRowFactory().makeRow(perm,null);
                int insertRetCode=ti.insertRow(row,tc);
                if(SanityManager.DEBUG){
                    SanityManager.ASSERT(insertRetCode==TabInfoImpl.ROWNOTDUPLICATE,
                            "Race condition in inserting table privilege.");
                }
            }
        }else{
            // add/remove these permissions to/from the existing permissions
            boolean[] colsChanged=new boolean[existingRow.nColumns()];
            boolean[] indicesToUpdate=new boolean[rf.getNumIndexes()];
            int changedColCount;
            if(add){
                changedColCount=rf.orPermissions(existingRow,perm,colsChanged);
            }else{
                changedColCount=rf.removePermissions(existingRow,perm,colsChanged);
            }

            if(changedColCount==0){
                //grant/revoke privilege didn't change anything and hence 
                //just return
                return PermissionOperation.NOCHANGE;
            }

            //set the uuid of the passed permission descriptor to
            //corresponding rows's uuid in permissions system table. The
            //permission descriptor's uuid is required to have the
            //dependency manager send the revoke privilege action to
            //all the dependent objects on that permission descriptor.
            rf.setUUIDOfThePassedDescriptor(existingRow,perm);

            if(changedColCount<0){
                // No permissions left in the current row
                ti.deleteRow(tc,key,primaryIndexNumber);
            }else if(changedColCount>0){
                int[] colsToUpdate=new int[changedColCount];
                changedColCount=0;
                for(int i=0;i<colsChanged.length;i++){
                    if(colsChanged[i])
                        colsToUpdate[changedColCount++]=i+1;
                }
                assert changedColCount==colsToUpdate.length:
                        "return value of "+rf.getClass().getName()+
                                ".orPermissions does not match the number of booleans it set in colsChanged.";
                ti.updateRow(key,existingRow,primaryIndexNumber,indicesToUpdate,colsToUpdate,tc);
            }
        }
        // Remove cached permissions data. The cache may hold permissions data for this key even if
        // the row in the permissions table is new. In that case the cache may have an entry indicating no
        // permissions
        removePermEntryInCache(perm);

        return add?PermissionOperation.ADD:PermissionOperation.REMOVE;
    } // end of addPermissionsDescriptor

    /**
     * Get a table permissions descriptor from the system tables, without going through the cache.
     * This method is called to fill the permissions cache.
     *
     * @return a TablePermsDescriptor that describes the table permissions granted to the grantee, null
     * if no table-level permissions have been granted to him on the table.
     * @throws StandardException
     */
    public TablePermsDescriptor getUncachedTablePermsDescriptor(TablePermsDescriptor key) throws StandardException{
        if(key.getObjectID()==null){
            //the TABLEPERMSID for SYSTABLEPERMS is not known, so use
            //table id, grantor and granteee to find TablePermsDescriptor
            return (TablePermsDescriptor)
                    getUncachedPermissionsDescriptor(SYSTABLEPERMS_CATALOG_NUM,
                            SYSTABLEPERMSRowFactory.GRANTEE_TABLE_GRANTOR_INDEX_NUM,
                            key);
        }else{
            //we know the TABLEPERMSID for SYSTABLEPERMS, so use that to
            //find TablePermsDescriptor from the sytem table
            return (TablePermsDescriptor)
                    getUncachedPermissionsDescriptor(SYSTABLEPERMS_CATALOG_NUM,
                            SYSTABLEPERMSRowFactory.TABLEPERMSID_INDEX_NUM,key);
        }
    } // end of getUncachedTablePermsDescriptor

    /**
     * Get a schema permissions descriptor from the system tables, without going through the cache.
     * This method is called to fill the permissions cache.
     *
     * @param key
     * @return SchemaPermsDescriptor that describes the table permissions granted to the grantee, null
     * if no schema-table permissions have been granted to him on the schema
     * @throws StandardException
     */
    public SchemaPermsDescriptor getUncachedSchemaPermsDescriptor(SchemaPermsDescriptor key) throws StandardException{
        if(key.getObjectID()==null){
            //the SCHEMAPERMSID for SYSTABLEPERMS is not known, so use
            //table id, grantor and granteee to find SchemaPermsDescriptor
            return (SchemaPermsDescriptor)
                    getUncachedPermissionsDescriptor(SYSSCHEMAPERMS_CATALOG_NUM,
                            SYSSCHEMAPERMSRowFactory.GRANTEE_SCHEMA_GRANTOR_INDEX_NUM,
                            key);
        }else{
            //we know the SCHEMAPERMSID for SYSTABLEPERMS, so use that to
            //find SchemaPermsDescriptor from the sytem table
            return (SchemaPermsDescriptor)
                    getUncachedPermissionsDescriptor(SYSSCHEMAPERMS_CATALOG_NUM,
                            SYSSCHEMAPERMSRowFactory.SCHEMAPERMSID_INDEX_NUM,key);
        }
    } // end of getUncachedSchemaPermsDescriptor

    /**
     * Get a column permissions descriptor from the system tables, without going through the cache.
     * This method is called to fill the permissions cache.
     *
     * @return a ColPermsDescriptor that describes the column permissions granted to the grantee, null
     * if no column permissions have been granted to him on the table.
     * @throws StandardException
     */
    public ColPermsDescriptor getUncachedColPermsDescriptor(ColPermsDescriptor key) throws StandardException{
        if(key.getObjectID()==null){
            //the COLPERMSID for SYSCOLPERMS is not known, so use tableid,
            //privilege type, grantor and granteee to find ColPermsDescriptor
            return (ColPermsDescriptor)
                    getUncachedPermissionsDescriptor(SYSCOLPERMS_CATALOG_NUM,
                            SYSCOLPERMSRowFactory.GRANTEE_TABLE_TYPE_GRANTOR_INDEX_NUM,
                            key);
        }else{
            //we know the COLPERMSID for SYSCOLPERMS, so use that to
            //find ColPermsDescriptor from the sytem table
            return (ColPermsDescriptor)
                    getUncachedPermissionsDescriptor(SYSCOLPERMS_CATALOG_NUM,
                            SYSCOLPERMSRowFactory.COLPERMSID_INDEX_NUM,
                            key);
        }
    } // end of getUncachedColPermsDescriptor

    private TupleDescriptor getUncachedPermissionsDescriptor(int catalogNumber,
                                                             int indexNumber,
                                                             PermissionsDescriptor key) throws StandardException{
        TabInfoImpl ti=getNonCoreTI(catalogNumber);
        PermissionsCatalogRowFactory rowFactory=(PermissionsCatalogRowFactory)ti.getCatalogRowFactory();
        ExecIndexRow keyRow=rowFactory.buildIndexKeyRow(indexNumber,key);
        return getDescriptorViaIndex(indexNumber,keyRow,null,ti,null,null,false);
    } // end of getUncachedPermissionsDescriptor
    /**
     * Get a routine permissions descriptor from the system tables, without going through the cache.
     * This method is called to fill the permissions cache.
     *
     * @return a RoutinePermsDescriptor that describes the table permissions granted to the grantee, null
     * if no table-level permissions have been granted to him on the table.
     * @throws StandardException
     */
    RoutinePermsDescriptor getUncachedRoutinePermsDescriptor(RoutinePermsDescriptor key) throws StandardException{
        if(key.getObjectID()==null){
            //the ROUTINEPERMSID for SYSROUTINEPERMS is not known, so use aliasid,
            //grantor and granteee to find RoutinePermsDescriptor
            return (RoutinePermsDescriptor)
                    getUncachedPermissionsDescriptor(SYSROUTINEPERMS_CATALOG_NUM,
                            SYSROUTINEPERMSRowFactory.GRANTEE_ALIAS_GRANTOR_INDEX_NUM,
                            key);
        }else{
            //we know the ROUTINEPERMSID for SYSROUTINEPERMS, so use that to
            //find RoutinePermsDescriptor from the sytem table
            return (RoutinePermsDescriptor)
                    getUncachedPermissionsDescriptor(SYSROUTINEPERMS_CATALOG_NUM,
                            SYSROUTINEPERMSRowFactory.ROUTINEPERMSID_INDEX_NUM,key);

        }
    } // end of getUncachedRoutinePermsDescriptor

    private String[][] DIAG_VTI_TABLE_CLASSES=
            {
                    {"LOCK_TABLE","com.splicemachine.db.diag.LockTable"},
                    {"STATEMENT_CACHE","com.splicemachine.db.diag.StatementCache"},
                    {"TRANSACTION_TABLE","com.splicemachine.db.diag.TransactionTable"},
                    {"ERROR_MESSAGES","com.splicemachine.db.diag.ErrorMessages"},
            };

    private String[][] DIAG_VTI_TABLE_FUNCTION_CLASSES=
            {
                    {"SPACE_TABLE","com.splicemachine.db.diag.SpaceTable"},
                    {"ERROR_LOG_READER","com.splicemachine.db.diag.ErrorLogReader"},
                    {"STATEMENT_DURATION","com.splicemachine.db.diag.StatementDuration"},
                    {"CONTAINED_ROLES","com.splicemachine.db.diag.ContainedRoles"},
            };

    @Override
    public String getVTIClass(TableDescriptor td,boolean asTableFunction) throws StandardException{
        if(SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME.equals(
                td.getSchemaName())){
            return getBuiltinVTIClass(td,asTableFunction);
        }else{// see if it's a user-defined table function
            String schemaName=td.getSchemaName();
            String functionName=td.getDescriptorName();
            SchemaDescriptor sd=getSchemaDescriptor(td.getSchemaName(),null,true);

            if(sd!=null){
                AliasDescriptor ad=getAliasDescriptor(sd.getUUID().toString(),functionName,AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR);

                if((ad!=null) && ad.isTableFunction()){
                    return ad.getJavaClassName();
                }

                throw StandardException.newException(SQLState.LANG_NOT_TABLE_FUNCTION,schemaName,functionName);
            }
        }

        return null;
    }

    @Override
    public String getBuiltinVTIClass(TableDescriptor td,boolean asTableFunction) throws StandardException{
        assert td.getTableType()==TableDescriptor.VTI_TYPE:"getVTIClass: invalid table type "+td;

        /* First check to see if this is a system VTI. Note that if no schema was specified then the
         * call to "td.getSchemaName()" will return the current schema.
         */
        if(SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME.equals(td.getSchemaName())){
            String[][] vtiMappings=asTableFunction?DIAG_VTI_TABLE_FUNCTION_CLASSES:DIAG_VTI_TABLE_CLASSES;

            for(String[] entry : vtiMappings){
                if(entry[0].equals(td.getDescriptorName()))
                    return entry[1];
            }
        }

        return null;
    }

    @Override
    public RoleGrantDescriptor getRoleGrantDescriptor(UUID uuid) throws StandardException{
        DataValueDescriptor UUIDStringOrderable;

        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);

        /* Use UUIDStringOrderable in both start and stop position for
         * scan.
         */
        UUIDStringOrderable=getIDValueAsCHAR(uuid);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,UUIDStringOrderable);

        return (RoleGrantDescriptor)
                getDescriptorViaIndex(SYSROLESRowFactory.SYSROLES_INDEX_UUID_IDX,keyRow,null,ti,null,null,false);
    }


    /**
     * Get the target role definition by searching for a matching row
     * in SYSROLES by rolename where isDef==true.  Read only scan.
     * Uses index on (rolename, isDef) columns.
     *
     * @param roleName The name of the role we're interested in.
     * @return The descriptor (row) for the role
     * @throws StandardException Thrown on error
     * @see DataDictionary#getRoleDefinitionDescriptor
     */
    @Override
    public RoleGrantDescriptor getRoleDefinitionDescriptor(String roleName) throws StandardException{

        Optional<RoleGrantDescriptor> optional = dataDictionaryCache.roleCacheFind(roleName);
        if (optional!=null)
            return optional.orNull();

        DataValueDescriptor roleNameOrderable;
        DataValueDescriptor isDefOrderable;

        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);

        /* Use aliasNameOrderable , isDefOrderable in both start
         * and stop position for scan.
         */
        roleNameOrderable=new SQLVarchar(roleName);
        isDefOrderable=new SQLVarchar("Y");

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,roleNameOrderable);
        keyRow.setColumn(2,isDefOrderable);

        RoleGrantDescriptor rgs = (RoleGrantDescriptor)
                getDescriptorViaIndex(SYSROLESRowFactory.SYSROLES_INDEX_ID_DEF_IDX,keyRow,null,ti,null,null,false);
        dataDictionaryCache.roleCacheAdd(roleName,rgs==null?Optional.<RoleGrantDescriptor>absent():Optional.of(rgs));
        return rgs;
    }


    /**
     * Get the target role by searching for a matching row
     * in SYSROLES by rolename, grantee and grantor.  Read only scan.
     * Uses index on roleid, grantee and grantor columns.
     *
     * @param roleName The name of the role we're interested in.
     * @param grantee  The grantee
     * @return The descriptor for the role grant
     * @throws StandardException Thrown on error
     * @see DataDictionary#getRoleGrantDescriptor(String,String)
     */
    @Override
    public RoleGrantDescriptor getRoleGrantDescriptor(String roleName,
                                                      String grantee) throws StandardException{
        DataValueDescriptor roleNameOrderable;
        DataValueDescriptor granteeOrderable;

        Pair<String, String> roleGrantPair = new Pair<>(roleName, grantee);
        Optional<RoleGrantDescriptor> optional = dataDictionaryCache.roleGrantCacheFind(roleGrantPair);
        if (optional!=null)
            return optional.orNull();

        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);

        /* Use aliasNameOrderable, granteeOrderable
         * in both start and stop position for scan.
         */
        roleNameOrderable=new SQLVarchar(roleName);
        granteeOrderable=new SQLVarchar(grantee);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow = exFactory.getIndexableRow(2);
        keyRow.setColumn(1, roleNameOrderable);
        keyRow.setColumn(2, granteeOrderable);

        RoleGrantDescriptor rgd = (RoleGrantDescriptor)
                getDescriptorViaIndex(SYSROLESRowFactory.SYSROLES_INDEX_ID_EE_OR_IDX,keyRow,null,ti,null,null,false);

        dataDictionaryCache.roleGrantCacheAdd(roleGrantPair,rgd==null?Optional.<RoleGrantDescriptor>absent():Optional.of(rgd));
        return rgd;
    }


    /**
     * Check all dictionary tables and return true if there is any GRANT
     * descriptor containing <code>authId</code> as its grantee.
     *
     * @param authId grantee for which a grant exists or not
     * @param tc     TransactionController for the transaction
     * @return boolean true if such a grant exists
     */
    @Override
    public boolean existsGrantToAuthid(String authId,TransactionController tc) throws StandardException{

        return
                (existsPermByGrantee(
                        authId,
                        tc,
                        SYSTABLEPERMS_CATALOG_NUM,
                        SYSTABLEPERMSRowFactory.GRANTEE_TABLE_GRANTOR_INDEX_NUM,
                        SYSTABLEPERMSRowFactory.
                                GRANTEE_COL_NUM_IN_GRANTEE_TABLE_GRANTOR_INDEX) ||
                        existsPermByGrantee(
                                authId,
                                tc,
                                SYSCOLPERMS_CATALOG_NUM,
                                SYSCOLPERMSRowFactory.GRANTEE_TABLE_TYPE_GRANTOR_INDEX_NUM,
                                SYSCOLPERMSRowFactory.
                                        GRANTEE_COL_NUM_IN_GRANTEE_TABLE_TYPE_GRANTOR_INDEX) ||
                        existsPermByGrantee(
                                authId,
                                tc,
                                SYSROUTINEPERMS_CATALOG_NUM,
                                SYSROUTINEPERMSRowFactory.GRANTEE_ALIAS_GRANTOR_INDEX_NUM,
                                SYSROUTINEPERMSRowFactory.
                                        GRANTEE_COL_NUM_IN_GRANTEE_ALIAS_GRANTOR_INDEX) ||
                        existsRoleGrantByGrantee(authId,tc));
    }


    /**
     * Remove metadata stored prepared statements.
     *
     * @param tc the xact
     */
    private void dropJDBCMetadataSPSes(TransactionController tc) throws StandardException{
        for(Object o : getAllSPSDescriptors()){
            SPSDescriptor spsd=(SPSDescriptor)o;
            SchemaDescriptor sd=spsd.getSchemaDescriptor();

            // don't drop statements in non-system schemas
            if(!sd.isSystemSchema()){
                continue;
            }

//            if (LOG.isTraceEnabled()) LOG.trace(String.format("Dropping metadata SPS: %s.%s", spsd.getSchemaDescriptor().getSchemaName(), spsd.getDescriptorName()));
            dropSPSDescriptor(spsd,tc);
            dropDependentsStoredDependencies(spsd.getUUID(),tc);
        }
    }


    /**
     * Drop and recreate metadata stored prepared statements.
     *
     * @param tc the xact
     * @throws StandardException
     */
    @Override
    public void updateMetadataSPSes(TransactionController tc) throws StandardException{
//        if (LOG.isInfoEnabled()) LOG.info("Dropping metadata stored prepared statements.");
        dropJDBCMetadataSPSes(tc);
//        if (LOG.isInfoEnabled()) LOG.info("Creating metadata stored prepared statements.");
        createSystemSps(tc);
    }

    /**
     * Create a system stored procedure.
     * PLEASE NOTE:
     * This method is currently not used, but will be used when Splice Machine has a SYS_DEBUG schema available
     * with tools to debug and repair databases and data dictionaries.
     *
     * @param schemaName name of the system schema
     * @param procName   name of the system stored procedure
     * @param tc         TransactionController to use
     * @throws StandardException
     */
    @Override
    public void createSystemProcedure(String schemaName,String procName,TransactionController tc) throws StandardException{
        HashSet newlyCreatedRoutines=new HashSet();
        SystemProcedureGenerator procedureGenerator=getSystemProcedures();

        procedureGenerator.createProcedure(schemaName,procName,tc,newlyCreatedRoutines);
        grantPublicAccessToSystemRoutines(newlyCreatedRoutines,tc,authorizationDatabaseOwner);
    }

    /**
     * Drop a system stored procedure.
     * PLEASE NOTE:
     * This method is currently not used, but will be used when Splice Machine has a SYS_DEBUG schema available
     * with tools to debug and repair databases and data dictionaries.
     *
     * @param schemaName name of the system schema
     * @param procName   name of the system stored procedure
     * @param tc         TransactionController to use
     * @throws StandardException
     */
    @Override
    public void dropSystemProcedure(String schemaName,String procName,TransactionController tc) throws StandardException{
        HashSet newlyCreatedRoutines=new HashSet();
        SystemProcedureGenerator procedureGenerator=getSystemProcedures();

        procedureGenerator.dropProcedure(schemaName,procName,tc,newlyCreatedRoutines);
        grantPublicAccessToSystemRoutines(newlyCreatedRoutines,tc,authorizationDatabaseOwner);
    }

    /**
     * Create or update a system stored procedure.  If the system stored procedure alreadys exists in the data dictionary,
     * the stored procedure will be dropped and then created again.
     *
     * @param schemaName the schema where the procedure does and/or will reside
     * @param procName   the procedure to create or update
     * @param tc         the xact
     * @throws StandardException
     */
    @Override
    public void createOrUpdateSystemProcedure(String schemaName,String procName,TransactionController tc) throws StandardException{
        HashSet newlyCreatedRoutines=new HashSet();
        SystemProcedureGenerator procedureGenerator=getSystemProcedures();

        procedureGenerator.createOrUpdateProcedure(schemaName,procName,tc,newlyCreatedRoutines);
        grantPublicAccessToSystemRoutines(newlyCreatedRoutines,tc,authorizationDatabaseOwner);
    }

    /**
     * Create or update all system stored procedures.  If the system stored procedure alreadys exists in the data dictionary,
     * the stored procedure will be dropped and then created again.
     *
     * @param tc the xact
     * @throws StandardException
     */
    @Override
    public void createOrUpdateAllSystemProcedures(TransactionController tc) throws StandardException{
        HashSet newlyCreatedRoutines=new HashSet();
        SystemProcedureGenerator procedureGenerator=getSystemProcedures();

        procedureGenerator.createOrUpdateAllProcedures(SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME,tc,newlyCreatedRoutines);
        procedureGenerator.createOrUpdateAllProcedures(SchemaDescriptor.STD_SYSTEM_UTIL_SCHEMA_NAME,tc,newlyCreatedRoutines);
        procedureGenerator.createOrUpdateAllProcedures(SchemaDescriptor.STD_SQLJ_SCHEMA_NAME,tc,newlyCreatedRoutines);
        procedureGenerator.createOrUpdateAllProcedures(SchemaDescriptor.IBM_SYSTEM_FUN_SCHEMA_NAME,tc,newlyCreatedRoutines);
        grantPublicAccessToSystemRoutines(newlyCreatedRoutines,tc,authorizationDatabaseOwner);
    }

    /**
     * Drops a sequence descriptor
     *
     * @param descriptor The descriptor to drop
     * @param tc         The TransactionController.
     * @throws StandardException Thrown on failure
     */
    @Override
    public void dropSequenceDescriptor(SequenceDescriptor descriptor,TransactionController tc) throws StandardException{
        DataValueDescriptor sequenceIdOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSSEQUENCES_CATALOG_NUM);

        sequenceIdOrderable=getIDValueAsCHAR(descriptor.getUUID());

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,sequenceIdOrderable);

        ti.deleteRow(tc,keyRow,SYSSEQUENCESRowFactory.SYSSEQUENCES_INDEX1_ID);
        ti.deleteRow(tc, keyRow, SYSSEQUENCESRowFactory.SYSSEQUENCES_INDEX1_ID);

        dropSequenceID(descriptor);
    }

    @Override
    public SequenceDescriptor getSequenceDescriptor(UUID uuid) throws StandardException{
        DataValueDescriptor UUIDStringOrderable;

        TabInfoImpl ti=getNonCoreTI(SYSSEQUENCES_CATALOG_NUM);

        /* Use UUIDStringOrderable in both start and stop position for
           * scan.
           */
        UUIDStringOrderable=getIDValueAsCHAR(uuid);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,UUIDStringOrderable);

        SequenceDescriptor sequenceDescriptor=(SequenceDescriptor)
                getDescriptorViaIndex(
                        SYSSEQUENCESRowFactory.SYSSEQUENCES_INDEX1_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false);

        putSequenceID(sequenceDescriptor);

        return sequenceDescriptor;
    }

    /**
     * Get the sequence descriptor given a sequence name and a schema Id.
     *
     * @param sequenceName The sequence name, guaranteed to be unique only within its schema.
     * @param sd           The schema descriptor.
     * @return The SequenceDescriptor for the constraints.
     * @throws StandardException Thrown on failure
     */
    @Override
    public SequenceDescriptor getSequenceDescriptor(SchemaDescriptor sd,String sequenceName) throws StandardException{
        DataValueDescriptor schemaIDOrderable;
        DataValueDescriptor sequenceNameOrderable;
        TabInfoImpl ti=getNonCoreTI(SYSSEQUENCES_CATALOG_NUM);

        /* Use sequenceNameOrderable and schemaIdOrderable in both start
           * and stop position for scan.
           */
        sequenceNameOrderable=new SQLVarchar(sequenceName);
        schemaIDOrderable=getIDValueAsCHAR(sd.getUUID());

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1,schemaIDOrderable);
        keyRow.setColumn(2,sequenceNameOrderable);

        SequenceDescriptor sequenceDescriptor=(SequenceDescriptor)
                getDescriptorViaIndex(
                        SYSSEQUENCESRowFactory.SYSSEQUENCES_INDEX2_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false);

        putSequenceID(sequenceDescriptor);

        return sequenceDescriptor;
    }

    /**
     * Map ( schemaName, sequenceName ) to sequenceID
     */
    protected void putSequenceID(SequenceDescriptor sd) throws StandardException{
        if(sd==null){
            return;
        }

        SchemaDescriptor schema=sd.getSchemaDescriptor();
        String schemaName=schema.getSchemaName();
        String sequenceName=sd.getSequenceName();
        String uuid=sd.getUUID().toString();

        Map<String, String> sequencesInSchema=sequenceIDs.get(schemaName);
        if(sequencesInSchema==null){
            sequencesInSchema=new HashMap<>();
            sequenceIDs.put(schemaName,sequencesInSchema);
        }

        if(sequencesInSchema.get(sequenceName)==null){
            sequencesInSchema.put(sequenceName,uuid);
        }
    }

    /**
     * Drop a sequenceID from the ( schemaName, sequenceName ) map
     */
    protected void dropSequenceID(SequenceDescriptor sd) throws StandardException{
        if(sd==null){
            return;
        }

        SchemaDescriptor schema=sd.getSchemaDescriptor();
        String schemaName=schema.getSchemaName();
        String sequenceName=sd.getSequenceName();

        HashMap sequencesInSchema=(HashMap)sequenceIDs.get(schemaName);
        if(sequencesInSchema==null){
            return;
        }

        if(sequencesInSchema.get(sequenceName)==null){
            return;
        }
        {
            sequencesInSchema.remove(sequenceName);
        }
    }

    /**
     * <p>
     * Get the uuid string of a sequence given its schema and sequence name.
     * </p>
     */
    protected String getSequenceID(String schemaName,String sequenceName) throws StandardException{
        Map<String, String> sequencesInSchema=sequenceIDs.get(schemaName);
        if(sequencesInSchema!=null){
            String uuid=sequencesInSchema.get(sequenceName);

            if(uuid!=null){
                return uuid;
            }
        }

        // oops, not saved in the sequenceID map yet. lookup the sequence.
        // this will save the uuid in the sequenceID map.
        SchemaDescriptor schemaDescriptor=getSchemaDescriptor(schemaName,getTransactionCompile(),true);
        SequenceDescriptor desc=getSequenceDescriptor(schemaDescriptor,sequenceName);

        if(desc==null){
            return null;
        }else{
            return desc.getUUID().toString();
        }
    }

    /**
     * Get an object's permission descriptor from the system tables, without going through the cache.
     * This method is called to fill the permissions cache.
     *
     * @return a PermDescriptor that describes the table permissions granted to the grantee on an objcet
     * , null if no table-level permissions have been granted to him on the table.
     * @throws StandardException
     */
    protected PermDescriptor getUncachedGenericPermDescriptor(PermDescriptor key) throws StandardException{
        int indexNum=SYSPERMSRowFactory.GRANTEE_OBJECTID_GRANTOR_INDEX_NUM;
        if(key.getObjectID()!=null)
            indexNum=SYSPERMSRowFactory.PERMS_UUID_IDX_NUM;

        Object o=getUncachedPermissionsDescriptor(SYSPERMS_CATALOG_NUM,indexNum,key);
        return (PermDescriptor)o;
    } // end of getUncachedGenericPermDescriptor

    /**
     * Get permissions granted to one user for an object using the object's Id
     * and the user's authorization Id.
     *
     * @param objectUUID    The id of the protected object
     * @param objectType    Type of the object (e.g., SEQUENCE)
     * @param privilege     The kind of privilege needed (e.g., PermDescriptor.USAGE_PRIV)
     * @param granteeAuthId The user or role who wants to have permission on this object
     * @return The descriptor of the permissions for the object
     * @throws StandardException
     */
    @Override
    public PermDescriptor getGenericPermissions(UUID objectUUID,
                                                String objectType,
                                                String privilege,
                                                String granteeAuthId) throws StandardException{
        PermDescriptor key=new PermDescriptor(this,null,objectType,objectUUID,privilege,null,granteeAuthId,false);

        return (PermDescriptor)getPermissions(key);
    }

    /**
     * Get one user's privileges for an object using the permUUID.
     *
     * @param permUUID the uuid of the permission
     * @return The descriptor of the user's permissions for the object.
     * @throws StandardException
     */
    @Override
    public PermDescriptor getGenericPermissions(UUID permUUID) throws StandardException{
        PermDescriptor key=new PermDescriptor(this,permUUID);
        return getUncachedGenericPermDescriptor(key);
    }

    @Override
    public DependableFinder getDependableFinder(int formatId){
        return new DDdependableFinder(formatId);
    }

    @Override
    public DependableFinder getColumnDependableFinder(int formatId,byte[] columnBitMap){
        return new DDColumnDependableFinder(formatId,columnBitMap);
    }

    protected class CoreCreation implements Runnable{
        private int coreCtr;
        private TransactionController tc;
        private ExecutionContext ec;

        public CoreCreation(int coreCtr,TransactionController tc,ExecutionContext ec){
            this.coreCtr=coreCtr;
            this.tc=tc;
            this.ec=ec;
        }

        @Override
        public void run(){
            try{
                ContextManager cm=ContextService.getFactory().newContextManager();
                cm.pushContext(ec);
                ContextService.getFactory().setCurrentContextManager(cm);
                TabInfoImpl ti=coreInfo[coreCtr];
                String version = catalogVersions.get(coreCtr);
                Properties heapProperties=new Properties();
                heapProperties.setProperty("tableDisplayName", ti.getTableName());
                heapProperties.setProperty("catalogVersion", version);
                ExecRow rowTemplate=ti.getCatalogRowFactory().makeEmptyRowForLatestVersion();
                long conglomerate=createConglomerate(tc,rowTemplate,heapProperties);
                ti.setHeapConglomerate(conglomerate);
            }catch(Exception e){
                e.printStackTrace();
                StringBuilder strbuf = new StringBuilder("Dictionary Table Failure - exiting ");
                strbuf.append(coreCtr);
                SanityManager.THROWASSERT(strbuf.toString());
            }
        }
    }


    public DataDictionaryCache getDataDictionaryCache() {
        return dataDictionaryCache;
    }
    @Override
    public void clearCaches()
    {
        dataDictionaryCache.clearAll();
    }

    @Override
    public void addBackup(TupleDescriptor descriptor, TransactionController tc) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSBACKUP_CATALOG_NUM);
        ExecRow row = ti.getCatalogRowFactory().makeRow(descriptor, null);
        int insertRetCode = ti.insertRow(row,tc);
        if(insertRetCode!=TabInfoImpl.ROWNOTDUPLICATE) {
            throw duplicateDescriptorException(descriptor, null);
        }
    }

    @Override
    public void deleteBackup(long backupId, TransactionController tc) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSBACKUP_CATALOG_NUM);
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1, new SQLLongint(backupId));
        ti.deleteRow(tc, keyRow, SYSBACKUPRowFactory.SYSBACKUP_INDEX1_ID);
    }

    @Override
    public List<BackupDescriptor> getBackupDescriptorList() throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSBACKUP_CATALOG_NUM);
        SYSBACKUPRowFactory rf=(SYSBACKUPRowFactory)ti.getCatalogRowFactory();
        ExecRow outRow;
        ScanController scanController;
        TransactionController tc;
        List<BackupDescriptor> backupDescriptorList = Lists.newArrayList();

        // Get the current transaction controller
        tc=getTransactionCompile();

        outRow=rf.makeEmptyRow();
        /*
        ** Table scan
        */
        scanController=tc.openScan(
                ti.getHeapConglomerate(),      // conglomerate to open
                false,                          // don't hold open across commit
                0,                              // for read
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,               // all fields as objects
                null, // start position - first row
                0,                          // startSearchOperation - none
                null,              // scanQualifier,
                null, // stop position -through last row
                0);                          // stopSearchOperation - none

        try{
            while(scanController.fetchNext(outRow.getRowArray())){
                BackupDescriptor bd = (BackupDescriptor)rf.buildDescriptor(outRow, null, this);
                backupDescriptorList.add(bd);
            }
        }finally{
            scanController.close();
        }

        return backupDescriptorList;
    }

    @Override
    public List<BackupItemsDescriptor> getBackupItemDescriptorList() throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSBACKUPITEMS_CATALOG_NUM);
        SYSBACKUPITEMSRowFactory rf=(SYSBACKUPITEMSRowFactory)ti.getCatalogRowFactory();
        ExecRow outRow;
        ScanController scanController;
        TransactionController tc;
        List<BackupItemsDescriptor> backupItemDescriptorList = Lists.newArrayList();

        // Get the current transaction controller
        tc=getTransactionCompile();

        outRow=rf.makeEmptyRow();
        /*
        ** Table scan
        */
        scanController=tc.openScan(
                ti.getHeapConglomerate(),      // conglomerate to open
                false,                          // don't hold open across commit
                0,                              // for read
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,               // all fields as objects
                null, // start position - first row
                0,                          // startSearchOperation - none
                null,              // scanQualifier,
                null, // stop position -through last row
                0);                          // stopSearchOperation - none

        try{
            while(scanController.fetchNext(outRow.getRowArray())){
                BackupItemsDescriptor bd = (BackupItemsDescriptor) rf.buildDescriptor(outRow, null, this);
                backupItemDescriptorList.add(bd);
            }
        }finally{
            scanController.close();
        }

        return backupItemDescriptorList;
    }

    @Override
    public void addBackupItems(TupleDescriptor[] descriptor, TransactionController tc) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSBACKUPITEMS_CATALOG_NUM);
        ExecRow[] rows = new ValueRow[descriptor.length];
        for (int i = 0; i < descriptor.length; ++i) {
            rows[i] = ti.getCatalogRowFactory().makeRow(descriptor[i], null);
        }

        int insertRetCode = ti.insertRowList(rows,tc);
        if (insertRetCode != TabInfoImpl.ROWNOTDUPLICATE)
            throw StandardException.newException(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,
                    "SYSBACKUPITEMS_INDEX2", SYSBACKUPITEMSRowFactory.TABLENAME_STRING);

    }

    @Override
    public void deleteAllBackupItems(long backupId, TransactionController tc) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSBACKUPITEMS_CATALOG_NUM);
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1, new SQLLongint(backupId));
        ti.deleteRow(tc, keyRow, SYSBACKUPITEMSRowFactory.SYSBACKUPITEMS_INDEX1_ID);
    }

    @Override
    public void addSnapshot(TupleDescriptor descriptor, TransactionController tc) throws StandardException
    {
        TabInfoImpl ti=getNonCoreTI(SYSSNAPSHOT_NUM);
        ExecRow row = ti.getCatalogRowFactory().makeRow(descriptor, null);
        int insertRetCode = ti.insertRow(row,tc);
        if(insertRetCode!=TabInfoImpl.ROWNOTDUPLICATE) {
            throw duplicateDescriptorException(descriptor, null);
        }
    }

    @Override
    public void deleteSnapshot(String snapshotName, long conglomeratenumber, TransactionController tc) throws StandardException
    {
        TabInfoImpl ti=getNonCoreTI(SYSSNAPSHOT_NUM);
        ExecIndexRow keyRow = exFactory.getIndexableRow(2);
        keyRow.setColumn(1, new SQLVarchar(snapshotName));
        keyRow.setColumn(2, new SQLLongint(conglomeratenumber));
        ti.deleteRow(tc, keyRow, SYSSNAPSHOTSRowFactory.SYSSNAPSHOTS_INDEX1_ID);
    }

    @Override
    public void saveSourceCode(SourceCodeDescriptor descriptor, TransactionController tc) throws StandardException {
        TabInfoImpl ti = getNonCoreTI(SYSSOURCECODE_CATALOG_NUM);

        ExecIndexRow keyRow;
        keyRow = exFactory.getIndexableRow(4);
        keyRow.setColumn(1, new SQLVarchar(descriptor.getSchemaName()));
        keyRow.setColumn(2, new SQLVarchar(descriptor.getObjectName()));
        keyRow.setColumn(3, new SQLVarchar(descriptor.getObjectType()));
        keyRow.setColumn(4, new SQLVarchar(descriptor.getObjectForm()));

        if (descriptor.getSourceCode() == null) { // drop sourcecode object
            ti.deleteRow(tc, keyRow, SYSSOURCECODERowFactory.SYSSOURCECODE_INDEX1_ID);
            return;
        }

        ExecRow row = ti.getCatalogRowFactory().makeRow(descriptor, null);

        ExecRow existingRow = ti.getRow(tc, keyRow, SYSSOURCECODERowFactory.SYSSOURCECODE_INDEX1_ID);
        if (existingRow == null) {
            int insertRetCode = ti.insertRow(row, tc);
            if (insertRetCode != TabInfoImpl.ROWNOTDUPLICATE)
                throw duplicateDescriptorException(descriptor, null);
        } else {
            boolean[] bArray = {false};
            int[] colsToUpdate = {
                    SYSSOURCECODERowFactory.DEFINER_NAME,
                    SYSSOURCECODERowFactory.LAST_MODIFIED,
                    SYSSOURCECODERowFactory.SOURCE_CODE,
            };
            ti.updateRow(keyRow, row, SYSSOURCECODERowFactory.SYSSOURCECODE_INDEX1_ID, bArray, colsToUpdate, tc);
        }
    }

    public List<String> getDefaultRoles(String username, TransactionController tc) throws StandardException {
        // check dictionary cache first
        List<String> roleList = dataDictionaryCache.defaultRoleCacheFind(username);
        if (roleList != null)
            return roleList;

        List<RoleGrantDescriptor> roleGrantDescriptors = new ArrayList<>();

        TabInfoImpl ti=getNonCoreTI(SYSROLES_CATALOG_NUM);
        /* set up the start/stop position for the scan */
        ExecIndexRow keyRow = exFactory.getIndexableRow(1);
        DataValueDescriptor granteeOrderable=new SQLVarchar(username);
        keyRow.setColumn(1, granteeOrderable);
        getDescriptorViaIndex(
                SYSROLESRowFactory.SYSROLES_INDEX_EE_DEFAULT_IDX,
                keyRow,
                null,
                ti,
                null,
                roleGrantDescriptors,
                false);

        roleList = FluentIterable.from(roleGrantDescriptors).filter(rgd -> rgd.isDefaultRole()).transform(rgd -> rgd.getRoleName()).toList();
        dataDictionaryCache.defaultRoleCacheAdd(username, roleList);
        return roleList;
    }

    public void addToken(TokenDescriptor descriptor, TransactionController tc) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSTOKENS_NUM);
        ExecRow row = ti.getCatalogRowFactory().makeRow(descriptor, null);
        int insertRetCode = ti.insertRow(row,tc);
        if(insertRetCode != TabInfoImpl.ROWNOTDUPLICATE) {
            throw duplicateDescriptorException(descriptor, null);
        }
        dataDictionaryCache.tokenCacheAdd(descriptor.getToken(), descriptor);
    }

    @Override
    public void deleteToken(byte[] token) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSTOKENS_NUM);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1, new SQLBit(token));

        ti.deleteRow(getTransactionCompile(), keyRow, SYSTOKENSRowFactory.SYSTOKENS_INDEX1_ID);
        dataDictionaryCache.tokenCacheRemove(token);
    }

    @Override
    public TokenDescriptor getToken(byte[] token) throws StandardException {
        TokenDescriptor result = dataDictionaryCache.tokenCacheFind(token);
        if (result != null) {
            return result;
        }

        TabInfoImpl ti=getNonCoreTI(SYSTOKENS_NUM);

        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1, new SQLBit(token));

        result = (TokenDescriptor)
                getDescriptorViaIndex(
                        SYSTOKENSRowFactory.SYSTOKENS_INDEX1_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false,
                        TransactionController.ISOLATION_REPEATABLE_READ,
                        getTransactionCompile());
        if (result != null)
            dataDictionaryCache.tokenCacheAdd(token, result);
        return result;
    }

    @Override
    public void addReplication(TupleDescriptor descriptor, TransactionController tc) throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSREPLICATION_CATALOG_NUM);
        ExecRow row = ti.getCatalogRowFactory().makeRow(descriptor, null);
        int insertRetCode = ti.insertRow(row,tc);
        if(insertRetCode != TabInfoImpl.ROWNOTDUPLICATE) {
            throw duplicateDescriptorException(descriptor, null);
        }
    }

    @Override
    public void deleteReplication(TupleDescriptor descriptor, TransactionController tc) throws StandardException {
        assert descriptor instanceof ReplicationDescriptor;
        ReplicationDescriptor d = (ReplicationDescriptor) descriptor;
        String scope = d.getScope();
        String schemaName = d.getSchemaName();
        String tableName = d.getTableName();

        TabInfoImpl ti=getNonCoreTI(SYSREPLICATION_CATALOG_NUM);
        if (scope!= null && schemaName == null && tableName == null) {
            ExecIndexRow keyRow = exFactory.getIndexableRow(1);
            keyRow.setColumn(1, new SQLVarchar(scope));
            ti.deleteRow(tc, keyRow, SYSREPLICATIONRowFactory.SYSREPLICATION_INDEX1_ID);
        }
        else if (scope!= null && schemaName != null && tableName == null) {
            ExecIndexRow keyRow = exFactory.getIndexableRow(2);
            keyRow.setColumn(1, new SQLVarchar(scope));
            keyRow.setColumn(2, new SQLVarchar(schemaName));
            ti.deleteRow(tc, keyRow, SYSREPLICATIONRowFactory.SYSREPLICATION_INDEX2_ID);
        }
        else if (scope!= null && schemaName != null && tableName != null) {
            ExecIndexRow keyRow = exFactory.getIndexableRow(3);
            keyRow.setColumn(1, new SQLVarchar(scope));
            keyRow.setColumn(2, new SQLVarchar(schemaName));
            keyRow.setColumn(3, new SQLVarchar(tableName));
            ti.deleteRow(tc, keyRow, SYSREPLICATIONRowFactory.SYSREPLICATION_INDEX3_ID);
        }
    }

    @Override
    public boolean databaseReplicationEnabled() throws StandardException {
        TabInfoImpl ti=getNonCoreTI(SYSREPLICATION_CATALOG_NUM);
        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1, new SQLVarchar(SYSREPLICATIONRowFactory.SCOPE_DATABASE));

        ReplicationDescriptor result = (ReplicationDescriptor)
                getDescriptorViaIndex(
                        SYSREPLICATIONRowFactory.SYSREPLICATION_INDEX1_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false,
                        TransactionController.ISOLATION_REPEATABLE_READ,
                        getTransactionCompile());
        return result != null;
    }

    @Override
    public boolean schemaReplicationEnabled(String schemaName) throws StandardException {

        TabInfoImpl ti=getNonCoreTI(SYSREPLICATION_CATALOG_NUM);
        ExecIndexRow keyRow=exFactory.getIndexableRow(2);
        keyRow.setColumn(1, new SQLVarchar(SYSREPLICATIONRowFactory.SCOPE_SCHEMA));
        keyRow.setColumn(2, new SQLVarchar(schemaName));

        ReplicationDescriptor result = (ReplicationDescriptor)
                getDescriptorViaIndex(
                        SYSREPLICATIONRowFactory.SYSREPLICATION_INDEX2_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false,
                        TransactionController.ISOLATION_REPEATABLE_READ,
                        getTransactionCompile());

        return result != null;
    }

    @Override
    public boolean tableReplicationEnabled(String schemaName,
                                      String tableName) throws StandardException {

        TabInfoImpl ti=getNonCoreTI(SYSREPLICATION_CATALOG_NUM);
        /* Set up the start/stop position for the scan */
        ExecIndexRow keyRow=exFactory.getIndexableRow(3);
        keyRow.setColumn(1, new SQLVarchar(SYSREPLICATIONRowFactory.SCOPE_TABLE));
        keyRow.setColumn(2, new SQLVarchar(schemaName));
        keyRow.setColumn(3, new SQLVarchar(tableName));

        ReplicationDescriptor result = (ReplicationDescriptor)
                getDescriptorViaIndex(
                        SYSREPLICATIONRowFactory.SYSREPLICATION_INDEX3_ID,
                        keyRow,
                        null,
                        ti,
                        null,
                        null,
                        false,
                        TransactionController.ISOLATION_REPEATABLE_READ,
                        getTransactionCompile());
        return result != null;
    }

    @Override
    public boolean useTxnAwareCache() {
        return false;
    }

}
