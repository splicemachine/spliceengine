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

package com.splicemachine.db.impl.db;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.db.InternalDatabase;
import com.splicemachine.db.iapi.db.DatabaseContext;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.AuthenticationService;
import com.splicemachine.db.iapi.reference.*;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.JarReader;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyFactory;
import com.splicemachine.db.iapi.services.property.PropertySetCallback;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.LanguageFactory;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionFactory;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.FileInfoDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.util.DoubleProperties;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.impl.sql.execute.JarUtil;
import com.splicemachine.db.io.StorageFile;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.InputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.Dictionary;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * The Database interface provides control over the physical database
 * (that is, the stored data and the files the data are stored in),
 * connections to the database, operations on the database such as
 * backup and recovery, and all other things that are associated
 * with the database itself.
 * <p>
 * The Database interface does not provide control over things that are part of
 * the Domain, such as users.
 * <p>
 * I'm not sure what this will hold in a real system, for now
 * it simply provides connection-creation for us.  Perhaps when it boots,
 * it creates the datadictionary object for the database, which all users
 * will then interact with?
 *
 */

public class BasicDatabase implements ModuleControl, ModuleSupportable, PropertySetCallback, InternalDatabase, JarReader
{
    protected boolean    active;
    private AuthenticationService authenticationService;
    protected AccessFactory af;
    protected PropertyFactory pf;
    protected ClassFactory cfDB; // classFactory but only set when per-database
    /**
     * DataDictionary for this database.
     */
    protected DataDictionary dd;
    
    protected LanguageConnectionFactory lcf;
    protected LanguageFactory lf;
    // hold resourceAdapter in an Object instead of a ResourceAdapter
    // so that XA class use can be isolated to XA modules.
    protected Object resourceAdapter;
    private Locale databaseLocale;
    private DateFormat dateFormat;
    private DateFormat timeFormat;
    private DateFormat timestampFormat;
    private UUID        myUUID;

    protected boolean lastToBoot; // is this class last to boot

    /*
     * ModuleControl interface
     */

    public boolean canSupport(Properties startParams) {
        return Monitor.isDesiredCreateType(startParams, getEngineType());
    }

    @SuppressFBWarnings(value="DLS_DEAD_LOCAL_STORE")
    public void boot(boolean create, Properties startParams)
        throws StandardException
    {

        ModuleFactory monitor = Monitor.getMonitor();
        if (create)
        {
            if (startParams.getProperty(Property.CREATE_WITH_NO_LOG) == null)
                startParams.put(Property.CREATE_WITH_NO_LOG, "true");

            String localeID =
                startParams.getProperty(
                    Attribute.TERRITORY);

            if (localeID == null) {
                localeID = Locale.getDefault().toString();
            }
            databaseLocale = monitor.setLocale(startParams, localeID);

        } else {
            databaseLocale = monitor.getLocale(this);
        }
        setLocale(databaseLocale);

        // boot the validation needed to do property validation, now property
        // validation is separated from AccessFactory, therefore from store
        bootValidation(create, startParams);

        // boot the type factory before store to ensure any dynamically
        // registered types (DECIMAL) are there before logical undo recovery
        // might need them.
        DataValueFactory dvf = (DataValueFactory)
            Monitor.bootServiceModule(
                create, 
                this,
                ClassName.DataValueFactory,
                startParams);

        bootStore(create, startParams);

        // create a database ID if one doesn't already exist
        myUUID = makeDatabaseID(create, startParams);


        // Add the database properties read from disk (not stored
        // in service.properties) into the set seen by booting modules.
        Properties allParams =
            new DoubleProperties(getAllDatabaseProperties(), startParams);

        if (pf != null)
            pf.addPropertySetNotification(this);

            // Boot the ClassFactory, will be per-database or per-system.
            // reget the tc in case someone inadverdently destroyed it
        bootClassFactory(create, allParams);
        
        dd = (DataDictionary)
            Monitor.bootServiceModule(create, this,
                    DataDictionary.MODULE, allParams);

        lcf = (LanguageConnectionFactory)
            Monitor.bootServiceModule(
                create, this, LanguageConnectionFactory.MODULE, allParams);

        lf = (LanguageFactory)
            Monitor.bootServiceModule(
                create, this, LanguageFactory.MODULE, allParams);

        bootResourceAdapter(create, allParams);


        // may also want to set up a check that we are a singleton,
        // or that there isn't already a database object in the system
        // for the same database?


        //
        // We boot the authentication service. There should at least be one
        // per database (even if authentication is turned off) .
        //
        authenticationService = bootAuthenticationService(create, allParams);
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                authenticationService != null,
                "Failed to set the Authentication service for the database");
        }

        // Lastly, let store knows that database creation is done and turn
        // on logging
        if (create && lastToBoot &&
            (startParams.getProperty(Property.CREATE_WITH_NO_LOG) != null))
        {
            createFinished();
        }

        active = true;

    }

    public void stop() {
        // The data dictionary is not available if this database has the
        // role as an active replication replica database.
        if (dd != null) {
            try {
                // on orderly shutdown, try not to leak unused numbers from
                // the sequence generators.
                dd.clearSequenceCaches();
            } catch (StandardException se) {
                Monitor.getStream().printThrowable(se);
            }
        }
        active = false;
    }

    /*
    ** Methods related to  ModuleControl
    */

    /*
     * Database interface
      */

    /**
     * Return the engine type that this Database implementation
     * supports.
     * This implementation supports the standard database.
      */
    public int getEngineType() {
        return EngineType.STANDALONE_DB;
    }

    public boolean isReadOnly()
    {
        //
        //Notice if no full users?
        //RESOLVE: (Make access factory check?)
        return af.isReadOnly();
    }

    @Override
    public LanguageConnectionContext setupConnection(ContextManager cm, String user, List<String> groupuserlist, String drdaID, String dbname,
                                                     String rdbIntTkn,
                                                     DataSetProcessorType type,
                                                     boolean skipStats,
                                                     double defaultSelectivityFactor,
                                                     String ipAddress,
                                                     String defaultSchema,
                                                     Properties sessionProperties)
        throws StandardException {

        String snapshot = sessionProperties.getProperty(Property.CONNECTION_SNAPSHOT);
        TransactionController tc = null;
        if (snapshot != null) {
            long id = Long.parseLong(snapshot);
            tc = af.getReadOnlyTransaction(cm, id);
        } else {
            tc = getConnectionTransaction(cm);
        }

        TransactionManager tm = (TransactionManager) tc;
        Transaction transaction = tm.getRawStoreXact();
        boolean isRestoreMode = transaction.isRestoreMode();
        if (isRestoreMode) {
            throw StandardException.newException(SQLState.CANNOT_CONNECT_DURING_RESTORE);
        }

        cm.setLocaleFinder(this);
        pushDbContext(cm);

        // push a database shutdown context
        // we also need to push a language connection context.
        LanguageConnectionContext lctx = lcf.newLanguageConnectionContext(cm, tc, lf, this, user, groupuserlist, drdaID, dbname,
                rdbIntTkn, type,skipStats,defaultSelectivityFactor, ipAddress, defaultSchema, sessionProperties);

        // push the context that defines our class factory
        pushClassFactoryContext(cm, lcf.getClassFactory());

        // we also need to push an execution context.
        ExecutionFactory ef = lcf.getExecutionFactory();

        ef.newExecutionContext(cm);
        //
        //Initialize our language connection context. Note: This is
        //a bit of a hack. Unfortunately, we can't initialize this
        //when we push it. We first must push a few more contexts.
        lctx.initialize();

        // Need to commit this to release locks gotten in initialize.
        // Commit it but make sure transaction not have any updates.
        lctx.internalCommitNoSync(
            TransactionController.RELEASE_LOCKS |
            TransactionController.READONLY_TRANSACTION_INITIALIZATION);

        return lctx;

    }
    
    /**
     * Return the DataDictionary for this database, set up at boot time.
     */
    public final DataDictionary getDataDictionary()
    {
        return dd;
    }

    @SuppressFBWarnings(value="DLS_DEAD_LOCAL_STORE")
    public void pushDbContext(ContextManager cm)
    {
        /* We cache the locale in the DatabaseContext
         * so that the Datatypes can get to it easily.
         */
        DatabaseContext dc = new DatabaseContextImpl(cm, this);
    }

    public AuthenticationService getAuthenticationService()
        throws StandardException{

        // Expected to find one - Sanity check being done at
        // DB boot-up.

        // We should have a Authentication Service
        //
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(this.authenticationService != null,
                "Unexpected - There is no valid authentication service for the database!");
        }
        return this.authenticationService;
    }

    public boolean isInReplicaMode() {
        return false;
    }

    public void freeze() throws SQLException
    {
        try {
            af.freeze();
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }

    public void unfreeze() throws SQLException
    {
        try {
            af.unfreeze();
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }


    public void backup(String backupDir, boolean wait) 
        throws SQLException
    {
        try {
            af.backup(backupDir, wait);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }

    public void restore(String restoreDir, boolean wait) throws SQLException {
            throw new RuntimeException("UnsupportedException");
        }


    public void    checkpoint() throws SQLException
    {
        try {
            af.checkpoint();
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /* Methods from com.splicemachine.db.database.Database */
    public Locale getLocale() {
        return databaseLocale;
    }


    /**
        Return the UUID of this database.
        @deprecated
    */
    public final UUID getId() {
        return myUUID;
    }

    /* LocaleFinder methods */

    /** @exception StandardException    Thrown on error */
    public Locale getCurrentLocale() throws StandardException {
        if (databaseLocale != null)
            return databaseLocale;
        throw noLocale();
    }

    /** @exception StandardException    Thrown on error */
    public DateFormat getDateFormat() throws StandardException {
        if (databaseLocale != null) {
            if (dateFormat == null) {
                dateFormat = DateFormat.getDateInstance(DateFormat.LONG,
                                                                databaseLocale);
            }

            return dateFormat;
        }

        throw noLocale();
    }

    /** @exception StandardException    Thrown on error */
    public DateFormat getTimeFormat() throws StandardException {
        if (databaseLocale != null) {
            if (timeFormat == null) {
                timeFormat = DateFormat.getTimeInstance(DateFormat.LONG,
                                                                databaseLocale);
            }

            return timeFormat;
        }

        throw noLocale();
    }

    /** @exception StandardException    Thrown on error */
    public DateFormat getTimestampFormat() throws StandardException {
        if (databaseLocale != null) {
            if (timestampFormat == null) {
                timestampFormat = DateFormat.getDateTimeInstance(
                                                            DateFormat.LONG,
                                                            DateFormat.LONG,
                                                            databaseLocale);
            }

            return timestampFormat;
        }

        throw noLocale();
    }

    private static StandardException noLocale() {
        return StandardException.newException(SQLState.NO_LOCALE);
    }

    public void setLocale(Locale locale) {
        databaseLocale = locale;

        dateFormat = null;
        timeFormat = null;
        timestampFormat = null;
    }

    /**
        Is the database active (open).
    */
    public boolean isActive() {
        return active;
    }

    /*
     * class interface
     */
    public BasicDatabase() {
        lastToBoot = true;
    }


    protected    UUID    makeDatabaseID(boolean create, Properties startParams)
        throws StandardException
    {

        TransactionController tc = af.getTransaction(
                ContextService.getFactory().getCurrentContextManager());

        String  upgradeID = null;
        UUID    databaseID;

        if ((databaseID = (UUID) tc.getProperty(DataDictionary.DATABASE_ID)) == null) {

            // no property defined in the Transaction set
            // this could be an upgrade, see if it's stored in the service set

            UUIDFactory    uuidFactory  = Monitor.getMonitor().getUUIDFactory();


            upgradeID = startParams.getProperty(DataDictionary.DATABASE_ID);
            if (upgradeID == null )
            {
                // just create one
                databaseID = uuidFactory.createUUID();
            } else {
                databaseID = uuidFactory.recreateUUID(upgradeID);
            }

            tc.setProperty(DataDictionary.DATABASE_ID, databaseID, true);
        }

        // Remove the database identifier from the service.properties
        // file only if we upgraded it to be stored in the transactional
        // property set.
        if (upgradeID != null)
            startParams.remove(DataDictionary.DATABASE_ID);

        tc.commit();
        tc.destroy();

        return databaseID;
    }

    /*
    ** Return an Object instead of a ResourceAdapter
    ** so that XA classes are only used where needed;
    ** caller must cast to ResourceAdapter.
    */
    public Object getResourceAdapter()
    {
        return resourceAdapter;
    }

    /*
    ** Methods of PropertySetCallback
    */
    public void init(boolean dbOnly, Dictionary p) {
        // not called yet ...
    }

    /**
      @see PropertySetCallback#validate
      @exception StandardException Thrown on error.
    */
    public boolean validate(String key,
                         Serializable value,
                         Dictionary p)
        throws StandardException
    {
        //
        //Disallow setting static creation time only configuration properties
        if (key.equals(EngineType.PROPERTY))
            throw StandardException.newException(SQLState.PROPERTY_UNSUPPORTED_CHANGE, key, value);

        // only interested in the classpath
        if (!key.equals(Property.DATABASE_CLASSPATH)) return false;

        String newClasspath = (String) value;
        String[][] dbcp = null; //The parsed dbclasspath

        if (newClasspath != null) {
            // parse it when it is set to ensure only valid values
            // are written to the actual conglomerate.
            dbcp = IdUtil.parseDbClassPath(newClasspath);
        }

        //
        //Verify that all jar files on the database classpath are in the data dictionary.
        if (dbcp != null)
        {
            for (String[] aDbcp : dbcp) {
                SchemaDescriptor sd = dd.getSchemaDescriptor(aDbcp[IdUtil.DBCP_SCHEMA_NAME], null, false);

                FileInfoDescriptor fid = null;
                if (sd != null)
                    fid = dd.getFileInfoDescriptor(sd, aDbcp[IdUtil.DBCP_SQL_JAR_NAME]);

                if (fid == null) {
                    throw StandardException.newException(SQLState.LANG_DB_CLASS_PATH_HAS_MISSING_JAR, IdUtil.mkQualifiedName(aDbcp));
                }
            }
        }

        return true;
    }
    /**
      @see PropertySetCallback#apply
      @exception StandardException Thrown on error.
    */
    @Override
    public Serviceable apply(String key, Serializable value, Dictionary p,TransactionController tc)
        throws StandardException
    {
        // only interested in the classpath
        if (!key.equals(Property.DATABASE_CLASSPATH)) return null;

        // only do the change dynamically if we are already
        // a per-database classapath.
        if (cfDB != null) {

            //
            // Invalidate stored plans.
            getDataDictionary().invalidateAllSPSPlans();

            String newClasspath = (String) value;
            if (newClasspath == null) newClasspath = "";
            cfDB.notifyModifyClasspath(newClasspath);
        }
        return null;
    }
    /**
      @see PropertySetCallback#map
    */
    public Serializable map(String key,Serializable value,Dictionary p)
    {
        return null;
    }

    /*
     * methods specific to this class
     */
    protected void createFinished() throws StandardException
    {
        // find the access factory and tell it that database creation has
        // finished
        af.createFinished();
    }

    protected String getClasspath(Properties startParams) {
        String cp = PropertyUtil.getPropertyFromSet(startParams, Property.DATABASE_CLASSPATH);
        if (cp == null)
            cp = PropertyUtil.getSystemProperty(Property.DATABASE_CLASSPATH, "");
        return cp;
    }


    protected void bootClassFactory(boolean create,
                                  Properties startParams)
         throws StandardException
    {
            String classpath = getClasspath(startParams);

            // parse the class path and allow 2 part names.
            IdUtil.parseDbClassPath(classpath);

            startParams.put(Property.BOOT_DB_CLASSPATH, classpath);
            cfDB = (ClassFactory) Monitor.bootServiceModule(create, this,
                    Module.ClassFactory, startParams);
    }


    /*
    ** Methods to allow sub-classes to offer alternate implementations.
    */

    protected TransactionController getConnectionTransaction(ContextManager cm)
        throws StandardException {

        // start a local transaction
        return af.getTransaction(cm);
    }

    protected AuthenticationService bootAuthenticationService(boolean create, Properties props) throws StandardException {
        return (AuthenticationService)
                Monitor.bootServiceModule(create, this, AuthenticationService.MODULE, props);
    }

    protected void bootValidation(boolean create, Properties startParams)
        throws StandardException {
        pf = (PropertyFactory) Monitor.bootServiceModule(create, this,
            Module.PropertyFactory, startParams);
    }

    protected void bootStore(boolean create, Properties startParams)
        throws StandardException {
        af = (AccessFactory) Monitor.bootServiceModule(create, this, AccessFactory.MODULE, startParams);
    }

    /**
     * Get the set of database properties from the set stored
     * on disk outside of service.properties.
     */
    protected Properties getAllDatabaseProperties()
        throws StandardException {

        TransactionController tc = af.getTransaction(
                    ContextService.getFactory().getCurrentContextManager());
        Properties dbProps = tc.getProperties();
        tc.commit();
        tc.destroy();

        return dbProps;
    }

    protected void bootResourceAdapter(boolean create, Properties allParams) {

        // Boot resource adapter - only if we are running Java 2 or
        // beyondwith JDBC20 extension, JTA and JNDI classes in the classpath
        //
        // assume if it doesn't boot it was because the required
        // classes were missing, and continue without it.
        // Done this way to work around Chai's need to preload
        // classes.
        // Assume both of these classes are in the class path.
        // Assume we may need a ResourceAdapter since we don't know how
        // this database is going to be used.
        try
        {
            resourceAdapter =
                Monitor.bootServiceModule(create, this,
                                         Module.ResourceAdapter,
                                         allParams);
        }
        catch (StandardException mse)
        {
            // OK, resourceAdapter is an optional module
        }
    }

    protected void pushClassFactoryContext(ContextManager cm, ClassFactory cf) {
        new StoreClassFactoryContext(cm, cf, af, this);
    }

    /*
    ** Methods of JarReader
    */
    public StorageFile getJarFile(String schemaName, String sqlName)
        throws StandardException {

        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null, true);
        FileInfoDescriptor fid = dd.getFileInfoDescriptor(sd,sqlName);
        if (fid == null)
            throw StandardException.newException(SQLState.LANG_JAR_FILE_DOES_NOT_EXIST, sqlName, schemaName);

        long generationId = fid.getGenerationId();

        ContextManager cm = ContextService.getFactory().getCurrentContextManager();
        FileResource fr = af.getTransaction(cm).getFileHandler();

        String externalName = JarUtil.mkExternalName(
            fid.getUUID(), schemaName, sqlName, fr.getSeparatorChar());

        return fr.getAsFile(externalName, generationId);
    }

    @Override
    public long addJar(InputStream is, JarUtil util) throws StandardException {
        //
        //Like create table we say we are writing before we read the dd
        dd.startWriting(util.getLanguageConnectionContext());
        FileInfoDescriptor fid = util.getInfo();
        if (fid != null)
            throw
                    StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                            fid.getDescriptorType(), util.getSqlName(), fid.getSchemaDescriptor().getDescriptorType(), util.getSchemaName());

        SchemaDescriptor sd = dd.getSchemaDescriptor(util.getSchemaName(), null, true);
        try {
            util.notifyLoader(false);
            dd.invalidateAllSPSPlans(); // This will break other nodes, must do ddl

            UUID id = Monitor.getMonitor().getUUIDFactory().createUUID();
            final String jarExternalName = JarUtil.mkExternalName(
                    id, util.getSchemaName(), util.getSqlName(), util.getFileResource().getSeparatorChar());

            long generationId = util.setJar(jarExternalName, is, true, 0L);

            fid = util.getDataDescriptorGenerator().newFileInfoDescriptor(id, sd, util.getSqlName(), generationId);
            dd.addDescriptor(fid, sd, DataDictionary.SYSFILES_CATALOG_NUM,
                    false, util.getLanguageConnectionContext().getTransactionExecute(), false);
            return generationId;
        } finally {
            util.notifyLoader(true);
        }
    }

    @Override
    public void dropJar(JarUtil util) throws StandardException {
        //
        //Like create table we say we are writing before we read the dd
        dd.startWriting(util.getLanguageConnectionContext());
        FileInfoDescriptor fid = util.getInfo();
        if (fid == null)
            throw StandardException.newException(SQLState.LANG_JAR_FILE_DOES_NOT_EXIST, util.getSqlName(), util.getSchemaName());

        String dbcp_s = PropertyUtil.getServiceProperty(util.getLanguageConnectionContext().getTransactionExecute(),Property.DATABASE_CLASSPATH);
        if (dbcp_s != null)
        {
            String[][]dbcp= IdUtil.parseDbClassPath(dbcp_s);
            boolean found = false;
            //
            //Look for the jar we are dropping on our database classpath.
            //We don't concern ourselves with 3 part names since they may
            //refer to a jar file in another database and may not occur in
            //a database classpath that is stored in the propert congomerate.
            for (String[] aDbcp : dbcp)
                if (dbcp.length == 2 &&
                        aDbcp[0].equals(util.getSchemaName()) && aDbcp[1].equals(util.getSqlName()))
                    found = true;
            if (found)
                throw StandardException.newException(SQLState.LANG_CANT_DROP_JAR_ON_DB_CLASS_PATH_DURING_EXECUTION,
                        IdUtil.mkQualifiedName(util.getSchemaName(),util.getSqlName()),
                        dbcp_s);
        }

        try {

            util.notifyLoader(false);
            dd.invalidateAllSPSPlans();
            DependencyManager dm = dd.getDependencyManager();
            dm.invalidateFor(fid, DependencyManager.DROP_JAR, util.getLanguageConnectionContext());

            UUID id = fid.getUUID();
            dd.dropFileInfoDescriptor(fid);
            util.getFileResource().remove(
                    JarUtil.mkExternalName(
                            id, util.getSchemaName(), util.getSqlName(), util.getFileResource().getSeparatorChar()),
                    fid.getGenerationId());
        } finally {
            util.notifyLoader(true);
        }


    }

    @Override
    public long replaceJar(InputStream is, JarUtil util) throws StandardException {
//
        //Like create table we say we are writing before we read the dd
        dd.startWriting(util.getLanguageConnectionContext());

        //
        //Temporarily drop the FileInfoDescriptor from the data dictionary.
        FileInfoDescriptor fid = util.getInfo();
        if (fid == null)
            throw StandardException.newException(SQLState.LANG_JAR_FILE_DOES_NOT_EXIST, util.getSqlName(), util.getSchemaName());

        try {
            // disable loads from this jar
            util.notifyLoader(false);
            dd.invalidateAllSPSPlans();
            dd.dropFileInfoDescriptor(fid);
            final String jarExternalName =
                    JarUtil.mkExternalName(
                            fid.getUUID(), util.getSchemaName(), util.getSqlName(), util.getFileResource().getSeparatorChar());

            //
            //Replace the file.
            long generationId = util.setJar(jarExternalName, is, false,
                    fid.getGenerationId());

            //
            //Re-add the descriptor to the data dictionary.
            FileInfoDescriptor fid2 =
                    util.getDataDescriptorGenerator().newFileInfoDescriptor(fid.getUUID(),fid.getSchemaDescriptor(),
                            util.getSqlName(),generationId);
            dd.addDescriptor(fid2, fid.getSchemaDescriptor(),
                    DataDictionary.SYSFILES_CATALOG_NUM, false, util.getLanguageConnectionContext().getTransactionExecute(), false);
            return generationId;

        } finally {

            // reenable class loading from this jar
            util.notifyLoader(true);
        }
    }
}
