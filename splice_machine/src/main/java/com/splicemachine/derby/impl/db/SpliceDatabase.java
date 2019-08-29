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

package com.splicemachine.derby.impl.db;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.AuthenticationConfiguration;
import com.splicemachine.db.iapi.ast.ISpliceVisitor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.AuthenticationService;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyFactory;
import com.splicemachine.db.iapi.services.property.PropertySetCallback;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.FileInfoDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.impl.ast.*;
import com.splicemachine.db.impl.db.BasicDatabase;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;
import com.splicemachine.db.impl.sql.execute.JarUtil;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.derby.impl.sql.execute.operations.batchonce.BatchOnceVisitor;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.security.auth.login.Configuration;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SpliceDatabase extends BasicDatabase{

    private static Logger LOG=Logger.getLogger(SpliceDatabase.class);
    private AtomicBoolean registered = new AtomicBoolean(false);

    @Override
    public void boot(boolean create,Properties startParams) throws StandardException{
        Configuration.setConfiguration(null);
        SConfiguration config = SIDriver.driver().getConfiguration();

        if (startParams == null) {
            startParams = new Properties();
        }

        // Set 60 Second Default if Missing from startup parameters
        if (System.getProperty("derby.drda.timeSlice") == null)
            System.setProperty("derby.drda.timeSlice","60000");

        //  System.setProperty("derby.language.logQueryPlan", Boolean.toString(true));
        String logStatementText = System.getProperty("derby.language.logStatementText");
        if (logStatementText == null) {
            startParams.put("derby.language.logStatementText", Boolean.toString(config.debugLogStatementContext()));
        }

        if (config.debugDumpClassFile()) {
            System.setProperty("com.splicemachine.enableLegacyAsserts",Boolean.TRUE.toString());
            SanityManager.DEBUG_SET("DumpClassFile");
        }
        if (config.debugDumpBindTree()) {
            System.setProperty("com.splicemachine.enableLegacyAsserts",Boolean.TRUE.toString());
            SanityManager.DEBUG_SET("DumpBindTree");
        }
        if (config.debugDumpOptimizedTree()) {
            System.setProperty("com.splicemachine.enableLegacyAsserts",Boolean.TRUE.toString());
            SanityManager.DEBUG_SET("DumpOptimizedTree");
        }

        configureAuthentication();

        // setup authorization


        create=Boolean.TRUE.equals(EngineLifecycleService.isCreate.get()); //written like this to avoid autoboxing

        if(create){
            SpliceLogUtils.info(LOG,"Creating the Splice Machine database");
        }else{
            SpliceLogUtils.info(LOG,"Booting the Splice Machine database");
        }
        super.boot(create,startParams);
    }

    @Override
    public LanguageConnectionContext setupConnection(ContextManager cm,String user, List<String> groupuserlist, String drdaID,String dbname,
                                                     String rdbIntTkn,
                                                     CompilerContext.DataSetProcessorType dspt,
                                                     boolean skipStats,
                                                     double defaultSelectivityFactor,
                                                     String ipAddress,
                                                     String defaultSchema,
                                                     Properties sessionProperties)
            throws StandardException{

        final LanguageConnectionContext lctx=super.setupConnection(cm, user, groupuserlist,
                drdaID, dbname, rdbIntTkn, dspt, skipStats, defaultSelectivityFactor, ipAddress, defaultSchema, sessionProperties);

        // If you add a visitor, be careful of ordering.

        List<Class<? extends ISpliceVisitor>> afterOptVisitors=new ArrayList<>();
        afterOptVisitors.add(UnsupportedFormsDetector.class);
        afterOptVisitors.add(AssignRSNVisitor.class);
        afterOptVisitors.add(RowLocationColumnVisitor.class);
        afterOptVisitors.add(FixSubqueryColRefs.class);
        afterOptVisitors.add(JoinConditionVisitor.class);
        afterOptVisitors.add(BatchOnceVisitor.class);
        afterOptVisitors.add(LimitOffsetVisitor.class);
        afterOptVisitors.add(PlanPrinter.class);

        List<Class<? extends ISpliceVisitor>> afterBindVisitors=new ArrayList<>(1);
        afterBindVisitors.add(RepeatedPredicateVisitor.class);

        List<Class<? extends ISpliceVisitor>> afterParseClasses=Collections.emptyList();
        lctx.setASTVisitor(new SpliceASTWalker(afterParseClasses, afterBindVisitors, afterOptVisitors));

        return lctx;
    }

    /**
     * This will perform a lookup of the user (index and main table) and the default schema (index and main table)
     * <p/>
     * This method should only be used by start() methods in coprocessors.  Do not use for sinks or observers.
     */
    public LanguageConnectionContext generateLanguageConnectionContext(TxnView txn,ContextManager cm,String user, List<String> groupuserlist, String drdaID,String dbname,
                                                                       String rdbIntTkn,
                                                                       CompilerContext.DataSetProcessorType type,
                                                                       boolean skipStats,
                                                                       double defaultSelectivityFactor,
                                                                       String ipAddress) throws StandardException{
        TransactionController tc=((SpliceAccessManager)af).marshallTransaction(cm,txn);
        cm.setLocaleFinder(this);
        pushDbContext(cm);
        LanguageConnectionContext lctx=lcf.newLanguageConnectionContext(cm,tc,lf,this,user,
                groupuserlist,drdaID,dbname,rdbIntTkn,type,skipStats, defaultSelectivityFactor, ipAddress,
                null, null);

        pushClassFactoryContext(cm,lcf.getClassFactory());
        ExecutionFactory ef=lcf.getExecutionFactory();
        ef.newExecutionContext(cm);
        lctx.initialize();
        return lctx;
    }

    @Override
    public void freeze() throws SQLException{
        throw new SQLException("Unsupported Exception");
    }

    @Override
    public void unfreeze() throws SQLException{
        throw new SQLException("Unsupported Exception");
    }

    @Override
    public void checkpoint() throws SQLException{
        throw new SQLException("Unsupported Exception");
    }

    protected void configureAuthentication(){
        SConfiguration configuration =SIDriver.driver().getConfiguration();
        if(configuration.authenticationNativeCreateCredentialsDatabase()) {
            System.setProperty(Property.AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE,Boolean.toString(true));
        }

        String authTypeString=configuration.getAuthentication();
        AuthenticationType authType= AuthenticationType.valueOf(authTypeString);
        switch(authType){
            case NONE:
                SpliceLogUtils.warn(LOG,"using no auth for Splice Machine");
                System.setProperty("derby.connection.requireAuthentication","false");
                System.setProperty("derby.database.sqlAuthorization","false");
                break;
            case LDAP:
                configureLDAPAuth(configuration);
                break;
            case NATIVE:
                configureNative(configuration,false);
                break;
            case KERBEROS:
                configureKerberosAuth(configuration);
                break;
            case CUSTOM:
                configureCustomAuth(configuration);
                break;
            default:// Default is Native with warning:
                configureNative(configuration,true);
        }
        configureImpersonation(configuration);
        configureUserMapping(configuration);
    }

    private void configureImpersonation(SConfiguration configuration) {
        System.setProperty("derby.authentication.impersonation.enabled",Boolean.toString(configuration.getAuthenticationImpersonationEnabled()));
        System.setProperty("derby.authentication.impersonation.users",configuration.getAuthenticationImpersonationUsers());
    }

    private void configureUserMapping(SConfiguration config) {
        String authenticationMapGroupAttr = config.getAuthenticationMapGroupAttr();
        System.setProperty("derby.authentication.ldap.mapGroupAttr",authenticationMapGroupAttr);
    }
    private void configureKerberosAuth(SConfiguration config){
        System.setProperty("derby.connection.requireAuthentication","true");
        System.setProperty("derby.database.sqlAuthorization","true");
        SpliceLogUtils.info(LOG,"using Kerberos to authorize Splice Machine");
        System.setProperty("derby.authentication.provider", Property.AUTHENTICATION_PROVIDER_KERBEROS);
    }

    private void configureLDAPAuth(SConfiguration config){
        System.setProperty("derby.connection.requireAuthentication","true");
        System.setProperty("derby.database.sqlAuthorization","true");
        String authenticationLDAPSearchAuthDN = config.getAuthenticationLdapSearchauthdn();
        String authenticationLDAPSearchAuthPW = config.getAuthenticationLdapSearchauthPassword();
        String authenticationLDAPSearchBase = config.getAuthenticationLdapSearchbase();
        String authenticationLDAPSearchFilter = config.getAuthenticationLdapSearchfilter();
        String authenticationLDAPServer = config.getAuthenticationLdapServer();

        SpliceLogUtils.info(LOG,"using LDAP to authorize Splice Machine with "+
                        "{ldap={searchAuthDN=%s,searchBase=%s, searchFilter=%s}}",
                authenticationLDAPSearchAuthDN,
                authenticationLDAPSearchBase,
                authenticationLDAPSearchFilter);
        System.setProperty("derby.authentication.provider", Property.AUTHENTICATION_PROVIDER_LDAP);
        System.setProperty("derby.authentication.ldap.searchAuthDN",authenticationLDAPSearchAuthDN);
        System.setProperty("derby.authentication.ldap.searchAuthPW",authenticationLDAPSearchAuthPW);
        System.setProperty("derby.authentication.ldap.searchBase",authenticationLDAPSearchBase);
        System.setProperty("derby.authentication.ldap.searchFilter",authenticationLDAPSearchFilter);
        System.setProperty("derby.authentication.server",authenticationLDAPServer);

    }

    private void configureCustomAuth(SConfiguration configuration){
        System.setProperty("derby.connection.requireAuthentication","true");
        System.setProperty("derby.database.sqlAuthorization","true");
        String authenticationCustomProvider = configuration.getAuthenticationCustomProvider();
        Level logLevel = Level.INFO;
        if(authenticationCustomProvider.equals(AuthenticationConfiguration.DEFAULT_AUTHENTICATION_CUSTOM_PROVIDER)){
            logLevel=Level.WARN;
        }
        LOG.log(logLevel,String.format("using custom authentication for SpliceMachine using class %s",authenticationCustomProvider));
        System.setProperty("derby.authentication.provider",authenticationCustomProvider);
    }

    private void configureNative(SConfiguration config,boolean warn){
        System.setProperty("derby.connection.requireAuthentication","true");
        System.setProperty("derby.database.sqlAuthorization","true");
        System.setProperty("derby.authentication.provider","NATIVE:spliceDB:LOCAL");
        String authenticationNativeAlgorithm = config.getAuthenticationNativeAlgorithm();
        System.setProperty("derby.authentication.builtin.algorithm",authenticationNativeAlgorithm);
        if(warn)
            SpliceLogUtils.warn(LOG,"authentication provider could not be determined from entry {%s},  using native",AuthenticationConfiguration.AUTHENTICATION);
    }

    @Override
    protected void bootValidation(boolean create,Properties startParams) throws StandardException{
        SpliceLogUtils.trace(LOG, "bootValidation create %s, startParams %s", create, startParams);
        pf=(PropertyFactory)Monitor.bootServiceModule(create,this,com.splicemachine.db.iapi.reference.Module.PropertyFactory,startParams);
    }

    public void registerDDL(){
        if(!registered.compareAndSet(false,true)) return; //only allow one registration
        DDLDriver.driver().ddlWatcher().registerDDLListener(new DDLWatcher.DDLListener(){
            @Override
            public void startGlobalChange(){
                System.out.println("Boot Store startGlobalChange -> ");
            }

            @Override
            public void finishGlobalChange(){
                System.out.println("Boot Store finishGlobalChange -> ");
            }

            @Override
            public void startChange(DDLChange change) throws StandardException{
                DataDictionary dataDictionary=getDataDictionary();
                DependencyManager dependencyManager=dataDictionary.getDependencyManager();
                switch(change.getDdlChangeType()){
                    case CREATE_INDEX:
                        DDLUtils.preCreateIndex(change,dataDictionary,dependencyManager);
                        break;
                    case DROP_INDEX:
                        DDLUtils.preDropIndex(change,dataDictionary,dependencyManager);
                        break;
                    case DROP_SEQUENCE:
                        DDLUtils.preDropSequence(change,dataDictionary,dependencyManager);
                        break;
                    case CHANGE_PK:
                    case ADD_CHECK:
                    case ADD_FOREIGN_KEY:
                    case ADD_NOT_NULL:
                    case ADD_COLUMN:
                    case ADD_PRIMARY_KEY:
                    case ADD_UNIQUE_CONSTRAINT:
                    case DROP_COLUMN:
                    case DROP_CONSTRAINT:
                    case DROP_PRIMARY_KEY:
                    case DROP_FOREIGN_KEY:
                    case DICTIONARY_UPDATE:
                    case CREATE_TABLE:
                    case CREATE_SCHEMA:
                        break;
                    case DROP_TABLE:
                        DDLUtils.preDropTable(change,dataDictionary,dependencyManager);
                        break;
                    case DROP_VIEW:
                        DDLUtils.preDropView(change,dataDictionary,dependencyManager);
                        break;
                    case ALTER_TABLE:
                        DDLUtils.preAlterTable(change,dataDictionary,dependencyManager);
                        break;
                    case RENAME_TABLE:
                        DDLUtils.preRenameTable(change,dataDictionary,dependencyManager);
                        break;
                    case CREATE_TRIGGER:
                        DDLUtils.preCreateTrigger(change,dataDictionary,dependencyManager);
                        break;
                    case CREATE_ROLE:
                        DDLUtils.preCreateRole(change,dataDictionary,dependencyManager);
                        break;
                    case DROP_TRIGGER:
                        DDLUtils.preDropTrigger(change,dataDictionary,dependencyManager);
                        break;
                    case DROP_ALIAS:
                        DDLUtils.preDropAlias(change,dataDictionary,dependencyManager);
                        break;
                    case RENAME_INDEX:
                        DDLUtils.preRenameIndex(change,dataDictionary,dependencyManager);
                        break;
                    case RENAME_COLUMN:
                        DDLUtils.preRenameColumn(change,dataDictionary,dependencyManager);
                        break;
                    case DROP_SCHEMA:
                        DDLUtils.preDropSchema(change,dataDictionary,dependencyManager);
                        break;
                    case UPDATE_SCHEMA_OWNER:
                        DDLUtils.preUpdateSchemaOwner(change,dataDictionary,dependencyManager);
                        break;
                    case DROP_ROLE:
                        DDLUtils.preDropRole(change,dataDictionary,dependencyManager);
                        break;
                    case TRUNCATE_TABLE:
                        DDLUtils.preTruncateTable(change,dataDictionary,dependencyManager);
                        break;
                    case REVOKE_PRIVILEGE:
                        DDLUtils.preRevokePrivilege(change,dataDictionary,dependencyManager);
                        break;
                    case ALTER_STATS:
                        DDLUtils.preAlterStats(change,dataDictionary,dependencyManager);
                        break;
                    case ENTER_RESTORE_MODE:
                        SIDriver.driver().lifecycleManager().enterRestoreMode();
                        Collection<LanguageConnectionContext> allContexts=ContextService.getFactory().getAllContexts(LanguageConnectionContext.CONTEXT_ID);
                        for(LanguageConnectionContext context : allContexts){
                            context.enterRestoreMode();
                        }
                        break;
                    case NOTIFY_JAR_LOADER:
                        DDLUtils.preNotifyJarLoader(change,dataDictionary,dependencyManager);
                        break;
                    case NOTIFY_MODIFY_CLASSPATH:
                        DDLUtils.preNotifyModifyClasspath(change,dataDictionary,dependencyManager);
                        break;
                    case REFRESH_ENTRPRISE_FEATURES:
                        EngineDriver.driver().refreshEnterpriseFeatures();
                        break;
                    case GRANT_REVOKE_ROLE:
                        DDLUtils.preGrantRevokeRole(change, dataDictionary, dependencyManager);
                        break;
                    case SET_DATABASE_PROPERTY:
                        DDLUtils.preSetDatabaseProperty(change, dataDictionary, dependencyManager);
                        break;
                    case UPDATE_SYSTEM_PROCEDURES:
                        DDLUtils.preUpdateSystemProcedures(change, dataDictionary);
                }
                final List<DDLAction> ddlActions = new ArrayList<>();
                ddlActions.add(new AddIndexToPipeline());
                ddlActions.add(new DropIndexFromPipeline());
                ddlActions.add(new AddForeignKeyToPipeline());
                ddlActions.add(new DropForeignKeyFromPipeline());
                ddlActions.add(new AddUniqueConstraintToPipeline());
                for (DDLAction action : ddlActions) {
                    action.accept(change);
                }
            }

            @Override
            @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
            public void changeSuccessful(String changeId,DDLChange change) throws StandardException{
                DataDictionary dataDictionary=getDataDictionary();
                DependencyManager dependencyManager=dataDictionary.getDependencyManager();
                switch(change.getDdlChangeType()){
                    case NOTIFY_JAR_LOADER:
                        DDLUtils.postNotifyJarLoader(change,dataDictionary,dependencyManager);
                        break;
                }
            }

            @Override
            public void changeFailed(String changeId){
                LOG.warn("Change failed "+ changeId);
            }
        });
    }

    @Override
    protected void bootStore(boolean create,Properties startParams) throws StandardException{
        //boot the ddl environment if necessary
//        DDLEnvironment env = DDLEnvironmentLoader.loadEnvironment(SIDriver.driver().getConfiguration());

        SpliceLogUtils.trace(LOG,"bootStore create %s, startParams %s",create,startParams);
        af=(AccessFactory)Monitor.bootServiceModule(create,this,AccessFactory.MODULE,startParams);
        ((SpliceAccessManager) af).setDatabase(this);
        if(create){
            TransactionController tc=af.getTransaction(ContextService.getFactory().getCurrentContextManager());
            ((SpliceTransaction)((SpliceTransactionManager)tc).getRawTransaction()).elevate(Bytes.toBytes("boot"));
        }

    }

    /**
     @see PropertySetCallback#apply
     @exception StandardException Thrown on error.
     */
    @Override
    public Serviceable apply(String key, Serializable value, Dictionary p,TransactionController tc)
            throws StandardException {
        // only interested in the classpath
        if (!key.equals(Property.DATABASE_CLASSPATH)) return null;
        // only do the change dynamically if we are already
        // a per-database classapath.
        if (cfDB != null) {
            String newClasspath = (String) value;
            if (newClasspath == null)
                newClasspath = "";
            dd.invalidateAllSPSPlans();
            DDLMessage.DDLChange ddlChange = ProtoUtil.createNotifyModifyClasspath( ((SpliceTransactionManager)tc).getActiveStateTxn().getTxnId(), newClasspath);
            tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
        }
        return null;
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
            TransactionController tc= ((DataDictionaryImpl)dd).getTransactionCompile();
            DDLMessage.DDLChange ddlChange = ProtoUtil.createNotifyJarLoader( ((SpliceTransactionManager)tc).getActiveStateTxn().getTxnId(), false,false,null,null);
            tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
            com.splicemachine.db.catalog.UUID id = Monitor.getMonitor().getUUIDFactory().createUUID();
            final String jarExternalName = JarUtil.mkExternalName(
                    id, util.getSchemaName(), util.getSqlName(), util.getFileResource().getSeparatorChar());

            long generationId = util.setJar(jarExternalName, is, true, 0L);
            fid = util.getDataDescriptorGenerator().newFileInfoDescriptor(id, sd, util.getSqlName(), generationId);
            dd.addDescriptor(fid, sd, DataDictionary.SYSFILES_CATALOG_NUM,
                    false, util.getLanguageConnectionContext().getTransactionExecute(), false);
            return generationId;
        } finally {
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
            for (int ix=0;ix<dbcp.length;ix++)
                if (dbcp.length == 2 &&
                        dbcp[ix][0].equals(util.getSchemaName()) && dbcp[ix][1].equals(util.getSqlName()))
                    found = true;
            if (found)
                throw StandardException.newException(SQLState.LANG_CANT_DROP_JAR_ON_DB_CLASS_PATH_DURING_EXECUTION,
                        IdUtil.mkQualifiedName(util.getSchemaName(),util.getSqlName()),
                        dbcp_s);
        }

        try {
            TransactionController tc= ((DataDictionaryImpl)dd).getTransactionCompile();
            DDLMessage.DDLChange ddlChange = ProtoUtil.createNotifyJarLoader( ((SpliceTransactionManager)tc).getActiveStateTxn().getTxnId(), false,true,util.getSchemaName(),util.getSqlName());
            tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
            com.splicemachine.db.catalog.UUID id = fid.getUUID();
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
            TransactionController tc= ((DataDictionaryImpl)dd).getTransactionCompile();
            DDLMessage.DDLChange ddlChange = ProtoUtil.createNotifyJarLoader( ((SpliceTransactionManager)tc).getActiveStateTxn().getTxnId(), false,false,null,null);
            tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
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

    /**
     * Override boot authentication service
     *
     * @param create
     * @param props
     * @return
     * @throws StandardException
     */
    @Override
    protected AuthenticationService bootAuthenticationService(boolean create, Properties props) throws StandardException {
        return (AuthenticationService)
                Monitor.bootServiceModule(create, this, AuthenticationService.MODULE, props);
    }

}
