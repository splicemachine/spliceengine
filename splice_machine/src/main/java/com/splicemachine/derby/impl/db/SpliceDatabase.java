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

package com.splicemachine.derby.impl.db;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.AuthenticationConfiguration;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.ast.ISpliceVisitor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.AuthenticationService;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.PropertyHelper;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyFactory;
import com.splicemachine.db.iapi.services.property.PropertySetCallback;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.compile.SparkExecutionType;
import com.splicemachine.db.iapi.sql.compile.costing.CostModelRegistry;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.impl.ast.*;
import com.splicemachine.db.impl.db.BasicDatabase;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.catalog.BaseDataDictionary;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;
import com.splicemachine.db.impl.sql.catalog.ManagedCache;
import com.splicemachine.db.impl.sql.execute.JarUtil;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.derby.impl.sql.compile.costing.V1CostModel;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.security.auth.login.Configuration;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SpliceDatabase extends BasicDatabase{

    private static Logger LOG=Logger.getLogger(SpliceDatabase.class);
    private AtomicBoolean registered = new AtomicBoolean(false);

    @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification = "intentional")
    @Override
    public void boot(boolean create,Properties startParams) throws StandardException{
        Configuration.setConfiguration(null);
        SConfiguration config = SIDriver.driver().getConfiguration();

        if (startParams == null) {
            startParams = new Properties();
        }

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

        create = Boolean.TRUE.equals(EngineLifecycleService.isCreate.get()); //written like this to avoid autoboxing

        if (!create) {
            String catalogVersion = startParams.getProperty("catalogVersion");
            if (catalogVersion == null) {
                BaseDataDictionary.WRITE_NEW_FORMAT = false;
                BaseDataDictionary.READ_NEW_FORMAT = false;
            }
            else {
                String s[] = catalogVersion.split("\\.");
                if (s.length > 3) {
                    int sprintNumber = Integer.parseInt(s[3]);
                    if (sprintNumber < BaseDataDictionary.SERDE_UPGRADE_SPRINT) {
                        BaseDataDictionary.WRITE_NEW_FORMAT = false;
                        BaseDataDictionary.READ_NEW_FORMAT = false;
                    }
                }
            }
        }

        CostModelRegistry.registerCostModel("v1", new V1CostModel());

        if(create){
            SpliceLogUtils.info(LOG,"Creating the Splice Machine database");
        }else{
            SpliceLogUtils.info(LOG,"Booting the Splice Machine database");
        }
        super.boot(create,startParams);
    }

    private void setupASTVisitors(LanguageConnectionContext lctx) {

        String role = SIDriver.driver().lifecycleManager().getReplicationRole();
        lctx.setReplicationRole(role);
        // If you add a visitor, be careful of ordering.

        List<Class<? extends ISpliceVisitor>> afterOptVisitors=new ArrayList<>();
        afterOptVisitors.add(UnsupportedFormsDetector.class);
        afterOptVisitors.add(AssignRSNVisitor.class);
        afterOptVisitors.add(FixSubqueryColRefs.class);
        afterOptVisitors.add(JoinConditionVisitor.class);
        afterOptVisitors.add(LimitOffsetVisitor.class);
        afterOptVisitors.add(PlanPrinter.class);

        List<Class<? extends ISpliceVisitor>> afterBindVisitors=new ArrayList<>(2);
        afterBindVisitors.add(RepeatedPredicateVisitor.class);
        afterBindVisitors.add(QueryRewriteVisitor.class);

        List<Class<? extends ISpliceVisitor>> afterParseClasses=Collections.emptyList();
        lctx.setASTVisitor(new SpliceASTWalker(afterParseClasses, afterBindVisitors, afterOptVisitors));

    }

    @Override
    public LanguageConnectionContext setupConnection(ContextManager cm, String user, List<String> groupuserlist, String drdaID, String dbname,
                                                     String rdbIntTkn,
                                                     long uselessMachineID,
                                                     DataSetProcessorType dspt,
                                                     SparkExecutionType sparkExecutionType,
                                                     boolean skipStats,
                                                     double defaultSelectivityFactor,
                                                     String ipAddress,
                                                     String defaultSchema,
                                                     Properties sessionProperties)
            throws StandardException{

        final LanguageConnectionContext lctx=super.setupConnection(cm, user, groupuserlist,
                drdaID, dbname, rdbIntTkn, getMachineId(), dspt, sparkExecutionType, skipStats,
                defaultSelectivityFactor, ipAddress, defaultSchema, sessionProperties);

        setupASTVisitors(lctx);

        SIDriver.driver().getSessionsWatcher().registerSession(lctx.getMachineID(), lctx.getSessionID());
        return lctx;
    }

    /**
     * This will perform a lookup of the user (index and main table) and the default schema (index and main table)
     * <p/>
     * This method should only be used by start() methods in coprocessors.  Do not use for sinks or observers.
     */
    public LanguageConnectionContext generateLanguageConnectionContext(TxnView txn, ContextManager cm, String user, List<String> groupuserlist, String drdaID, String dbname,
                                                                       String rdbIntTkn,
                                                                       DataSetProcessorType type,
                                                                       SparkExecutionType sparkExecutionType, boolean skipStats,
                                                                       double defaultSelectivityFactor,
                                                                       String ipAddress,
                                                                       ManagedCache<UUID, SPSDescriptor> spsCache,
                                                                       List<String> defaultRoles,
                                                                       SchemaDescriptor initialDefaultSchemaDescriptor,
                                                                       long driverTxnId,
                                                                       TransactionController reuseTC,
                                                                       ArrayList<DisplayedTriggerInfo> triggerInfos,
                                                                       ConcurrentMap<UUID, String> triggerIdToNameMap,
                                                                       ConcurrentMap<java.util.UUID, DisplayedTriggerInfo> queryIdToTriggerInfoMap,
                                                                       ConcurrentMap<java.util.UUID, Long> queryTxnIdSet) throws StandardException{
        TransactionController tc = reuseTC == null ? ((SpliceAccessManager)af).marshallTransaction(cm,txn) : reuseTC;
        cm.setLocaleFinder(this);
        pushDbContext(cm);
        LanguageConnectionContext lctx=lcf.newLanguageConnectionContext(cm,tc,lf,this,user,
                groupuserlist,drdaID,dbname,rdbIntTkn,getMachineId(),type, sparkExecutionType, skipStats, defaultSelectivityFactor,
                ipAddress, null,
                spsCache, defaultRoles, initialDefaultSchemaDescriptor, driverTxnId, null,
                triggerInfos, triggerIdToNameMap, queryIdToTriggerInfoMap, queryTxnIdSet);

        pushClassFactoryContext(cm,lcf.getClassFactory());
        ExecutionFactory ef=lcf.getExecutionFactory();
        ef.newExecutionContext(cm);
        lctx.initialize();
        setupASTVisitors(lctx);
        return lctx;
    }

    private long getMachineId() {
        // In EngineLifeCycleService, internal connections can be created before
        // engine driver is loaded. For these internal connections, machine IDs
        // are not ready yet. Assign 0 for them.
        EngineDriver driver = EngineDriver.driver();
        return driver == null ? 0 : driver.getMachineID();
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
            System.setProperty(PropertyHelper.AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE,Boolean.toString(true));
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
            case TOKEN:
                configureTokenAuth(configuration);
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
        System.setProperty("derby.authentication.provider", PropertyHelper.AUTHENTICATION_PROVIDER_KERBEROS);
    }

    private void configureTokenAuth(SConfiguration config){
        System.setProperty("derby.connection.requireAuthentication","true");
        System.setProperty("derby.database.sqlAuthorization","true");
        SpliceLogUtils.info(LOG,"using Token to authorize Splice Machine");
        System.setProperty("derby.authentication.provider", Property.AUTHENTICATION_PROVIDER_JWT_TOKEN);
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
        System.setProperty("derby.authentication.provider", PropertyHelper.AUTHENTICATION_PROVIDER_LDAP);
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
                DDLUtils.dispatchChangeAction(change, dataDictionary, dependencyManager, null);
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
            af.elevateRawTransaction(Bytes.toBytes("boot"));
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

        SchemaDescriptor sd = dd.getSchemaDescriptor(null, util.getSchemaName(), null, true);
        try {
            TransactionController tc= ((DataDictionaryImpl)dd).getTransactionCompile();
            DDLMessage.DDLChange ddlChange = ProtoUtil.createNotifyJarLoader( ((SpliceTransactionManager)tc).getActiveStateTxn().getTxnId(), false,false,null,null, null);
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
            DDLMessage.DDLChange ddlChange = ProtoUtil.createNotifyJarLoader(
                    ((SpliceTransactionManager)tc).getActiveStateTxn().getTxnId(),
                    false,
                    true,
                    util.getSchemaName(),
                    util.getSqlName(),
                    (BasicUUID) util.getDbId());
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
            DDLMessage.DDLChange ddlChange = ProtoUtil.createNotifyJarLoader( ((SpliceTransactionManager)tc).getActiveStateTxn().getTxnId(), false,false,null,null, null);
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

    @Override
    public void unregisterSession(long machineID, String sessionId) {
        SIDriver.driver().getSessionsWatcher().unregisterSession(machineID, sessionId);
    }
}
