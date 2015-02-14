package com.splicemachine.derby.impl.db;

import javax.security.auth.login.Configuration;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CancellationException;

import com.google.common.collect.Lists;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLWatcher;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.hbase.backup.*;
import com.google.common.io.Closeables;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.db.iapi.error.ShutdownException;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyFactory;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.db.BasicDatabase;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.ast.AssignRSNVisitor;
import com.splicemachine.derby.impl.ast.FindHashJoinColumns;
import com.splicemachine.derby.impl.ast.FixSubqueryColRefs;
import com.splicemachine.derby.impl.ast.ISpliceVisitor;
import com.splicemachine.derby.impl.ast.JoinConditionVisitor;
import com.splicemachine.derby.impl.ast.PlanPrinter;
import com.splicemachine.derby.impl.ast.RepeatedPredicateVisitor;
import com.splicemachine.derby.impl.ast.RowLocationColumnVisitor;
import com.splicemachine.derby.impl.ast.SpliceASTWalker;
import com.splicemachine.derby.impl.ast.UnsupportedFormsDetector;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.hbase.backup.Backup;
import com.splicemachine.hbase.backup.Backup.BackupScope;
import com.splicemachine.job.JobFuture;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;
import com.splicemachine.derby.impl.ast.XPlainTraceVisitor;

public class SpliceDatabase extends BasicDatabase {

    private static Logger LOG = Logger.getLogger(SpliceDatabase.class);

    @Override
    public void boot(boolean create, Properties startParams) throws StandardException {
        Configuration.setConfiguration(null);
        //System.setProperty("derby.language.logQueryPlan", Boolean.toString(true));
        if (SpliceConstants.logStatementContext)
            System.setProperty("derby.language.logStatementText", Boolean.toString(true));
        if (SpliceConstants.authenticationNativeCreateCredentialsDatabase) {
            System.setProperty(Property.AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE, Boolean.toString(true));
        }
        if (SpliceConstants.AuthenticationType.NONE.toString().equals(SpliceConstants.authentication)) {
            SpliceLogUtils.warn(LOG, "using no auth for Splice Machine",SpliceConstants.authentication);
            System.setProperty("derby.connection.requireAuthentication","false");
            System.setProperty("derby.database.sqlAuthorization", "false");
        } else {
            System.setProperty("derby.connection.requireAuthentication","true");
            System.setProperty("derby.database.sqlAuthorization", "true");
            if (SpliceConstants.AuthenticationType.CUSTOM.toString().equals(SpliceConstants.authentication)) {
                if (SpliceConstants.authenticationCustomProvider.equals(SpliceConstants.DEFAULT_AUTHENTICATION_CUSTOM_PROVIDER))
                    SpliceLogUtils.warn(LOG, "using custom authentication for Splice Machine using class {%s}, this class allows all usernames to proceed",SpliceConstants.authenticationCustomProvider);
                else
                    SpliceLogUtils.info(LOG, "using custom authentication for Splice Machine using class %s",SpliceConstants.authenticationCustomProvider);
                System.setProperty("derby.authentication.provider", SpliceConstants.DEFAULT_AUTHENTICATION_CUSTOM_PROVIDER);
            } else if (SpliceConstants.AuthenticationType.LDAP.toString().equals(SpliceConstants.authentication)) {
                SpliceLogUtils.info(LOG, "using LDAP to authorize Splice Machine with {ldap={searchAuthDN=%s,searchAuthPW=%s,searchBase=%s, searchFilter=%s"
                        + "}}",SpliceConstants.authenticationLDAPSearchAuthDN, SpliceConstants.authenticationLDAPSearchAuthPW, SpliceConstants.authenticationLDAPSearchBase, SpliceConstants.authenticationLDAPSearchFilter);
                System.setProperty("derby.authentication.provider", "LDAP");
                System.setProperty("derby.authentication.ldap.searchAuthDN", SpliceConstants.authenticationLDAPSearchAuthDN);
                System.setProperty("derby.authentication.ldap.searchAuthPW", SpliceConstants.authenticationLDAPSearchAuthPW);
                System.setProperty("derby.authentication.ldap.searchBase", SpliceConstants.authenticationLDAPSearchBase);
                System.setProperty("derby.authentication.ldap.searchFilter", SpliceConstants.authenticationLDAPSearchFilter);
                System.setProperty("derby.authentication.server", SpliceConstants.authenticationLDAPServer);
            } else if (SpliceConstants.AuthenticationType.NATIVE.toString().equals(SpliceConstants.authentication)) {
                System.setProperty("derby.authentication.provider", "NATIVE:spliceDB:LOCAL");
                System.setProperty("derby.authentication.builtin.algorithm",SpliceConstants.authenticationNativeAlgorithm);
            } else { // Default is Native with warning
                SpliceLogUtils.warn(LOG, "authentication provider could not be determined from entry {%s},  using native",SpliceConstants.authentication);
                System.setProperty("derby.authentication.provider", "NATIVE:spliceDB:LOCAL");
                System.setProperty("derby.authentication.builtin.algorithm",SpliceConstants.authenticationNativeAlgorithm);
            }
        }
        //SanityManager.DEBUG_SET("ByteCodeGenInstr");
        if(SpliceConstants.dumpClassFile)
            SanityManager.DEBUG_SET("DumpClassFile");
        //SanityManager.DEBUG_SET("DumpOptimizedTree");
        try {
            create = !ZkUtils.isSpliceLoaded();
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG,"isSpliceLoadedOnBoot failure", Exceptions.parseException(e));
        }

        if (create){
            SpliceLogUtils.info(LOG,"Creating the Splice Machine database");
        }else {
            SpliceLogUtils.info(LOG,"Booting the Splice Machine database");
        }
        super.boot(create, startParams);
        if (!create) {
            HBaseRegionLoads.start();
        }
    }


    @Override
    protected void bootValidation(boolean create, Properties startParams) throws StandardException {
        SpliceLogUtils.trace(LOG,"bootValidation create %s, startParams %s",create,startParams);
        pf = (PropertyFactory) Monitor.bootServiceModule(create, this,com.splicemachine.db.iapi.reference.Module.PropertyFactory, startParams);
    }

    @Override
    protected void bootStore(boolean create, Properties startParams) throws StandardException {
        SpliceLogUtils.trace(LOG,"bootStore create %s, startParams %s",create,startParams);
        af = (AccessFactory) Monitor.bootServiceModule(create, this, AccessFactory.MODULE, startParams);
        if(create){
            TransactionController tc = af.getTransaction( ContextService.getFactory().getCurrentContextManager());
            ((SpliceTransaction)((SpliceTransactionManager) tc).getRawTransaction()).elevate("boot".getBytes());
        }

        DDLCoordinationFactory.getWatcher().registerDDLListener(new DDLWatcher.DDLListener() {
            @Override
            public void startGlobalChange() {
                Collection<LanguageConnectionContext> allContexts = ContextService.getFactory().getAllContexts(LanguageConnectionContext.CONTEXT_ID);
                for(LanguageConnectionContext context:allContexts){
                    context.startGlobalDDLChange();
                }
            }

            @Override
            public void finishGlobalChange() {
                Collection<LanguageConnectionContext> allContexts = ContextService.getFactory().getAllContexts(LanguageConnectionContext.CONTEXT_ID);
                for(LanguageConnectionContext context:allContexts){
                    context.finishGlobalDDLChange();
                }
            }

            @Override
            public void startChange(DDLChange change) throws StandardException {
                if(change.getChangeType()==DDLChangeType.DROP_TABLE){
                    try {
                        Collection<LanguageConnectionContext> allContexts = ContextService.getFactory().getAllContexts(LanguageConnectionContext.CONTEXT_ID);
                        for(LanguageConnectionContext context:allContexts){
                            context.getDataDictionary().clearCaches();
                        }
                    } catch (ShutdownException e) {
                        LOG.warn("could not get contexts, database shutting down", e);
                    }
                } else if (change.getChangeType() == DDLChangeType.ENTER_RESTORE_MODE) {
                    TransactionLifecycle.getLifecycleManager().enterRestoreMode();
                    Collection<LanguageConnectionContext> allContexts = ContextService.getFactory().getAllContexts(LanguageConnectionContext.CONTEXT_ID);
                    for(LanguageConnectionContext context:allContexts){
                        context.enterRestoreMode();
                    }
                }
            }

            @Override
            public void finishChange(String changeId) {

            }
        });
    }

    @Override
    public LanguageConnectionContext setupConnection(ContextManager cm, String user, String drdaID, String dbname)
            throws StandardException {

        final LanguageConnectionContext lctx = super.setupConnection(cm, user, drdaID, dbname);

        DDLCoordinationFactory.getWatcher().registerDDLListener(new DDLWatcher.DDLListener() {
            @Override public void startGlobalChange() { lctx.startGlobalDDLChange(); }
            @Override public void finishGlobalChange() { lctx.finishGlobalDDLChange(); }

            @Override
            public void startChange(DDLChange change) throws StandardException {
                /* Clear DD caches on remote nodes for each DDL statement.  Before we did this remote nodes would
                 * correctly generate new activations classes and instances of constant action classes for statements on
                 * tables dropped and re-added with the same name, but would include in them stale information from the
                 * DD caches (conglomerate ID, for example) */

                DDLChangeType changeType = change.getChangeType();
                if(changeType==null) return;
                switch (changeType) {
                    case DROP_TABLE:
                    case DROP_SCHEMA:
                        lctx.getDataDictionary().clearCaches();
                        break;
                    default:
                        break; //no-op
                }
            }

            @Override public void finishChange(String changeId) {  }
        });

        // If you add a visitor, be careful of ordering.

        List<Class<? extends ISpliceVisitor>> afterOptVisitors = new ArrayList<Class<? extends ISpliceVisitor>>();
        afterOptVisitors.add(UnsupportedFormsDetector.class);
        afterOptVisitors.add(AssignRSNVisitor.class);
        afterOptVisitors.add(RowLocationColumnVisitor.class);
        afterOptVisitors.add(JoinConditionVisitor.class);
        afterOptVisitors.add(FindHashJoinColumns.class);
        afterOptVisitors.add(FixSubqueryColRefs.class);
        afterOptVisitors.add(PlanPrinter.class);
        afterOptVisitors.add(XPlainTraceVisitor.class);

        List<Class<? extends ISpliceVisitor>> afterBindVisitors = new ArrayList<Class<? extends ISpliceVisitor>>(1);
        afterBindVisitors.add(RepeatedPredicateVisitor.class);

        lctx.setASTVisitor(new SpliceASTWalker(Collections.EMPTY_LIST, afterBindVisitors, afterOptVisitors));

        return lctx;
    }


    /**
     * This is the light creation of languageConnectionContext that removes 4 rpc calls per context creation.
     */
    public LanguageConnectionContext generateLanguageConnectionContext(TxnView txn,
                                                                       ContextManager cm,
                                                                       String user,
                                                                       String drdaID,
                                                                       String dbname,
                                                                       String sessionUserName,
                                                                       SchemaDescriptor defaultSchemaDescriptor) throws StandardException {
        TransactionController tc = ((SpliceAccessManager) af).marshallTransaction(cm, txn);
        cm.setLocaleFinder(this);
        pushDbContext(cm);
        LanguageConnectionContext lctx = lcf.newLanguageConnectionContext(cm, tc, lf, this, user, drdaID, dbname);
        pushClassFactoryContext(cm, lcf.getClassFactory());
        ExecutionFactory ef = lcf.getExecutionFactory();
        ef.newExecutionContext(cm);
        lctx.initializeSplice(sessionUserName, defaultSchemaDescriptor);
        return lctx;
    }
    /**
     * This will perform a lookup of the user (index and main table) and the default schema (index and main table)
     *
     * This method should only be used by start() methods in coprocessors.  Do not use for sinks or observers.
     */
    public LanguageConnectionContext generateLanguageConnectionContext(TxnView txn, ContextManager cm, String user, String drdaID, String dbname) throws StandardException {
        TransactionController tc = ((SpliceAccessManager) af).marshallTransaction(cm, txn);
        cm.setLocaleFinder(this);
        pushDbContext(cm);
        LanguageConnectionContext lctx = lcf.newLanguageConnectionContext(cm, tc, lf, this, user, drdaID, dbname);
        pushClassFactoryContext(cm, lcf.getClassFactory());
        ExecutionFactory ef = lcf.getExecutionFactory();
        ef.newExecutionContext(cm);
        lctx.initialize();
        return lctx;
    }

    @Override
    public void startReplicationMaster(String dbmaster, String host, int port, String replicationMode) throws SQLException {
        throw new SQLException("Unsupported Exception");
    }
    @Override
    public void stopReplicationMaster() throws SQLException {
        throw new SQLException("Unsupported Exception");
    }
    @Override
    public void stopReplicationSlave() throws SQLException {
        throw new SQLException("Unsupported Exception");
    }
    @Override
    public void failover(String dbname) throws StandardException {
        throw StandardException.plainWrapException(new SQLException("Unsupported Exception"));
    }
    @Override
    public void freeze() throws SQLException {
        throw new SQLException("Unsupported Exception");
    }
    @Override
    public void unfreeze() throws SQLException {
        throw new SQLException("Unsupported Exception");
    }

    @Override
    public void restore(String restoreDir, boolean wait) throws SQLException {}

    public static void SYSCS_RESTORE_DATABASE(String restoreDir, ResultSet[] resultSets) throws StandardException, SQLException{
        HBaseAdmin admin = null;
        String changeId = null;
        LanguageConnectionContext lcc = null;
        Connection conn = null;
        IteratorNoPutResultSet inprs = null;
        try {
            admin = SpliceUtilities.getAdmin();

            conn = SpliceAdmin.getDefaultConn();
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

            // Check for ongoing backup...
            String backupResponse = null;
            if ( (backupResponse = BackupUtils.isBackupRunning()) != null)
                throw new SQLException(backupResponse); // TODO i18n
            Backup backup = Backup.readBackup(restoreDir,null,BackupScope.D);

            // enter restore mode
            DDLChange change = new DDLChange(backup.getBackupTransaction(), DDLChangeType.ENTER_RESTORE_MODE);
            changeId = DDLCoordinationFactory.getController().notifyMetadataChange(change);


            // recreate tables
            for (HTableDescriptor table : admin.listTables()) {
                // TODO keep old tables around in case something goes wrong
                admin.disableTable(table.getName());
                admin.deleteTable(table.getName());
            }

            HashMap<String, BackupItem> backUpItems = backup.getBackupItems();
            for (String key : backUpItems.keySet()) {
                BackupItem backupItem = backUpItems.get(key);
                backupItem.recreateItem(admin);
            }

            JobFuture future = null;
            JobInfo info = null;
            long start = System.currentTimeMillis();
            int totalItems = backup.getBackupItems().size();
            int completedItems = 0;
            // bulk import the regions
            for (String key : backUpItems.keySet()) {
                BackupItem backupItem = backUpItems.get(key);
                HTableInterface table = SpliceAccessManager.getHTable(backupItem.getBackupItemBytes());
                RestoreBackupJob job = new RestoreBackupJob(backupItem,table,null);
                future = SpliceDriver.driver().getJobScheduler().submit(job);
                info = new JobInfo(job.getJobId(),future.getNumTasks(), start);
                info.setJobFuture(future);
                try{
                    future.completeAll(info);
                }catch(CancellationException ce){
                    throw Exceptions.parseException(ce);
                }catch(Throwable t){
                    info.failJob();
                    throw t;
                }
                completedItems++;
                LOG.info(String.format("Restore progress: %d of %d items restored", completedItems, totalItems));
            }

            // purge transactions
            PurgeTransactionsJob job = new PurgeTransactionsJob(backup.getBackupTransaction(),
                    backup.getBackupTimestamp(),
                    SpliceAccessManager.getHTable(SpliceConstants.TRANSACTION_TABLE_BYTES) );
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            info = new JobInfo(job.getJobId(),future.getNumTasks(), start);
            info.setJobFuture(future);
            try{
                future.completeAll(info);
            }catch(CancellationException ce){
                throw Exceptions.parseException(ce);
            }catch(Throwable t){
                info.failJob();
                throw t;
            }

            // Print reboot statement
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{
                    new GenericColumnDescriptor("result", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 30)),
                    new GenericColumnDescriptor("warnings", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 50))
            };
            ExecRow template = new ValueRow(2);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar(), new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue("Restore completed");
            template.getColumn(2).setValue("Database has to be rebooted");
            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();

            LOG.info("Restore completed. Database reboot is required.");

        } catch (Throwable t) {
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{
                    new GenericColumnDescriptor("Error", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, t.getMessage().length()))};
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue(t.getMessage());

            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();
            SpliceLogUtils.error(LOG, "Error recovering backup", t);

        } finally {
            try {
                if (changeId != null) {
                    DDLCoordinationFactory.getController().finishMetadataChange(changeId);
                }
            } catch (StandardException e) {
                SpliceLogUtils.error(LOG, "Error recovering backup", e);
            }
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
            Closeables.closeQuietly(admin);
        }

    }

    private static void createSnapshots(String snapId) throws StandardException {
        
    	try {
            HBaseAdmin admin = SpliceUtilities.getAdmin();
            HTableDescriptor[] descriptorArray = admin.listTables();
            LOG.info("Snapshot database id="+snapId+
            		" starts for "+ descriptorArray.length+" tables.");
            long globalStart = System.currentTimeMillis();
            for (HTableDescriptor descriptor: descriptorArray) {
                String tableName = descriptor.getNameAsString();                
                long start = System.currentTimeMillis();
                String snapshotName = tableName + "_"+snapId;
                admin.snapshot(snapshotName.getBytes(), tableName.getBytes());
                LOG.info("Snapshot: "+tableName+" done in "+ (System.currentTimeMillis() - start)+"ms");
            }
            LOG.info("Snapshot database finished in +" +(System.currentTimeMillis() - globalStart)/1000+ " sec");
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    	
    }
    
    // 
    // This API call is only for full DB backup
    // TODO: add backup for schema:table(s)
    //
    @Override
    public void backup(String backupDir, long parent_backup_id, boolean wait) throws SQLException {

        HBaseAdmin admin = null;
        Backup backup = null;
        try {

            // Check for ongoing backup...
            // TODO: move backup table to sys schema. Use zookeeper to make sure only one backup or restore job is executing
            String backupResponse = null;
            if ( (backupResponse = BackupUtils.isBackupRunning()) != null)
                throw new SQLException(backupResponse); // TODO i18n

            backup = Backup.createBackup(backupDir, BackupScope.D, parent_backup_id);
            backup.createBaseBackupDirectory();
            admin = SpliceUtilities.getAdmin();
            backup.createBackupItems(admin);
            backup.insertBackup();
            backup.createProperties();
            // Create snapshots first for all tables in backup list
            createSnapshots( Long.toString(backup.getBackupTimestamp()));
            
            HashMap<String, BackupItem> backupItems = backup.getBackupItems();
            for (String key : backupItems.keySet()) {
                BackupItem backupItem =  backupItems.get(key);
                backupItem.doBackup();
            }

            // create metadata, including timestamp source's timestamp
            // this has to be called after all tables have been dumped.
            backup.createMetadata();

            if (backup.isTemporaryBaseFolder()) {
                backup.moveToBaseFolder();
            }

            backup.markBackupSuccesful();
            backup.writeBackupStatusChange();
        } catch (Throwable e) {
            if (backup != null) {
                backup.markBackupFailed();
                backup.writeBackupStatusChange();
            }
            LOG.error("Couldn't backup database", e);
            throw new SQLException(Exceptions.parseException(e));
        }finally {
            Closeables.closeQuietly(admin);
        }
    }
    @Override
    public void backupAndEnableLogArchiveMode(String backupDir,  boolean deleteOnlineArchivedLogFiles, boolean wait) throws SQLException {
        throw new SQLException("Unsupported Exception");
    }
    @Override
    public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles) throws SQLException {
        throw new SQLException("Unsupported Exception");
    }
    @Override
    public void checkpoint() throws SQLException {
        throw new SQLException("Unsupported Exception");
    }

}
