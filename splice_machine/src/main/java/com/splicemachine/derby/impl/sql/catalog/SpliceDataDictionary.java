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

package com.splicemachine.derby.impl.sql.catalog;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.catalog.*;
import com.splicemachine.db.impl.sql.execute.IndexColumnOrder;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.ddl.DDLWatcher;
import com.splicemachine.derby.impl.sql.catalog.upgrade.SpliceCatalogUpgradeScripts;
import com.splicemachine.derby.impl.sql.depend.SpliceDependencyManager;
import com.splicemachine.derby.impl.sql.execute.sequence.SequenceKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.*;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.management.Manager;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.tools.version.ManifestReader;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.sql.Types;
import java.util.*;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceDataDictionary extends DataDictionaryImpl{

    protected static final Logger LOG=Logger.getLogger(SpliceDataDictionary.class);
    private volatile TabInfoImpl pkTable=null;
    private volatile TabInfoImpl backupTable=null;
    private volatile TabInfoImpl backupItemsTable=null;
    private volatile TabInfoImpl tableStatsTable=null;
    private volatile TabInfoImpl columnStatsTable=null;
    private volatile TabInfoImpl physicalStatsTable=null;
    private volatile TabInfoImpl sourceCodeTable=null;
    private volatile TabInfoImpl snapshotTable = null;
    private volatile TabInfoImpl tokenTable = null;
    private volatile TabInfoImpl replicationTable = null;
    private volatile TabInfoImpl ibmConnectionTable = null;
    private Splice_DD_Version spliceSoftwareVersion;
    protected boolean metadataAccessRestrictionEnabled;

    public static final String SPLICE_DATA_DICTIONARY_VERSION="SpliceDataDictionaryVersion";
    private ConcurrentLinkedHashMap<String, byte[]> sequenceRowLocationBytesMap=null;
    private ConcurrentLinkedHashMap<String, SequenceDescriptor[]> sequenceDescriptorMap=null;

    @Override
    public SystemProcedureGenerator getSystemProcedures(){
        return new SpliceSystemProcedures(this);
    }


    @Override
    protected void addSubKeyConstraint(KeyConstraintDescriptor descriptor,
                                       TransactionController tc) throws StandardException{
        ExecRow row;
        TabInfoImpl ti;

        /*
         * Foreign keys get a row in SYSFOREIGNKEYS, and all others get a row in SYSKEYS.
         */
        if(descriptor.getConstraintType()==DataDictionary.FOREIGNKEY_CONSTRAINT){
            if(SanityManager.DEBUG){
                if(!(descriptor instanceof ForeignKeyConstraintDescriptor)){
                    SanityManager.THROWASSERT("descriptor not an fk descriptor, is "+descriptor.getClass().getName());
                }
            }
            @SuppressWarnings("ConstantConditions")
            ForeignKeyConstraintDescriptor fkDescriptor=(ForeignKeyConstraintDescriptor)descriptor;

            ti=getNonCoreTI(SYSFOREIGNKEYS_CATALOG_NUM);
            SYSFOREIGNKEYSRowFactory fkkeysRF=(SYSFOREIGNKEYSRowFactory)ti.getCatalogRowFactory();

            row=fkkeysRF.makeRow(fkDescriptor,null);

            /*
             * Now we need to bump the reference count of the constraint that this FK references
             */
            ReferencedKeyConstraintDescriptor refDescriptor=fkDescriptor.getReferencedConstraint();
            refDescriptor.incrementReferenceCount();
            int[] colsToSet=new int[1];
            colsToSet[0]=SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_REFERENCECOUNT;

            /* Have to update the reference count in a nested transaction here because the SYSCONSTRAINTS row we are
             * updating (a primary key constraint or unique index constraint) may have been created in the same
             * statement as the FK (create table for self referencing FK, for example). In that case the KeyValue for
             * that constraint row will have the same rowKey AND timestamp. Updating here with the same ts would REPLACE
             * the entire row with just the updated reference count column, corrupting the row (DB-3345). */
            TransactionController transactionController=tc.startNestedUserTransaction(false,true);
            try{
                updateConstraintDescriptor(refDescriptor,refDescriptor.getUUID(),colsToSet,transactionController);
            }finally{
                transactionController.commit();
                transactionController.destroy();
            }

        }else if(descriptor.getConstraintType()==DataDictionary.PRIMARYKEY_CONSTRAINT){
            ti=getNonCoreTI(SYSPRIMARYKEYS_CATALOG_NUM);
            SYSPRIMARYKEYSRowFactory pkRF=(SYSPRIMARYKEYSRowFactory)ti.getCatalogRowFactory();

            row=pkRF.makeRow(descriptor,null);
        }else{
            ti=getNonCoreTI(SYSKEYS_CATALOG_NUM);
            SYSKEYSRowFactory keysRF=(SYSKEYSRowFactory)ti.getCatalogRowFactory();

            // build the row to be stuffed into SYSKEYS
            row=keysRF.makeRow(descriptor,null);
        }

        // insert row into catalog and all its indices
        int insertRetCode = ti.insertRow(row,tc);
        if(insertRetCode != TabInfoImpl.ROWNOTDUPLICATE) {
            throw duplicateDescriptorException(descriptor, null);
        }
    }

    public void createTokenTable(TransactionController tc) throws StandardException {
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();
        TabInfoImpl tokenTableInfo=getTokenTable();
        addTableIfAbsent(tc,systemSchema,tokenTableInfo,null, null);
    }

    private TabInfoImpl getTokenTable() throws StandardException{
        if(tokenTable==null){
            tokenTable=new TabInfoImpl(new SYSTOKENSRowFactory(uuidFactory,exFactory,dvf, this));
        }
        initSystemIndexVariables(tokenTable);
        return tokenTable;
    }

    public void createSnapshotTable(TransactionController tc) throws StandardException {
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();
        TabInfoImpl snapshotTableInfo=getSnapshotTable();
        addTableIfAbsent(tc,systemSchema,snapshotTableInfo,null, null);
    }

    private TabInfoImpl getSnapshotTable() throws StandardException{
        if(snapshotTable==null){
            snapshotTable=new TabInfoImpl(new SYSSNAPSHOTSRowFactory(uuidFactory,exFactory,dvf, this));
        }
        initSystemIndexVariables(snapshotTable);
        return snapshotTable;
    }

    public void createStatisticsTables(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();

        //sys_table_statistics
        TabInfoImpl tableStatsInfo=getTableStatisticsTable();
        ColumnOrdering[] tableStatsOrder= {
                new IndexColumnOrder(0),
                new IndexColumnOrder(1)
        };
        addTableIfAbsent(tc,systemSchema,tableStatsInfo,tableStatsOrder, null);

        createSysTableStatsView(tc);

        //sys_column_statistics
        ColumnOrdering[] columnPkOrder= {
                new IndexColumnOrder(0),
                new IndexColumnOrder(1),
                new IndexColumnOrder(2)
        };
        TabInfoImpl columnStatsInfo=getColumnStatisticsTable();
        addTableIfAbsent(tc,systemSchema,columnStatsInfo,columnPkOrder, null);

        createSysColumnStatsView(tc);

        //sys_physical_statistics
        ColumnOrdering[] physicalPkOrder= {
                new IndexColumnOrder(0)
        };
        TabInfoImpl physicalStatsInfo=getPhysicalStatisticsTable();
        addTableIfAbsent(tc,systemSchema,physicalStatsInfo,physicalPkOrder, null);
    }

    private void createOneSystemView(TransactionController tc,
                                     int catalogNum,
                                     String viewName,
                                     int viewIndex,
                                     SchemaDescriptor sd,
                                     String viewDef) throws StandardException {

        TableDescriptor td = getTableDescriptor(viewName, sd, tc);
        if (td != null) {
            SpliceLogUtils.info(LOG, "View: " + viewName + " in " + sd.getSchemaName() + " already exists!");
            return;
        }

        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TableDescriptor view=ddg.newTableDescriptor(viewName,
                sd,TableDescriptor.VIEW_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY,-1,null,null,null,null,null,null,false,false,null);
        addDescriptor(view,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);
        UUID viewId=view.getUUID();
        TabInfoImpl ti;
        if (catalogNum < NUM_CORE)
            ti=coreInfo[catalogNum];
        else
            ti=getNonCoreTI(catalogNum);
        CatalogRowFactory crf=ti.getCatalogRowFactory();

        ColumnDescriptor[] tableViewCds=crf.getViewColumns(view, viewId).get(viewIndex);
        addDescriptorArray(tableViewCds,view,DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        ColumnDescriptorList viewDl=view.getColumnDescriptorList();
        Collections.addAll(viewDl,tableViewCds);

        ViewDescriptor vd=ddg.newViewDescriptor(viewId,viewName, viewDef,0,sd.getUUID());
        addDescriptor(vd,sd,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc,false);

        SpliceLogUtils.info(LOG, "View: " + viewName + " in " + sd.getSchemaName() + " is created!");
    }

    private String getSchemaViewSQL() {
        SConfiguration configuration=SIDriver.driver().getConfiguration();
        String metadataRestrictionEnabled = configuration.getMetadataRestrictionEnabled();
        String schemaViewSQL;
        if (metadataRestrictionEnabled.equals(SQLConfiguration.METADATA_RESTRICTION_NATIVE)) {
            schemaViewSQL = SYSSCHEMASRowFactory.SYSSCHEMASVIEW_VIEW_SQL;
        } else if (metadataRestrictionEnabled.equals(SQLConfiguration.METADATA_RESTRICTION_RANGER)) {
            schemaViewSQL = SYSSCHEMASRowFactory.SYSSCHEMASVIEW_VIEW_RANGER;
        } else {
            schemaViewSQL = SYSSCHEMASRowFactory.SYSSCHEMASVIEW_VIEW_SQL1;
        }
        return schemaViewSQL;
    }

    public void createSystemViews(TransactionController tc) throws StandardException {
        tc.elevate("dictionary");
        //Add the SYSVW schema if it does not exists
        if (getSchemaDescriptor(SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME, tc, false) == null) {
            sysViewSchemaDesc = addSystemSchema(SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME, SchemaDescriptor.SYSVW_SCHEMA_UUID, tc);
        }

        //create AllRoles view
        SchemaDescriptor sysVWSchema=sysViewSchemaDesc;

        createOneSystemView(tc, SYSROLES_CATALOG_NUM, "SYSALLROLES", 0, sysVWSchema, SYSROLESRowFactory.ALLROLES_VIEW_SQL);

        // create sysschemasview
        createOneSystemView(tc, SYSSCHEMAS_CATALOG_NUM, "SYSSCHEMASVIEW", 0, sysVWSchema, getSchemaViewSQL());

        // create conglomeratesInSchemas view
        createOneSystemView(tc, SYSCONGLOMERATES_CATALOG_NUM, "SYSCONGLOMERATEINSCHEMAS", 0, sysVWSchema, SYSCONGLOMERATESRowFactory.SYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL);

        // create systablesView
        createOneSystemView(tc, SYSTABLES_CATALOG_NUM, "SYSTABLESVIEW", 0, sysVWSchema, SYSTABLESRowFactory.SYSTABLE_VIEW_SQL);


        // create syscolumnsView
        createOneSystemView(tc, SYSCOLUMNS_CATALOG_NUM, "SYSCOLUMNSVIEW", 0, sysVWSchema, SYSCOLUMNSRowFactory.SYSCOLUMNS_VIEW_SQL);

        SpliceLogUtils.info(LOG, "Views in SYSVW created!");
    }

    public void createTableColumnViewInSysIBM(TransactionController tc) throws StandardException {
        tc.elevate("dictionary");

        /**
         * handle syscolumns in sysibm
         */
        // check the existence of syscolumns view in sysibm
        TableDescriptor td = getTableDescriptor("SYSCOLUMNS", sysIBMSchemaDesc, tc);

        // drop it if it exists
        if (td != null) {
            ViewDescriptor vd = getViewDescriptor(td);

            // drop the view deifnition
            dropAllColumnDescriptors(td.getUUID(), tc);
            dropViewDescriptor(vd, tc);
            dropTableDescriptor(td, sysIBMSchemaDesc, tc);
        }

        // add new view deifnition
        createOneSystemView(tc, SYSCOLUMNS_CATALOG_NUM, "SYSCOLUMNS", 1, sysIBMSchemaDesc, SYSCOLUMNSRowFactory.SYSCOLUMNS_VIEW_IN_SYSIBM);

        /**
         * handle systables in sysibm
         */
        td = getTableDescriptor("SYSTABLES", sysIBMSchemaDesc, tc);

        // drop it if it exists
        if (td != null) {
            ViewDescriptor vd = getViewDescriptor(td);

            // drop the view deifnition
            dropAllColumnDescriptors(td.getUUID(), tc);
            dropViewDescriptor(vd, tc);
            dropTableDescriptor(td, sysIBMSchemaDesc, tc);
        }

        // add new view deifnition
        createOneSystemView(tc, SYSTABLES_CATALOG_NUM, "SYSTABLES", 1, sysIBMSchemaDesc, SYSTABLESRowFactory.SYSTABLES_VIEW_IN_SYSIBM);

        SpliceLogUtils.info(LOG, "The view syscolumns and systables in SYSIBM are created!");
    }

    private TabInfoImpl getIBMADMConnectionTable() throws StandardException{
        if(ibmConnectionTable==null){
            ibmConnectionTable=new TabInfoImpl(new SYSMONGETCONNECTIONRowFactory(uuidFactory,exFactory,dvf, this));
        }
        initSystemIndexVariables(ibmConnectionTable);
        return ibmConnectionTable;
    }

    public void createTablesAndViewsInSysIBMADM(TransactionController tc) throws StandardException {
        tc.elevate("dictionary");
        //Add the SYSIBMADM schema if it does not exists
        if (getSchemaDescriptor(SchemaDescriptor.IBM_SYSTEM_ADM_SCHEMA_NAME, tc, false) == null) {
            sysIBMADMSchemaDesc=addSystemSchema(SchemaDescriptor.IBM_SYSTEM_ADM_SCHEMA_NAME, SchemaDescriptor.SYSIBMADM_SCHEMA_UUID, tc);
        }

        TabInfoImpl connectionTableInfo=getIBMADMConnectionTable();
        addTableIfAbsent(tc,sysIBMADMSchemaDesc,connectionTableInfo,null, null);

        createOneSystemView(tc, SYSMONGETCONNECTION_CATALOG_NUM, "SNAPAPPL", 0, sysIBMADMSchemaDesc, SYSMONGETCONNECTIONRowFactory.SNAPAPPL_VIEW_SQL);

        createOneSystemView(tc, SYSMONGETCONNECTION_CATALOG_NUM, "SNAPAPPL_INFO", 1, sysIBMADMSchemaDesc, SYSMONGETCONNECTIONRowFactory.SNAPAPPL_INFO_VIEW_SQL);

        createOneSystemView(tc, SYSMONGETCONNECTION_CATALOG_NUM, "APPLICATIONS", 2, sysIBMADMSchemaDesc, SYSMONGETCONNECTIONRowFactory.APPLICATIONS_VIEW_SQL);

        SpliceLogUtils.info(LOG, "Tables and views in SYSIBMADM are created!");
    }

    private void updateColumnViewInSys(TransactionController tc, String tableName, int viewIndex, SchemaDescriptor schemaDescriptor, String viewDef) throws StandardException {
        tc.elevate("dictionary");

        /**
         * handle syscolumns in sysibm or sysvw
         */
        // check the existence of syscolumns view in sysibm or sysvw
        TableDescriptor td = getTableDescriptor(tableName, schemaDescriptor, tc);

        Boolean needUpdate = true;
        // drop it if it exists
        if (td != null) {
            ColumnDescriptor cd = td.getColumnDescriptor(SYSCOLUMNSRowFactory.DEFAULT_COLUMN);
            if (cd != null)
                needUpdate = false;
        }

        if (needUpdate) {
            if (td != null) {
                ViewDescriptor vd = getViewDescriptor(td);
                // drop the view deifnition
                dropAllColumnDescriptors(td.getUUID(), tc);
                dropViewDescriptor(vd, tc);
                dropTableDescriptor(td, schemaDescriptor, tc);
            }

            // add new view deifnition
            createOneSystemView(tc, SYSCOLUMNS_CATALOG_NUM, tableName, viewIndex, schemaDescriptor, viewDef);

            SpliceLogUtils.info(LOG, String.format("The view %s in %s has been updated with default column!", tableName, schemaDescriptor.getSchemaName()));
        }
    }


    public void updateColumnViewInSysIBM(TransactionController tc) throws StandardException {
        updateColumnViewInSys(tc, "SYSCOLUMNS", 1, sysIBMSchemaDesc, SYSCOLUMNSRowFactory.SYSCOLUMNS_VIEW_IN_SYSIBM);
    }

    public void updateColumnViewInSysVW(TransactionController tc) throws StandardException {
        updateColumnViewInSys(tc, "SYSCOLUMNSVIEW", 0, sysViewSchemaDesc, SYSCOLUMNSRowFactory.SYSCOLUMNS_VIEW_SQL);
    }

    public void moveSysStatsViewsToSysVWSchema(TransactionController tc) throws StandardException {
        //drop table descriptor corresponding to the tablestats view
        SchemaDescriptor sd=getSystemSchemaDescriptor();
        tc.elevate("dictionary");

        TableDescriptor td = getTableDescriptor("SYSTABLESTATISTICS", sd, tc);
        if (td != null) {
            ViewDescriptor vd = getViewDescriptor(td);

            // drop the view deifnition
            dropAllColumnDescriptors(td.getUUID(), tc);
            dropViewDescriptor(vd, tc);
            dropTableDescriptor(td, sd, tc);
        }
        // create tablestats view in sysvw schema
        SchemaDescriptor sysVWSchema=sysViewSchemaDesc;
        createOneSystemView(tc, SYSTABLESTATS_CATALOG_NUM, "SYSTABLESTATISTICS", 0, sysVWSchema, SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL );


        // drop table descriptor corresponding to the columnstats view
        td = getTableDescriptor("SYSCOLUMNSTATISTICS", sd, tc);
        if (td != null) {
            ViewDescriptor vd = getViewDescriptor(td);

            // drop the view deifnition
            dropAllColumnDescriptors(td.getUUID(), tc);
            dropViewDescriptor(vd, tc);
            dropTableDescriptor(td, sd, tc);
        }
        // create columnstats view in sysvw schema
        createOneSystemView(tc, SYSCOLUMNSTATS_CATALOG_NUM, "SYSCOLUMNSTATISTICS", 0, sysVWSchema, SYSCOLUMNSTATISTICSRowFactory.STATS_VIEW_SQL );

        SpliceLogUtils.info(LOG, "move stats views to the sysvw schema");
    }

    public void createSourceCodeTable(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();

        TabInfoImpl tableStatsInfo=getSourceCodeTable();
        addTableIfAbsent(tc,systemSchema,tableStatsInfo,null, null);
    }

    private TabInfoImpl getBackupTable() throws StandardException{
        if(backupTable==null){
            backupTable=new TabInfoImpl(new SYSBACKUPRowFactory(uuidFactory,exFactory,dvf,this));
        }
        initSystemIndexVariables(backupTable);
        return backupTable;
    }

    private TabInfoImpl getBackupItemsTable() throws StandardException{
        if(backupItemsTable==null){
            backupItemsTable=new TabInfoImpl(new SYSBACKUPITEMSRowFactory(uuidFactory,exFactory,dvf,this));
        }
        initSystemIndexVariables(backupItemsTable);
        return backupItemsTable;
    }

    public void createLassenTables(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchemaDescriptor=getSystemSchemaDescriptor();

        // Create BACKUP table
        TabInfoImpl backupTabInfo=getBackupTable();
        if(getTableDescriptor(backupTabInfo.getTableName(),systemSchemaDescriptor,tc)==null){
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Creating system table %s.%s",
                        systemSchemaDescriptor.getSchemaName(),backupTabInfo.getTableName()));
            }
            makeCatalog(backupTabInfo,systemSchemaDescriptor,tc,null);
        }else{
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.",
                        systemSchemaDescriptor.getSchemaName(),backupTabInfo.getTableName()));
            }
        }

        // Create BACKUPITEMS
        TabInfoImpl backupItemsTabInfo=getBackupItemsTable();
        if(getTableDescriptor(backupItemsTabInfo.getTableName(),systemSchemaDescriptor,tc)==null){
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Creating system table %s.%s",systemSchemaDescriptor.getSchemaName(),
                        backupItemsTabInfo.getTableName()));
            }
            makeCatalog(backupItemsTabInfo,systemSchemaDescriptor,tc,null);
        }else{
            if(LOG.isTraceEnabled()){
                LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.",
                        systemSchemaDescriptor.getSchemaName(),backupItemsTabInfo.getTableName()));
            }
        }
    }

    public void createReplicationTables(TransactionController tc) throws StandardException {
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();
        TabInfoImpl replicationTableInfo=getReplicationTable();
        addTableIfAbsent(tc,systemSchema,replicationTableInfo,null, null);
    }

    private TabInfoImpl getReplicationTable() throws StandardException{
        if(replicationTable==null){
            replicationTable=new TabInfoImpl(new SYSREPLICATIONRowFactory(uuidFactory,exFactory,dvf,this));
        }
        initSystemIndexVariables(replicationTable);
        return replicationTable;
    }

    @Override
    protected void createDictionaryTables(Properties params,
                                          TransactionController tc,
                                          DataDescriptorGenerator ddg) throws StandardException{
        //create the base dictionary tables
        super.createDictionaryTables(params,tc,ddg);

        createLassenTables(tc);

        //create the Statistics tables
        createStatisticsTables(tc);

        createSourceCodeTable(tc);

        // TODO - this needs to be included into an upgrade script (JY)
        createSnapshotTable(tc);

        createTokenTable(tc);

        createSystemViews(tc);

        createPermissionTableSystemViews(tc);

        createReplicationTables(tc);

        createTableColumnViewInSysIBM(tc);

        createTablesAndViewsInSysIBMADM(tc);
        
        createAliasToTableSystemView(tc);
    }

    @Override
    protected SystemAggregateGenerator getSystemAggregateGenerator(){
        return new SpliceSystemAggregatorGenerator(this);
    }

    @Override
    protected void loadDictionaryTables(TransactionController tc,
                                        Properties startParams) throws StandardException{
        super.loadDictionaryTables(tc,startParams);

        // Check splice data dictionary version to decide if upgrade is necessary
        upgradeIfNecessary(tc);


    }

    /**
     * Overridden so that SQL functions implemented as system procedures
     * will be found if in the SYSFUN schema. Otherwise, the default
     * behavior would be to ignore these and only consider functions
     * implicitly defined in {@link BaseDataDictionary#SYSFUN_FUNCTIONS},
     * which are not actually in the system catalog.
     */
    @SuppressWarnings("unchecked")
    public List getRoutineList(String schemaID,String routineName,char nameSpace) throws StandardException{

        List list=super.getRoutineList(schemaID,routineName,nameSpace);
        if(list.isEmpty()){
            if(schemaID.equals(SchemaDescriptor.SYSFUN_SCHEMA_UUID) &&
                    (nameSpace==AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR ||
                            nameSpace==AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR)){
                AliasDescriptor ad=getAliasDescriptor(schemaID,routineName,nameSpace);
                return ad==null?
                        Collections.EMPTY_LIST:
                        Collections.singletonList(ad);
            }
        }
        return list;
    }

    @Override
    protected void setDependencyManager(){
        SpliceLogUtils.trace(LOG,"Initializing the Splice Dependency Manager");
        this.dmgr=new SpliceDependencyManager(this);
    }

    @Override
    public void boot(boolean create,Properties startParams) throws StandardException{
        SpliceLogUtils.trace(LOG,"boot with create=%s,startParams=%s",create,startParams);
        DatabaseVersion databaseVersion=(new ManifestReader()).createVersion();
        if(!databaseVersion.isUnknown()){
            spliceSoftwareVersion=new Splice_DD_Version(this,databaseVersion.getMajorVersionNumber(),
                    databaseVersion.getMinorVersionNumber(),databaseVersion.getPatchVersionNumber(),
                    databaseVersion.getSprintVersionNumber());
        }
        if(create){
            SpliceAccessManager af=(SpliceAccessManager)Monitor.findServiceModule(this,AccessFactory.MODULE);
            SpliceTransactionManager txnManager=(SpliceTransactionManager)af.getTransaction(ContextService.getFactory().getCurrentContextManager());
            ((SpliceTransaction)txnManager.getRawTransaction()).elevate(Bytes.toBytes("boot"));
            if(spliceSoftwareVersion!=null){
                txnManager.setProperty(SPLICE_DATA_DICTIONARY_VERSION,spliceSoftwareVersion,true);
            }
        }

        super.boot(create,startParams);
    }

    @Override
    public boolean canSupport(Properties startParams){
        SpliceLogUtils.trace(LOG,"canSupport startParam=%s",startParams);
        return super.canSupport(startParams);
    }

    @Override
    public void startWriting(LanguageConnectionContext lcc) throws StandardException{
        startWriting(lcc,true);
    }

    @Override
    public void startWriting(LanguageConnectionContext lcc,boolean setDDMode) throws StandardException{
        lcc.setDataDictionaryWriteMode();
        elevateTxnForDictionaryOperations(lcc);
    }

    @Override
    public void getCurrentValueAndAdvance(String sequenceUUIDstring,NumberDataValue returnValue, boolean useBatch)
            throws StandardException{
        SpliceSequence sequence=getSpliceSequence(sequenceUUIDstring, useBatch);
        returnValue.setValue(sequence.getNext());
    }

    @Override
    public Long peekAtSequence(String schemaName,String sequenceName) throws StandardException {
        String sequenceUUIDstring=getSequenceID(schemaName, sequenceName);
        if(sequenceUUIDstring==null)
            throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION,"SEQUENCE",(schemaName+"."+sequenceName));

        SpliceSequence sequence=getSpliceSequence(sequenceUUIDstring, true);
        return sequence.peekAtCurrentValue();
    }

    private SpliceSequence getSpliceSequence(String sequenceUUIDstring, boolean useBatch)
        throws StandardException {
        try{
            if(sequenceRowLocationBytesMap==null){
                sequenceRowLocationBytesMap=new ConcurrentLinkedHashMap.Builder<String, byte[]>()
                        .maximumWeightedCapacity(512)
                        .concurrencyLevel(64)
                        .build();
            }

            if(sequenceDescriptorMap==null){
                sequenceDescriptorMap=new ConcurrentLinkedHashMap.Builder<String, SequenceDescriptor[]>()
                        .maximumWeightedCapacity(512)
                        .concurrencyLevel(64)
                        .build();
            }
            byte[] sequenceRowLocationBytes=sequenceRowLocationBytesMap.get(sequenceUUIDstring);
            SequenceDescriptor[] sequenceDescriptor=sequenceDescriptorMap.get(sequenceUUIDstring);
            if(sequenceRowLocationBytes==null || sequenceDescriptor==null){
                RowLocation[] rowLocation=new RowLocation[1];
                sequenceDescriptor=new SequenceDescriptor[1];

                LanguageConnectionContext llc=(LanguageConnectionContext)
                        ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);

                TransactionController tc=llc.getTransactionExecute();
                computeSequenceRowLocation(tc,sequenceUUIDstring,rowLocation,sequenceDescriptor);
                sequenceRowLocationBytes=rowLocation[0].getBytes();
                sequenceRowLocationBytesMap.put(sequenceUUIDstring,sequenceRowLocationBytes);
                sequenceDescriptorMap.put(sequenceUUIDstring,sequenceDescriptor);
            }

            long start=sequenceDescriptor[0].getStartValue();
            long increment=sequenceDescriptor[0].getIncrement();

            SIDriver siDriver =SIDriver.driver();
            PartitionFactory partFactory = siDriver.getTableFactory();
            TxnOperationFactory txnOpFactory = siDriver.getOperationFactory();
            return EngineDriver.driver().sequencePool().
                    get(new SequenceKey(sequenceRowLocationBytes,useBatch?SIDriver.driver().getConfiguration().getSequenceBlockSize():1l,start,increment,partFactory,txnOpFactory));
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
    public void createOrUpdateAllSystemProcedures(TransactionController tc) throws StandardException{
        tc.elevate("dictionary");
        super.createOrUpdateAllSystemProcedures(tc);
        SpliceLogUtils.info(LOG, "System procedures created or updated");
    }

    /*Table fetchers for Statistics tables*/
    private TabInfoImpl getPhysicalStatisticsTable() throws StandardException{
        if(physicalStatsTable==null){
            physicalStatsTable=new TabInfoImpl(new SYSPHYSICALSTATISTICSRowFactory(uuidFactory,exFactory,dvf,this));
        }
        initSystemIndexVariables(physicalStatsTable);
        return physicalStatsTable;
    }

    private TabInfoImpl getColumnStatisticsTable() throws StandardException{
        if(columnStatsTable==null){
            columnStatsTable=new TabInfoImpl(new SYSCOLUMNSTATISTICSRowFactory(uuidFactory,exFactory,dvf,this));
        }
        initSystemIndexVariables(columnStatsTable);
        return columnStatsTable;
    }

    private TabInfoImpl getTableStatisticsTable() throws StandardException{
        if(tableStatsTable==null){
            tableStatsTable=new TabInfoImpl(new SYSTABLESTATISTICSRowFactory(uuidFactory,exFactory,dvf,this));
        }
        initSystemIndexVariables(tableStatsTable);
        return tableStatsTable;
    }

    private TabInfoImpl getSourceCodeTable() throws StandardException{
        if(sourceCodeTable==null){
            sourceCodeTable=new TabInfoImpl(new SYSSOURCECODERowFactory(uuidFactory,exFactory,dvf,this));
        }
        initSystemIndexVariables(sourceCodeTable);
        return sourceCodeTable;
    }

    protected TabInfoImpl getPkTable() throws StandardException{
        if(pkTable==null){
            pkTable=new TabInfoImpl(new SYSPRIMARYKEYSRowFactory(uuidFactory,exFactory,dvf,this));
        }
        initSystemIndexVariables(pkTable);
        return pkTable;
    }

    private void upgradeIfNecessary(TransactionController tc) throws StandardException{

        boolean toUpgrade = Boolean.TRUE.equals(EngineLifecycleService.toUpgrade.get());
        // Only master can upgrade
        if (!toUpgrade) {
            return;
        }

        Splice_DD_Version catalogVersion=(Splice_DD_Version)tc.getProperty(SPLICE_DATA_DICTIONARY_VERSION);
        if(needToUpgrade(catalogVersion)){
            tc.elevate("dictionary");
            SpliceCatalogUpgradeScripts scripts=new SpliceCatalogUpgradeScripts(this,catalogVersion,tc);
            scripts.run();
            tc.setProperty(SPLICE_DATA_DICTIONARY_VERSION,spliceSoftwareVersion,true);
            tc.commit();
        }
    }

    public void upgradeSystablesFor260(TransactionController tc) throws StandardException {
        addNewColumToSystables(tc);
    }

    private void addNewColumToSystables(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSTABLESRowFactory.TABLENAME_STRING, sd, tc);
        ColumnDescriptor cd = td.getColumnDescriptor(SYSTABLESRowFactory.PURGE_DELETED_ROWS);
        if (cd == null)
        {
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence() + 1);
            // add the table descriptor with new name
            addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc, false);

            DataValueDescriptor storableDV = getDataValueFactory().getNullBoolean(null);
            int colNumber = td.getNumberOfColumns() + 1;
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.BOOLEAN));
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());
            UUID uuid = getUUIDFactory().createUUID();
            ColumnDescriptor columnDescriptor = new ColumnDescriptor(
                    SYSTABLESRowFactory.PURGE_DELETED_ROWS,
                    colNumber,
                    colNumber,
                    dtd,
                    new SQLBoolean(false),
                    null,
                    td,
                    uuid,
                    0,
                    0,
                    td.getColumnSequence());

            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);

            // now add the column to the tables column descriptor list.
            td.getColumnDescriptorList().add(columnDescriptor);

            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
            SpliceLogUtils.info(LOG, "SYS.SYSTABLES upgraded: added a new column %s.", SYSTABLESRowFactory.PURGE_DELETED_ROWS);
        }
    }

    public void upgradeSysStatsTableFor260(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSTABLESTATISTICSRowFactory.TABLENAME_STRING, sd, tc);
        ColumnDescriptor cd = td.getColumnDescriptor("SAMPLEFRACTION");
        if (cd == null) {
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence()+1);
            // add the table descriptor with new name
            addDescriptor(td,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);

            DataValueDescriptor storableDV;
            int colNumber;
            DataTypeDescriptor dtd;
            ColumnDescriptor columnDescriptor;
            UUID uuid = getUUIDFactory().createUUID();

            /**
             *  Add the column NUMPARTITIONS
             */
            if (td.getColumnDescriptor("NUMPARTITIONS") == null) {
                storableDV = getDataValueFactory().getNullLong(null);
                colNumber = SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS;
                dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.BIGINT));
                tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

                columnDescriptor = new ColumnDescriptor("NUMPARTITIONS",9,9,dtd,new SQLLongint(1),null,td,uuid,0,0,8);

                addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc,false);
                // now add the column to the tables column descriptor list.
                td.getColumnDescriptorList().add(columnDescriptor);
                updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
            }
            /**
             * Add the column STATSTYPE
             */
            storableDV = getDataValueFactory().getNullInteger(null);
            colNumber = SYSTABLESTATISTICSRowFactory.STATSTYPE;
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.INTEGER));
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

            columnDescriptor =  new ColumnDescriptor("STATSTYPE",10,10,dtd,new SQLInteger(0),null,td,uuid,0,0, 9);

            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);
            td.getColumnDescriptorList().add(columnDescriptor);
            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);

            /**
             * Add the column SAMPLEFRACTION
             */
            storableDV = getDataValueFactory().getNullDouble(null);
            colNumber = SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION;
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.DOUBLE));
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

            columnDescriptor =  new ColumnDescriptor("SAMPLEFRACTION",11,11,dtd,new SQLDouble(0),null,td,uuid,0,0,10);
            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);
            td.getColumnDescriptorList().add(columnDescriptor);

            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
            SpliceLogUtils.info(LOG, "SYS.SYSTABLESTATS upgraded: added columns: NUMPARTITIONS, STATSTYPE, SAMPLEFRACTION.");

            updateSysTableStatsView(tc);
        }
    }

    private void updateSysTableStatsView(TransactionController tc) throws StandardException{
        //drop table descriptor corresponding to the tablestats view and add
        SchemaDescriptor sd=getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor("SYSTABLESTATISTICS", sd, tc);
        dropTableDescriptor(td, sd, tc);
        td.setColumnSequence(td.getColumnSequence()+1);
        // add the table descriptor with new name
        addDescriptor(td,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);

        // add the two newly added columns statType and sampleFraction
        ColumnDescriptor columnDescriptor = new ColumnDescriptor("STATS_TYPE",10,10,DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),null,null,td,td.getUUID(),0,0,9);
        addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);
        td.getColumnDescriptorList().add(columnDescriptor);

        columnDescriptor = new ColumnDescriptor("SAMPLE_FRACTION",11,11,DataTypeDescriptor.getBuiltInDataTypeDescriptor((Types.DOUBLE)),null,null,td,td.getUUID(),0,0,10);
        addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);
        td.getColumnDescriptorList().add(columnDescriptor);

        ViewDescriptor vd=getViewDescriptor(td);
        dropViewDescriptor(vd, tc);
        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        vd=ddg.newViewDescriptor(td.getUUID(),"SYSTABLESTATISTICS",
                SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL,0,sd.getUUID());
        addDescriptor(vd,sd,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc,false);
        SpliceLogUtils.info(LOG, "SYS.SYSVIEWS upgraded: updated view SYSTABLESTATISTICS with two more columns: STATSTYPE, SAMPLEFRACTION.");
    }

    private boolean needToUpgrade(Splice_DD_Version catalogVersion){

        LOG.info(String.format("Splice Software Version = %s",(spliceSoftwareVersion==null?"null":spliceSoftwareVersion.toString())));
        LOG.info(String.format("Splice Catalog Version = %s",(catalogVersion==null?"null":catalogVersion.toString())));

        // Check if there is a manual override that is forcing an upgrade.
        // This flag should only be true for the master server.  If the upgrade runs on the region server,
        // it would probably be bad (at least if it ran concurrently with another upgrade).
        SConfiguration configuration=SIDriver.driver().getConfiguration();
        if(configuration.upgradeForced()) {
            LOG.info(String.format("Upgrade has been manually forced from version %s",
                    configuration.getUpgradeForcedFrom()));
            return true;
        }

        // Not sure about the current version, do not upgrade
        if(spliceSoftwareVersion==null){
            return false;
        }

        // This is a pre-Fuji catalog, upgrade it.
        if(catalogVersion==null){
            LOG.info("Upgrade needed since catalog version is null");
            return true;
        }

        // Compare software version and catalog version
        if(catalogVersion.toLong()<spliceSoftwareVersion.toLong()){
            LOG.info("Upgrade needed since catalog version < software version");
            return true;
        }
        return false;
    }


    private void addTableIfAbsent(TransactionController tc,SchemaDescriptor systemSchema,TabInfoImpl sysTableToAdd,
                                  ColumnOrdering[] columnOrder, String version) throws StandardException{
        if(getTableDescriptor(sysTableToAdd.getTableName(),systemSchema,tc)==null){
            SpliceLogUtils.trace(LOG,String.format("Creating system table %s.%s",systemSchema.getSchemaName(),sysTableToAdd.getTableName()));
            makeCatalog(sysTableToAdd,systemSchema,tc,columnOrder, version);
        }else{
            SpliceLogUtils.trace(LOG,String.format("Skipping table creation since system table %s.%s already exists",systemSchema.getSchemaName(),sysTableToAdd.getTableName()));
        }
    }

    private void createSysTableStatsView(TransactionController tc) throws StandardException{
        //create statistics views
        SchemaDescriptor sysSchema=sysViewSchemaDesc;

        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TableDescriptor view=ddg.newTableDescriptor("SYSTABLESTATISTICS",
                sysSchema,TableDescriptor.VIEW_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY,-1,null,null,null,null,null,null,false,false,null);
        addDescriptor(view,sysSchema,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);
        UUID viewId=view.getUUID();
        TabInfoImpl ti = getNonCoreTI(SYSTABLESTATS_CATALOG_NUM);
        CatalogRowFactory crf=ti.getCatalogRowFactory();
        ColumnDescriptor[] tableViewCds=crf.getViewColumns(view,viewId).get(0);
        addDescriptorArray(tableViewCds,view,DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        ColumnDescriptorList viewDl=view.getColumnDescriptorList();
        Collections.addAll(viewDl,tableViewCds);


        ViewDescriptor vd=ddg.newViewDescriptor(viewId,"SYSTABLESTATISTICS",
                SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL,0,sysSchema.getUUID());
        addDescriptor(vd,sysSchema,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc,false);
    }

    private void createSysColumnStatsView(TransactionController tc) throws StandardException{
        //create statistics views
        SchemaDescriptor sysSchema=sysViewSchemaDesc;

        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TableDescriptor view=ddg.newTableDescriptor("SYSCOLUMNSTATISTICS",
                sysSchema,TableDescriptor.VIEW_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY,-1,null,null,null,null,null,null,false,false,null);
        addDescriptor(view,sysSchema,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);
        UUID viewId=view.getUUID();
        TabInfoImpl ti = getNonCoreTI(SYSCOLUMNSTATS_CATALOG_NUM);
        CatalogRowFactory crf=ti.getCatalogRowFactory();
        ColumnDescriptor[] tableViewCds=crf.getViewColumns(view,viewId).get(0);
        addDescriptorArray(tableViewCds,view,DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        ColumnDescriptorList viewDl=view.getColumnDescriptorList();
        Collections.addAll(viewDl,tableViewCds);

        ViewDescriptor vd=ddg.newViewDescriptor(viewId,"SYSCOLUMNSTATISTICS",
                SYSCOLUMNSTATISTICSRowFactory.STATS_VIEW_SQL,0,sysSchema.getUUID());
        addDescriptor(vd,sysSchema,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc,false);
    }

    private void elevateTxnForDictionaryOperations(LanguageConnectionContext lcc) throws StandardException{
        BaseSpliceTransaction rawTransaction=((SpliceTransactionManager)lcc.getTransactionExecute()).getRawTransaction();
        if (rawTransaction instanceof SpliceTransactionView) // Already serde
            return;
        assert rawTransaction instanceof SpliceTransaction:
                "Programmer Error: Cannot perform a data dictionary write with a non-SpliceTransaction";
        // No subtransactions from here on, since we are modifying the data dictionary we rely on persisted
        // transactions for coordination
        ((SpliceTransaction) rawTransaction).getActiveStateTxn().forbidSubtransactions();
        /*
         * This is a bit of an awkward hack--at this stage, we need to ensure that the transaction
         * allows writes, but we don't really know where it's going, except to the data dictionary (and
         * therefore to system tables only)
         *
         * Thankfully, we only use the write-table transaction field to determine whether or not to
         * pause DDL operations, which can only occur against non-system tables. Since we are indicating
         * that this transaction will be writing to system tables, we don't have to worry about it.
         *
         * HOWEVER, it's possible that a transaction could modify both dictionary and non-dictionary tables.
         * In that situation, we don't want to confuse people with which table is being modified. So to do this,
         * we just only elevate the transaction if we absolutely have to.
         */
        if(rawTransaction.allowsWrites())
            return;
        SpliceTransaction txn=(SpliceTransaction)rawTransaction;
        txn.elevate(Bytes.toBytes("dictionary"));
    }

    @Override
    public boolean canWriteCache(TransactionController xactMgr) throws StandardException {
        // Only enable dictionary cache for region server.
        // TODO - enable it for master and spark executor
        if (!SpliceClient.isRegionServer) {
            SpliceLogUtils.debug(LOG, "Cannot use dictionary cache.");
            return false;
        }
        DDLDriver driver=DDLDriver.driver();
        if(driver==null) return false;
        DDLWatcher ddlWatcher=driver.ddlWatcher();
        if(xactMgr==null)
            xactMgr = getTransactionCompile();
        return ddlWatcher.canWriteCache((TransactionManager)xactMgr);
    }

    @Override
    public boolean canReadCache(TransactionController xactMgr) throws StandardException {
        // Only enable dictionary cache for region server.
        // TODO - enable it for master and spark executor
        if (!SpliceClient.isRegionServer) {
            SpliceLogUtils.debug(LOG, "Cannot use dictionary cache.");
            return false;
        }
        DDLDriver driver=DDLDriver.driver();
        if(driver==null) return false;
        DDLWatcher ddlWatcher=driver.ddlWatcher();
        if(xactMgr==null)
            xactMgr = getTransactionCompile();
        return ddlWatcher.canReadCache((TransactionManager)xactMgr);
    }

    @Override
    public boolean canUseSPSCache() throws StandardException {
        DDLDriver driver=DDLDriver.driver();
        if(driver==null) return false;
        DDLWatcher ddlWatcher=driver.ddlWatcher();
        return ddlWatcher.canUseSPSCache((TransactionManager)getTransactionCompile());
    }

    @Override
    public boolean canUseDependencyManager() {
        return !SpliceClient.isClient();
    }

    @Override
    public ColPermsDescriptor getColumnPermissions(UUID colPermsUUID) throws StandardException {
        Manager manager = EngineDriver.driver().manager();
            return manager.isEnabled()?manager.getColPermsManager().getColumnPermissions(this,colPermsUUID):null;
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
        Manager manager = EngineDriver.driver().manager();
        return manager.isEnabled()?manager.getColPermsManager().getColumnPermissions(this,tableUUID,privType,forGrant,authorizationId):null;
    } // end of getColumnPermissions

    public void upgradeSysSchemaPermsForModifySchemaPrivilege(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSSCHEMAPERMSRowFactory.SCHEMANAME_STRING, sd, tc);
        ColumnDescriptor cd = td.getColumnDescriptor("MODIFYPRIV");
        if (cd == null) {
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence()+1);
            // add the table descriptor with new name
            addDescriptor(td,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);

            ColumnDescriptor columnDescriptor;
            UUID uuid = getUUIDFactory().createUUID();

            /**
             *  Add the column MODIFYPRIV
             */
            DataValueDescriptor storableDV = getDataValueFactory().getNullChar(null);
            int colNumber = td.getNumberOfColumns() + 1;
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 1);
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

            columnDescriptor = new ColumnDescriptor(SYSSCHEMAPERMSRowFactory.MODIFYPRIV_COL_NAME,colNumber,
                    colNumber,dtd,null,null,td,uuid,0,0,td.getColumnSequence());

            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc,false);
            // now add the column to the tables column descriptor list.
            td.getColumnDescriptorList().add(columnDescriptor);
            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);

            SpliceLogUtils.info(LOG, "SYS.SYSSCHEMAPERMS upgraded: added columns: MODIFYPRIV.");
        }
    }

    public void upgradeSysRolesWithDefaultRoleColumn(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSROLESRowFactory.TABLENAME_STRING, sd, tc);
        ColumnDescriptor cd = td.getColumnDescriptor("DEFAULTROLE");

        // column already exists, no upgrade needed
        if (cd != null)
            return;

        /**
         * LOGIC below add the new column DEFAULTROLE to SYSROLES
         */
        tc.elevate("dictionary");
        dropTableDescriptor(td, sd, tc);
        td.setColumnSequence(td.getColumnSequence()+1);
        // add the table descriptor with new name
        addDescriptor(td,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);

        ColumnDescriptor columnDescriptor;
        UUID uuid = getUUIDFactory().createUUID();

        /**
         *  Add the column DEFAULTROLE
         */
        DataValueDescriptor storableDV = getDataValueFactory().getNullChar(null);
        int colNumber = td.getNumberOfColumns() + 1;
        DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 1);
        tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

        columnDescriptor = new ColumnDescriptor("DEFAULTROLE",colNumber,
                colNumber,dtd,null,null,td,uuid,0,0,td.getColumnSequence());

        addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc,false);
        // now add the column to the tables column descriptor list.
        td.getColumnDescriptorList().add(columnDescriptor);
        updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);

        /**
         * LOGIC below create and populuate index on (GRANTEE, DEFAULTROLE)
         */
        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TabInfoImpl ti = getNonCoreTIByNumber(SYSROLES_CATALOG_NUM);
        {
            ConglomerateDescriptor[] cds=td.getConglomerateDescriptors();

			/* Init the heap conglomerate here */
            for(ConglomerateDescriptor conglomerateDescriptor : cds){

                if(!conglomerateDescriptor.isIndex()){
                    ti.setHeapConglomerate(conglomerateDescriptor.getConglomerateNumber());
                    break;
                }
            }
        }
        ConglomerateDescriptor cgd = bootstrapOneIndex(systemSchemaDesc, tc, ddg, ti, SYSROLESRowFactory.SYSROLES_INDEX_EE_DEFAULT_IDX, ti.getHeapConglomerate());
        addDescriptor(cgd,sd,SYSCONGLOMERATES_CATALOG_NUM,false,tc,false);

        /* purge td dictionary cache as it may have the sysrole td without the new index info */
        dataDictionaryCache.clearNameTdCache();
        dataDictionaryCache.clearOidTdCache();

        // scan the sysroles table
        SYSROLESRowFactory rf=(SYSROLESRowFactory)ti.getCatalogRowFactory();
        ExecRow outRow = rf.makeEmptyRow();
        ScanController scanController=tc.openScan(
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

        int batch = 1024;
        ExecRow[] rowList = new ExecRow[batch];
        RowLocation[] rowLocationList = new RowLocation[batch];

        try{
            int i = 0;
            while(scanController.fetchNext(outRow.getRowArray())){
                rowList[i%batch] = outRow.getClone();
                rowLocationList[i%batch] = scanController.newRowLocationTemplate();
                scanController.fetchLocation(rowLocationList[i%batch]);
                i++;
                if (i % batch == 0) {
                    ti.insertIndexRowListImpl(rowList, rowLocationList, tc, SYSROLESRowFactory.SYSROLES_INDEX_EE_DEFAULT_IDX, batch);
                }
            }
            // insert last batch
            if (i % batch > 0)
                ti.insertIndexRowListImpl(rowList, rowLocationList, tc, SYSROLESRowFactory.SYSROLES_INDEX_EE_DEFAULT_IDX, i%batch);
        }finally{
            scanController.close();
        }

        // reset TI for sysroles in NonCoreTI array, as we only used the 4th index here, so inforamtion about the other
        // 3 indexes is not fully populated. This TI should not be reused for future operations
        clearNoncoreTable(SYSROLES_CATALOG_NUM-NUM_CORE);
        SpliceLogUtils.info(LOG, "SYS.SYSROLES upgraded: added columns: DEFAULTROLE; added index on (GRANTEE, DEFAULTROLE)");
    }

    // remove rows in sysroutineperms whose aliasid are no longer valid
    public void cleanSysRoutinePerms(TransactionController tc) throws StandardException {
        // scan the sysroutineperms table
        TabInfoImpl ti = getNonCoreTI(SYSROUTINEPERMS_CATALOG_NUM);
        SYSROUTINEPERMSRowFactory rf=(SYSROUTINEPERMSRowFactory)ti.getCatalogRowFactory();
        ExecRow outRow = rf.makeEmptyRow();
        ScanController scanController=tc.openScan(
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

        List<RoutinePermsDescriptor> listToDelete = new ArrayList<>();

        try{
            while(scanController.fetchNext(outRow.getRowArray())){
                String aliasUUIDString = outRow.getColumn(SYSROUTINEPERMSRowFactory.ALIASID_COL_NUM).getString();
                UUID aliasUUID = getUUIDFactory().recreateUUID(aliasUUIDString);
                RoutinePermsDescriptor permsDescriptor = (RoutinePermsDescriptor)rf.buildDescriptor(outRow, null, this);
                // we have looked up the sysaliases table when building the permsDescriptor above,
                // if the aliasid does not exist, routineName is null
                String ad = permsDescriptor.getRoutineName();
                if (ad == null) {
                    listToDelete.add(permsDescriptor);
                }
            }
        }finally{
            scanController.close();
        }

        // delete the obselete rows
        for (RoutinePermsDescriptor permsDescriptor: listToDelete) {
            addRemovePermissionsDescriptor(false, permsDescriptor, permsDescriptor.getGrantee(), tc);
        }
        
        SpliceLogUtils.info(LOG, "SYS.SYSROUTINEPERMS upgraded: obsolete rows deleted");
    }

    public void removeFKDependencyOnPrivileges(TransactionController tc) throws StandardException {
        // scan the sysdepends table
        TabInfoImpl ti = getNonCoreTI(SYSDEPENDS_CATALOG_NUM);
        SYSDEPENDSRowFactory rf=(SYSDEPENDSRowFactory)ti.getCatalogRowFactory();
        ExecRow outRow = rf.makeEmptyRow();
        ScanController scanController=tc.openScan(
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

        List<RowLocation> rowsToDelete = new ArrayList<>();

        try{
            while(scanController.fetchNext(outRow.getRowArray())){
                RowLocation rowLocation = scanController.newRowLocationTemplate();
                scanController.fetchLocation(rowLocation);
                DependencyDescriptor dependencyDescriptor=(DependencyDescriptor)rf.buildDescriptor(outRow,null, this);
                // check if the dependencyDescriptors are the ones that we want
                DependableFinder finder = dependencyDescriptor.getDependentFinder();
                if (finder == null)
                    continue;
                String dependentObjectType = finder.getSQLObjectType();
                if (dependentObjectType == null || !dependentObjectType.equals(Dependable.CONSTRAINT))
                    continue;

                Dependent tempD = (Dependent) finder.getDependable(this, dependencyDescriptor.getUUID());
                if (tempD == null || !(tempD instanceof ForeignKeyConstraintDescriptor))
                    continue;

                // is the provider a role definition, or a permission descriptor
                finder = dependencyDescriptor.getProviderFinder();
                String objectType = finder == null? "":finder.getSQLObjectType();
                if (objectType.equals(Dependable.ROLE_GRANT) ||
                    objectType.equals(Dependable.SCHEMA_PERMISSION) ||
                    objectType.equals(Dependable.TABLE_PERMISSION) ||
                    objectType.equals(Dependable.COLUMNS_PERMISSION)) {
                        rowsToDelete.add(rowLocation);
                }
            }
        }finally{
            scanController.close();
        }

        // delete the unwanted dependency rows
        for (RowLocation rowLocation: rowsToDelete) {
            ti.deleteRowBasedOnRowLocation(tc, rowLocation, true, null);
        }

        SpliceLogUtils.info(LOG,
                "SYS.SYSDEPENDS updated: Foreign keys dependencies on RoleDescriptors or permission descriptors deleted, total rows deleted: " + rowsToDelete.size());
    }

    public void upgradeSysColumnsWithUseExtrapolationColumn(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSCOLUMNSRowFactory.TABLENAME_STRING, sd, tc);
        ColumnDescriptor cd = td.getColumnDescriptor("USEEXTRAPOLATION");
        if (cd == null) {
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence()+1);
            // add the table descriptor with new name
            addDescriptor(td,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);

            ColumnDescriptor columnDescriptor;
            UUID uuid = getUUIDFactory().createUUID();

            /**
             *  Add the column USEEXTRAPOLATION
             */
            DataValueDescriptor storableDV = getDataValueFactory().getNullByte(null);
            int colNumber = SYSCOLUMNSRowFactory.SYSCOLUMNS_USEEXTRAPOLATION;
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TINYINT);
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

            columnDescriptor = new ColumnDescriptor("USEEXTRAPOLATION",colNumber,
                    colNumber,dtd,null,null,td,uuid,0,0,td.getColumnSequence());

            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc,false);
            // now add the column to the tables column descriptor list.
            td.getColumnDescriptorList().add(columnDescriptor);
            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);

            SpliceLogUtils.info(LOG, "SYS.SYSCOLUMNS upgraded: added column: USEEXTRAPOLATION.");
        }
    }

    public void removeUnusedBackupTables(TransactionController tc) throws StandardException {
        dropUnusedBackupTable("SYSBACKUPFILESET", tc);
        dropUnusedBackupTable("SYSBACKUPJOBS", tc);
    }

    public void removeUnusedBackupProcedures(TransactionController tc) throws StandardException {
        AliasDescriptor ad = getAliasDescriptor(SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
                "SYSCS_SCHEDULE_DAILY_BACKUP", AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR);
        if (ad != null) {
            dropAliasDescriptor(ad, tc);
            SpliceLogUtils.info(LOG, "Dropped system procedure SYSCS_UTIL.SYSCS_SCHEDULE_DAILY_BACKUP");
        }

        ad = getAliasDescriptor(SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
                "SYSCS_CANCEL_DAILY_BACKUP", AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR);
        if (ad != null) {
            dropAliasDescriptor(ad, tc);
            SpliceLogUtils.info(LOG, "Dropped system procedure SYSCS_UTIL.SYSCS_CANCEL_DAILY_BACKUP");
        }
    }

    private void dropUnusedBackupTable(String tableName, TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(tableName, sd, tc);
        SpliceTransactionManager sm = (SpliceTransactionManager) tc;
        if (td != null) {
            UUID tableId = td.getUUID();

            // Drop column descriptors
            dropAllColumnDescriptors(tableId, tc);

            long heapId = td.getHeapConglomerateId();

            /*
             * Drop all the conglomerates.  Drop the heap last, because the
             * store needs it for locking the indexes when they are dropped.
             */
            ConglomerateDescriptor[] cds = td.getConglomerateDescriptors();
            for (ConglomerateDescriptor cd : cds) {
                // Remove Statistics
                deletePartitionStatistics(cd.getConglomerateNumber(), tc);

                // Drop index conglomerates
                if (cd.getConglomerateNumber() != heapId) {
                    Conglomerate conglomerate = sm.findConglomerate(cd.getConglomerateNumber());
                    conglomerate.drop(sm);
                }
            }
            // Drop the conglomerate descriptors
            dropAllConglomerateDescriptors(td, tc);

            // Drop table descriptors
            dropTableDescriptor(td, sd, tc);

            // Drop base table conglomerate
            Conglomerate conglomerate = sm.findConglomerate(heapId);
            conglomerate.drop(sm);

            SpliceLogUtils.info(LOG, "Dropped table %s", tableName);
        }
    }

    public void upgradeSysSchemaPermsForAccessSchemaPrivilege(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSSCHEMAPERMSRowFactory.SCHEMANAME_STRING, sd, tc);
        ColumnDescriptor cd = td.getColumnDescriptor(SYSSCHEMAPERMSRowFactory.ACCESSPRIV_COL_NAME);
        if (cd == null) {
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence() + 1);
            // add the table descriptor with new name
            addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc, false);

            ColumnDescriptor columnDescriptor;
            UUID uuid = getUUIDFactory().createUUID();

            /**
             *  Add the column ACCESSPRIV
             */
            DataValueDescriptor storableDV = getDataValueFactory().getNullChar(null);
            int colNumber = td.getNumberOfColumns() + 1;
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 1);
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

            columnDescriptor = new ColumnDescriptor(SYSSCHEMAPERMSRowFactory.ACCESSPRIV_COL_NAME, colNumber,
                    colNumber, dtd, null, null, td, uuid, 0, 0, td.getColumnSequence());

            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);
            // now add the column to the tables column descriptor list.
            td.getColumnDescriptorList().add(columnDescriptor);
            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);

            SpliceLogUtils.info(LOG, "SYS.SYSSCHEMAPERMS upgraded: added columns: ACCESSPRIV.");
        }
    }

    @Override
    public TablePermsDescriptor getTablePermissions(UUID tableUUID,String authorizationId) throws StandardException{
        TablePermsDescriptor key=new TablePermsDescriptor(this,authorizationId,null,tableUUID);
        return (TablePermsDescriptor)getPermissions(key, metadataAccessRestrictionEnabled);
    } // end of getTablePermissions

    @Override
    public SchemaPermsDescriptor getSchemaPermissions(UUID schemaPermsUUID, String authorizationId) throws StandardException{
        SchemaPermsDescriptor key=new SchemaPermsDescriptor(this,authorizationId,null,schemaPermsUUID);
        return (SchemaPermsDescriptor)getPermissions(key, metadataAccessRestrictionEnabled);
    }

    public RoutinePermsDescriptor getRoutinePermissions(UUID routineUUID,String authorizationId) throws StandardException{
        RoutinePermsDescriptor key=new RoutinePermsDescriptor(this,authorizationId,null,routineUUID);

        return (RoutinePermsDescriptor)getPermissions(key, metadataAccessRestrictionEnabled);
    } // end of getRoutinePermissions

    @Override
    public PermDescriptor getGenericPermissions(UUID objectUUID,
                                                String objectType,
                                                String privilege,
                                                String granteeAuthId) throws StandardException{
        PermDescriptor key=new PermDescriptor(this,null,objectType,objectUUID,privilege,null,granteeAuthId,false);

        return (PermDescriptor)getPermissions(key, metadataAccessRestrictionEnabled);
    }

    @Override
    public boolean isMetadataAccessRestrictionEnabled() {
        return metadataAccessRestrictionEnabled;
    }

    @Override
    public void setMetadataAccessRestrictionEnabled() {
        String metadataRestriction =
                SIDriver.driver().getConfiguration().getMetadataRestrictionEnabled();
        metadataAccessRestrictionEnabled =
                metadataRestriction.equals(SQLConfiguration.METADATA_RESTRICTION_NATIVE) ||
                        metadataRestriction.equals(SQLConfiguration.METADATA_RESTRICTION_RANGER);
        SpliceLogUtils.info(LOG,"metadataAccessRestritionEnabled=%s",metadataRestriction);
    }

    @Override
    public void updateSystemSchemasView(TransactionController tc) throws StandardException {
        boolean toUpgrade = Boolean.TRUE.equals(EngineLifecycleService.toUpgrade.get());
        // Only master can upgrade
        if (!toUpgrade) {
            return;
        }

        tc.commit();

        SchemaDescriptor sysVWSchema=sysViewSchemaDesc;
        tc.elevate("dictionary");

        SConfiguration configuration=SIDriver.driver().getConfiguration();

        String metadataRestrictionEnabled = configuration.getMetadataRestrictionEnabled();

        // check sysschemasview
        TableDescriptor td = getTableDescriptor("SYSSCHEMASVIEW", sysVWSchema, tc);
        String schemaViewSQL = getSchemaViewSQL();

        if (td != null) {
            ViewDescriptor vd = getViewDescriptor(td);
            boolean needUpdate = !vd.getViewText().equals(schemaViewSQL);

            // view definition matches the setting, no update needed
            if (!needUpdate) return;

            // drop the view deifnition
            dropAllColumnDescriptors(td.getUUID(), tc);
            dropViewDescriptor(vd, tc);
            dropTableDescriptor(td, sysVWSchema, tc);

        }

        // add new view deifnition
        createOneSystemView(tc, SYSSCHEMAS_CATALOG_NUM, "SYSSCHEMASVIEW", 0, sysVWSchema, schemaViewSQL);

        // we need to re-generate the metadataSPS due to the definition change of sysschemasview
        updateMetadataSPSes(tc);

        tc.commit();
        SpliceLogUtils.info(LOG, "SYSVW.SYSSCHEMAVIEW updated to " + metadataRestrictionEnabled);
    }

    public void updateSystemViewForSysConglomerates(TransactionController tc) throws StandardException {
        tc.elevate("dictionary");
        SchemaDescriptor sysVWSchema=sysViewSchemaDesc;

        // check sysconglomerateinschemas view
        TableDescriptor td = getTableDescriptor("SYSCONGLOMERATEINSCHEMAS", sysVWSchema, tc);
        if (td != null) {
            ViewDescriptor vd = getViewDescriptor(td);
            boolean needUpdate = !vd.getViewText().equals(SYSCONGLOMERATESRowFactory.SYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL);

            // view definition matches the setting, no update needed
            if (!needUpdate) return;

            // drop the view deifnition
            dropAllColumnDescriptors(td.getUUID(), tc);
            dropViewDescriptor(vd, tc);
            dropTableDescriptor(td, sysVWSchema, tc);

        }

        // add new view deifnition
        createOneSystemView(tc, SYSCONGLOMERATES_CATALOG_NUM, "SYSCONGLOMERATEINSCHEMAS", 0, sysVWSchema, SYSCONGLOMERATESRowFactory.SYSCONGLOMERATE_IN_SCHEMAS_VIEW_SQL);

        SpliceLogUtils.info(LOG, "SYSVW.SYSCONGLOMERATEINSCHEMAS is updated!");
    }

    public void createPermissionTableSystemViews(TransactionController tc) throws StandardException {
        tc.elevate("dictionary");
        //Add the SYSVW schema if it does not exists
        if (getSchemaDescriptor(SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME, tc, false) == null) {
            SpliceLogUtils.info(LOG, "SYSVW does not exist, system views for permission tables are not created!");
            return;
        }

        SchemaDescriptor sysVWSchema=sysViewSchemaDesc;

        // create systablepermsView
        createOneSystemView(tc, SYSTABLEPERMS_CATALOG_NUM, "SYSTABLEPERMSVIEW", 0, sysVWSchema, SYSTABLEPERMSRowFactory.SYSTABLEPERMS_VIEW_SQL);

        // create sysschemapermsView
        createOneSystemView(tc, SYSSCHEMAPERMS_CATALOG_NUM, "SYSSCHEMAPERMSVIEW", 0, sysVWSchema, SYSSCHEMAPERMSRowFactory.SYSSCHEMAPERMS_VIEW_SQL);

        // create syscolpermsView
        createOneSystemView(tc, SYSCOLPERMS_CATALOG_NUM, "SYSCOLPERMSVIEW", 0, sysVWSchema, SYSCOLPERMSRowFactory.SYSCOLPERMS_VIEW_SQL);

        // create sysroutinepermsView
        createOneSystemView(tc, SYSROUTINEPERMS_CATALOG_NUM, "SYSROUTINEPERMSVIEW", 0, sysVWSchema, SYSROUTINEPERMSRowFactory.SYSROUTINEPERMS_VIEW_SQL);

        // create syspermsView
        createOneSystemView(tc, SYSPERMS_CATALOG_NUM, "SYSPERMSVIEW", 0, sysVWSchema, SYSPERMSRowFactory.SYSPERMS_VIEW_SQL);


        SpliceLogUtils.info(LOG, "System Views for permission tables created in SYSVW!");
    }

    public void removeUnusedIndexInSysFiles(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor("SYSFILES", sd, tc);
        ConglomerateDescriptor cd = td.getConglomerateDescriptor(new BasicUUID("80000000-00d3-e222-be7c-000a0a0b1900"));

        if (cd != null) {
            tc.elevate("dictionary");
            dropConglomerateDescriptor(cd,tc);

            SpliceLogUtils.info(LOG, "Dropped index %s", "SYSFILES_INDEX3");
        }
    }

    public void createAliasToTableSystemView(TransactionController tc) throws StandardException {
        tc.elevate("dictionary");
        //Add the SYSVW schema if it does not exists
        if (getSchemaDescriptor(SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME, tc, false) == null) {
            SpliceLogUtils.info(LOG, "SYSVW does not exist, system views for permission tables are not created!");
            return;
        }

        SchemaDescriptor sysVWSchema=sysViewSchemaDesc;

        // create sysaliastotableview
        createOneSystemView(tc, SYSALIASES_CATALOG_NUM, "SYSALIASTOTABLEVIEW", 0, sysVWSchema, SYSALIASESRowFactory.SYSALIAS_TO_TABLE_VIEW_SQL);

        SpliceLogUtils.info(LOG, "System View SYSALIASTOTABLEVIEW created in SYSVW!");
    }

    public void addCatalogVersion(TransactionController tc) throws StandardException{
        for (int i = 0; i < coreInfo.length; ++i) {
            long conglomerateId = coreInfo[i].getHeapConglomerate();
            tc.setCatalogVersion(conglomerateId, catalogVersions.get(i));
        }

        for (int i = 0; i < noncoreInfo.length; ++i) {
            long conglomerateId = getNonCoreTI(i+NUM_CORE).getHeapConglomerate();
            tc.setCatalogVersion(conglomerateId, catalogVersions.get(i + NUM_CORE));

        }
    }

    public void addMinRetentionPeriodColumn(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        TableDescriptor td = getTableDescriptor(SYSTABLESRowFactory.TABLENAME_STRING, sd, tc);
        ColumnDescriptor cd = td.getColumnDescriptor(SYSTABLESRowFactory.MIN_RETENTION_PERIOD);
        if (cd == null) { // needs updating
            tc.elevate("dictionary");
            dropTableDescriptor(td, sd, tc);
            td.setColumnSequence(td.getColumnSequence() + 1);
            // add the table descriptor with new name
            addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc, false);

            ColumnDescriptor columnDescriptor;
            UUID uuid = getUUIDFactory().createUUID();

            // Add the column MIN_RETENTION_PERIOD
            DataValueDescriptor storableDV = getDataValueFactory().getNullLong(null);
            int colNumber = td.getNumberOfColumns() + 1;
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, 1);
            tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV, dtd.getCollationType());

            columnDescriptor = new ColumnDescriptor(SYSTABLESRowFactory.MIN_RETENTION_PERIOD, colNumber,
                    colNumber, dtd, null, null, td, uuid, 0, 0, td.getColumnSequence());

            addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);

            // now add the column to the table's column descriptor list.
            td.getColumnDescriptorList().add(columnDescriptor);
            updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);

            SpliceLogUtils.info(LOG, String.format("%s upgraded: added a column: %s.", SYSTABLESRowFactory.TABLENAME_STRING,
                    SYSTABLESRowFactory.MIN_RETENTION_PERIOD));

            // now upgrade the views if necessary
            TableDescriptor td1 = getTableDescriptor(SYSTABLESRowFactory.SYSTABLE_VIEW_NAME, sysViewSchemaDesc, tc);
            if (td1 != null) {
                ViewDescriptor vd1 = getViewDescriptor(td1);
                dropAllColumnDescriptors(td1.getUUID(), tc);
                dropViewDescriptor(vd1, tc);
                dropTableDescriptor(td1, sysViewSchemaDesc, tc);
            }
            createOneSystemView(tc, SYSTABLES_CATALOG_NUM, SYSTABLESRowFactory.SYSTABLE_VIEW_NAME, 0,
                    sysViewSchemaDesc, SYSTABLESRowFactory.SYSTABLE_VIEW_SQL);
            SpliceLogUtils.info(LOG, String.format("%s upgraded: added a column: %s.", SYSTABLESRowFactory.SYSTABLE_VIEW_NAME,
                    SYSTABLESRowFactory.MIN_RETENTION_PERIOD));
        }
    }

    @Override
    public boolean useTxnAwareCache() {
        return !SpliceClient.isRegionServer;
    }
}
