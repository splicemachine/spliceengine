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
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.configuration.SIConfigurations;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.SynonymAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionContext;
import com.splicemachine.db.iapi.sql.execute.ScanQualifier;
import com.splicemachine.db.iapi.store.access.*;
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
import com.splicemachine.derby.impl.store.access.hbase.HBaseController;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.management.Manager;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.tools.version.ManifestReader;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

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
    private volatile TabInfoImpl naturalNumbersTable = null;
    private volatile TabInfoImpl ibmConnectionTable = null;
    private volatile TabInfoImpl databaseTable = null;
    private Splice_DD_Version spliceSoftwareVersion;
    protected boolean metadataAccessRestrictionEnabled;

    public static final String SPLICE_DATA_DICTIONARY_VERSION="SpliceDataDictionaryVersion";
    private ConcurrentLinkedHashMap<String, byte[]> sequenceRowLocationBytesMap=null;
    private ConcurrentLinkedHashMap<String, SequenceDescriptor[]> sequenceDescriptorMap=null;

    public static final SystemViewDefinitions viewDefinitions = new SystemViewDefinitions();

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

    String getSchemaViewSQL() {
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
        if (getSchemaDescriptor(spliceDbDesc.getUUID(), SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME, tc, false) == null) {
            sysViewSchemaDesc = addSystemSchema(SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME, SchemaDescriptor.SYSVW_SCHEMA_UUID, spliceDbDesc, tc);
        }

        createOrUpdateSystemView(tc, "SYSVW", "SYSSEQUENCESVIEW");
        createOrUpdateSystemView(tc, "SYSVW", "SYSALLROLES");
        createOrUpdateSystemView(tc, "SYSVW", "SYSSCHEMASVIEW");
        createOrUpdateSystemView(tc, "SYSVW", "SYSCONGLOMERATEINSCHEMAS");
        createOrUpdateSystemView(tc, "SYSVW", "SYSTABLESVIEW");
        createOrUpdateSystemView(tc, "SYSVW", "SYSCOLUMNSVIEW");
        createOrUpdateSystemView(tc, "SYSVW", "SYSDATABASESVIEW");

        SpliceLogUtils.info(LOG, "Views in SYSVW created!");
    }

    public void createTableColumnViewInSysIBM(TransactionController tc) throws StandardException {
        createOrUpdateSystemView(tc, "SYSIBM", "SYSCOLUMNS");
        createOrUpdateSystemView(tc, "SYSIBM", "SYSTABLES");
    }

    public void createKeyColumnUseViewInSysIBM(TransactionController tc) throws StandardException {
        createOrUpdateSystemView(tc, "SYSIBM", "SYSKEYCOLUSE");
    }

    public void createIndexColumnUseViewInSysCat(TransactionController tc) throws StandardException {
        String viewName = "INDEXCOLUSE";
        createOrUpdateSystemView(tc, "SYSCAT", viewName);

        // create an synonym SYSIBM.SYSINDEXCOLUSE for SYSCAT.INDEXCOLUSE
        String synonymName = "SYSINDEXCOLUSE";
        TableDescriptor synonymTD = getTableDescriptor(synonymName, sysIBMSchemaDesc, tc);
        if (synonymTD == null)
        {
            // To prevent any possible deadlocks with SYSTABLES, we insert a row into
            // SYSTABLES also for synonyms. This also ensures tables/views/synonyms share
            // same namespace
            DataDescriptorGenerator ddg = getDataDescriptorGenerator();
            TableDescriptor td = ddg.newTableDescriptor(synonymName, sysIBMSchemaDesc, TableDescriptor.SYNONYM_TYPE,
                    TableDescriptor.DEFAULT_LOCK_GRANULARITY,-1,
                    null,null,null,null,null,null,false,false,null);
            addDescriptor(td, sysIBMSchemaDesc, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc, false);

            // Create a new alias descriptor with a UUID filled in.
            UUID synonymID = getUUIDFactory().createUUID();
            AliasDescriptor ads = new AliasDescriptor(this, synonymID,
                    synonymName,
                    sysIBMSchemaDesc.getUUID(),
                    null,
                    AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR,
                    AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR,
                    true,
                    new SynonymAliasInfo(sysCatSchemaDesc.getSchemaName(), viewName),
                    null, tc);
            addDescriptor(ads, null, DataDictionary.SYSALIASES_CATALOG_NUM,
                    false, tc, false);

            SpliceLogUtils.info(LOG, "SYSIBM." + synonymName + " is created as an alias of SYSCAT." + viewName + "!");
        }
    }

    public void createReferencesViewInSysCat(TransactionController tc) throws StandardException {
        createOrUpdateSystemView(tc, "SYSCAT", "REFERENCES");
    }

    public void createSysIndexesViewInSysIBM(TransactionController tc) throws StandardException {
        createOrUpdateSystemView(tc, "SYSIBM", "SYSINDEXES");
    }

    // SYSCAT.COLUMNS view must be created after SYSIBM.SYSCOLUMNS because it's defined on top of SYSCOLUMNS view
    public void createColumnsViewInSysCat(TransactionController tc) throws StandardException {
        createOrUpdateSystemView(tc, "SYSCAT", "COLUMNS");
    }

    private TabInfoImpl getNaturalNumbersTable() throws StandardException{
        if(naturalNumbersTable==null){
            naturalNumbersTable=new TabInfoImpl(new SYSNATURALNUMBERSRowFactory(uuidFactory,exFactory,dvf, this));
        }
        initSystemIndexVariables(naturalNumbersTable);
        return naturalNumbersTable;
    }

    /**
     * Populate SYSNATURALNUMBERS table with 1-2048.
     *
     * @throws StandardException Standard Derby error policy
     */
    private void populateSYSNATURALNUMBERS(TransactionController tc) throws StandardException{
        SYSNATURALNUMBERSRowFactory.populateSYSNATURALNUMBERS(getNonCoreTI(SYSNATURALNUMBERS_CATALOG_NUM), tc);
    }

    public void createNaturalNumbersTable(TransactionController tc) throws StandardException {
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();

        TabInfoImpl table=getNaturalNumbersTable();
        String catalogVersion = DataDictionary.catalogVersions.get(SYSNATURALNUMBERS_CATALOG_NUM);
        addTableIfAbsent(tc,systemSchema,table,null, catalogVersion);

        populateSYSNATURALNUMBERS(tc);
    }

    public void updateNaturalNumbersTable(TransactionController tc) throws StandardException {
        SchemaDescriptor sd = getSystemSchemaDescriptor();
        tc.elevate("dictionary");

        TableDescriptor td = getTableDescriptor("SYSNATURALNUMBERS", sd, tc);
        if (td == null) {
            createNaturalNumbersTable(tc);
        }
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
        if (getSchemaDescriptor(spliceDbDesc.getUUID(), SchemaDescriptor.IBM_SYSTEM_ADM_SCHEMA_NAME, tc, false) == null) {
            sysIBMADMSchemaDesc=addSystemSchema(SchemaDescriptor.IBM_SYSTEM_ADM_SCHEMA_NAME, SchemaDescriptor.SYSIBMADM_SCHEMA_UUID, spliceDbDesc, tc);
        }

        TabInfoImpl connectionTableInfo=getIBMADMConnectionTable();
        addTableIfAbsent(tc,sysIBMADMSchemaDesc,connectionTableInfo,null, null);

        createOrUpdateSystemView(tc, "SYSIBMADM", "SNAPAPPL");
        createOrUpdateSystemView(tc, "SYSIBMADM", "SNAPAPPL_INFO");
        createOrUpdateSystemView(tc, "SYSIBMADM", "APPLICATIONS");

        SpliceLogUtils.info(LOG, "Tables and views in SYSIBMADM are created!");
    }

    private TabInfoImpl getDatabaseTable() throws StandardException {
        if (databaseTable == null) {
            databaseTable = new TabInfoImpl(new SYSDATABASESRowFactory(uuidFactory, exFactory, dvf, this));
        }
        initSystemIndexVariables(databaseTable);
        return databaseTable;
    }

    public void createSysDatabasesTableAndAddDatabaseIdColumnsToSysTables(TransactionController tc, Properties params) throws StandardException {
        tc.elevate("dictionary");

        DataDescriptorGenerator ddg = getDataDescriptorGenerator();

        // Create SYS.SYSDATABASES
        if(getTableDescriptor(SYSDATABASESRowFactory.TABLENAME_STRING, systemSchemaDesc,tc) == null) { // XXX This will not work because the index is not reset yet
            ExecutionContext ec = (ExecutionContext) ContextService.getContext(ExecutionContext.CONTEXT_ID);
            new CoreCreation(SYSDATABASES_CATALOG_NUM, tc, ec).run();
            if (coreInfo[SYSDATABASES_CATALOG_NUM].getNumberOfIndexes() > 0) {
                TabInfoImpl ti = coreInfo[SYSDATABASES_CATALOG_NUM];
                bootStrapSystemIndexes(systemSchemaDesc, tc, ddg, ti);
            }

            TabInfoImpl ti = coreInfo[SYSDATABASES_CATALOG_NUM];
            addSystemTableToDictionary(ti, systemSchemaDesc, tc, ddg);

            params.put(CFG_SYSDATABASES_ID, Long.toString(coreInfo[SYSDATABASES_CORE_NUM].getHeapConglomerate()));
            params.put(CFG_SYSDATABASES_INDEX1_ID,
                    Long.toString(
                            coreInfo[SYSDATABASES_CORE_NUM].getIndexConglomerate(
                                    SYSDATABASESRowFactory.SYSDATABASES_INDEX1_ID)));
            params.put(CFG_SYSDATABASES_INDEX2_ID,
                    Long.toString(
                            coreInfo[SYSDATABASES_CORE_NUM].getIndexConglomerate(
                                    SYSDATABASESRowFactory.SYSDATABASES_INDEX2_ID)));
        }

        // Add first row sysdatabases
        UUID databaseID = (UUID) tc.getProperty(DataDictionary.DATABASE_ID);
        String owner = sysIBMSchemaDesc.getAuthorizationId();
        spliceDbDesc = new DatabaseDescriptor(this, DatabaseDescriptor.STD_DB_NAME, owner, databaseID);
        addDescriptor(spliceDbDesc, null, SYSDATABASES_CATALOG_NUM, false, tc, false);

        // Add new databaseid columns to relevant system tables
        ExecRow templateRow = getExecutionFactory().getValueRow(SYSSCHEMASRowFactory.SYSSCHEMAS_COLUMN_COUNT);
        templateRow.setColumn(4, new SQLChar(databaseID.toString()));
        upgradeAddIndexedColumnToSystemTable(
                tc, SYSSCHEMAS_CATALOG_NUM,
                new int[]{4},
                templateRow,
                new int[]{SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID});

        params.put(CFG_SYSSCHEMAS_INDEX1_ID,
                Long.toString(
                        coreInfo[SYSSCHEMAS_CORE_NUM].getIndexConglomerate(
                                SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID)));

        templateRow = getExecutionFactory().getValueRow(SYSROLESRowFactory.SYSROLES_COLUMN_COUNT);
        templateRow.setColumn(8, new SQLChar(databaseID.toString()));
        upgradeAddIndexedColumnToSystemTable(
                tc, SYSROLES_CATALOG_NUM,
                new int[]{8},
                templateRow,
                new int[]{
                        SYSROLESRowFactory.SYSROLES_INDEX_ID_EE_OR_IDX,
                        SYSROLESRowFactory.SYSROLES_INDEX_ID_DEF_IDX,
                        SYSROLESRowFactory.SYSROLES_INDEX_EE_DEFAULT_IDX,
                });

        templateRow = getExecutionFactory().getValueRow(SYSROLESRowFactory.SYSROLES_COLUMN_COUNT);
        templateRow.setColumn(5, new SQLChar(databaseID.toString()));
        upgradeAddIndexedColumnToSystemTable(tc, SYSUSERS_CATALOG_NUM,
                new int[]{5},
                templateRow,
                new int[]{0});
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
        createOrUpdateSystemView(tc, "SYSVW", "SYSTABLESTATISTICS");


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
        createOrUpdateSystemView(tc, "SYSVW", "SYSCOLUMNSTATISTICS");

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

        createNaturalNumbersTable(tc);

        createTableColumnViewInSysIBM(tc);

        createKeyColumnUseViewInSysIBM(tc);

        createTablesAndViewsInSysIBMADM(tc);

        createAliasToTableSystemView(tc);

        createIndexColumnUseViewInSysCat(tc);

        createReferencesViewInSysCat(tc);

        createSysIndexesViewInSysIBM(tc);

        // don't pull this call before createTableColumnViewInSysIBM()
        createColumnsViewInSysCat(tc);
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
        upgradeIfNecessary(tc, startParams);

        resetSpliceDbOwner(tc, spliceDbDesc.getUUID());
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
                AliasDescriptor ad=getAliasDescriptor(schemaID,routineName,nameSpace, null);
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
        if(create) {
            SpliceAccessManager af=(SpliceAccessManager)Monitor.findServiceModule(this,AccessFactory.MODULE);
            ContextService.getFactory();
            SpliceTransactionManager txnManager=(SpliceTransactionManager)af.getTransaction(ContextService.getCurrentContextManager());
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
    public Long peekAtSequence(UUID dbId, String schemaName,String sequenceName) throws StandardException {
        String sequenceUUIDstring=getSequenceID(dbId, schemaName, sequenceName);
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

                TransactionController tc=getLCC().getTransactionExecute();
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
    public void createOrUpdateAllSystemProcedures(DatabaseDescriptor dbDesc, TransactionController tc) throws StandardException{
        tc.elevate("dictionary");
        super.createOrUpdateAllSystemProcedures(dbDesc, tc);
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

    private void upgradeIfNecessary(TransactionController tc, Properties startParams) throws StandardException{

        boolean toUpgrade = Boolean.TRUE.equals(EngineLifecycleService.toUpgrade.get());
        // Only master can upgrade
        if (!toUpgrade) {
            return;
        }

        Splice_DD_Version catalogVersion=(Splice_DD_Version)tc.getProperty(SPLICE_DATA_DICTIONARY_VERSION);
        if(needToUpgrade(catalogVersion)){
            tc.elevate("dictionary");
            SpliceCatalogUpgradeScripts scripts=new SpliceCatalogUpgradeScripts(this, tc, startParams);
            scripts.runUpgrades(catalogVersion);
            tc.setProperty(SPLICE_DATA_DICTIONARY_VERSION,spliceSoftwareVersion,true);
            tc.commit();
        }
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

    public int upgradeTablePriorities(TransactionController tc) throws Exception {
        PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
        ArrayList<String> toUpgrade = new ArrayList<>();
        Function<TabInfoImpl, Void> addTabInfo =  (TabInfoImpl info ) ->
                {
                    toUpgrade.add( Long.toString(info.getHeapConglomerate()) );
                    for( int j = 0; j < info.getNumberOfIndexes(); j++ )
                        toUpgrade.add( Long.toString(info.getIndexConglomerate(j)) );
                    return null;
                };
        for (int i = 0; i < coreInfo.length; ++i) {
            assert coreInfo[i] != null;
            addTabInfo.apply(coreInfo[i]);
        }
        for (int i = 0; i < NUM_NONCORE; ++i) {
            // noncoreInfo[x] will be null otherwise
            addTabInfo.apply( getNonCoreTI(i+NUM_CORE) );
        }

        for( String s : HBaseConfiguration.internalTablesArr) {
            toUpgrade.add(s);
        }
        toUpgrade.add("16"); // splice:16 core table
        toUpgrade.add(SIConfigurations.CONGLOMERATE_TABLE_NAME);

        return admin.upgradeTablePrioritiesFromList(toUpgrade);
    }

    public void removeUnusedBackupTables(TransactionController tc) throws StandardException {
        dropUnusedBackupTable("SYSBACKUPFILESET", tc);
        dropUnusedBackupTable("SYSBACKUPJOBS", tc);
    }

    public void removeUnusedBackupProcedures(TransactionController tc) throws StandardException {
        AliasDescriptor ad = getAliasDescriptor(SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
                "SYSCS_SCHEDULE_DAILY_BACKUP", AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR, tc);
        if (ad != null) {
            dropAliasDescriptor(ad, tc);
            SpliceLogUtils.info(LOG, "Dropped system procedure SYSCS_UTIL.SYSCS_SCHEDULE_DAILY_BACKUP");
        }

        ad = getAliasDescriptor(SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID,
                "SYSCS_CANCEL_DAILY_BACKUP", AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR, tc);
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
        RoutinePermsDescriptor key=new RoutinePermsDescriptor(this,authorizationId,null,routineUUID, null);

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

        tc.elevate("dictionary");

        SConfiguration configuration=SIDriver.driver().getConfiguration();

        String metadataRestrictionEnabled = configuration.getMetadataRestrictionEnabled();

        // check sysschemasview
        createOrUpdateSystemView(tc, "SYSVW", "SYSSCHEMASVIEW");

        // we need to re-generate the metadataSPS due to the definition change of sysschemasview
        updateMetadataSPSes(tc);

        tc.commit();
        SpliceLogUtils.info(LOG, "SYSVW.SYSSCHEMAVIEW updated to " + metadataRestrictionEnabled);
    }

    public void createOrUpdateSystemView(TransactionController tc, SchemaDescriptor viewSchema, int catalogNum, String viewName, int viewIndex, String viewSql) throws StandardException {
        tc.elevate("dictionary");
        TableDescriptor td = getTableDescriptor(viewName, viewSchema, tc);
        if (td != null) {
            ViewDescriptor vd = getViewDescriptor(td);
            boolean needUpdate = !vd.getViewText().equals(viewSql);

            // view definition matches the setting, no update needed
            if (!needUpdate)
                return;

            // drop the view definition
            dropAllColumnDescriptors(td.getUUID(), tc);
            dropViewDescriptor(vd, tc);
            dropTableDescriptor(td, viewSchema, tc);
        }

        // add new view definition
        createOneSystemView(tc, catalogNum, viewName, viewIndex, viewSchema, viewSql);

        SpliceLogUtils.info(LOG, String.format("%s.%s is updated!", viewSchema.getSchemaName(), viewName));
    }

    public void createOrUpdateSystemView(TransactionController tc, String schemaName, String viewName) throws StandardException {
        viewDefinitions.createOrUpdateView(tc, this, schemaName, viewName);
    }

    public void createPermissionTableSystemViews(TransactionController tc) throws StandardException {
        tc.elevate("dictionary");
        //Add the SYSVW schema if it does not exists
        if (getSchemaDescriptor(spliceDbDesc.getUUID(), SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME, tc, false) == null) {
            SpliceLogUtils.info(LOG, "SYSVW does not exist, system views for permission tables are not created!");
            return;
        }

        createOrUpdateSystemView(tc, "SYSVW", "SYSTABLEPERMSVIEW");
        createOrUpdateSystemView(tc, "SYSVW", "SYSSCHEMAPERMSVIEW");
        createOrUpdateSystemView(tc, "SYSVW", "SYSCOLPERMSVIEW");
        createOrUpdateSystemView(tc, "SYSVW", "SYSROUTINEPERMSVIEW");
        createOrUpdateSystemView(tc, "SYSVW", "SYSPERMSVIEW");

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
        if (getSchemaDescriptor(spliceDbDesc.getUUID(), SchemaDescriptor.STD_SYSTEM_VIEW_SCHEMA_NAME, tc, false) == null) {
            SpliceLogUtils.info(LOG, "SYSVW does not exist, system views for permission tables are not created!");
            return;
        }

        // create sysaliastotableview
        createOrUpdateSystemView(tc, "SYSVW", "SYSALIASTOTABLEVIEW");

        SpliceLogUtils.info(LOG, "System View SYSALIASTOTABLEVIEW created in SYSVW!");
    }

    public void addCatalogVersion(TransactionController tc) throws StandardException{
        for (int i = 0; i < coreInfo.length; ++i) {
            long conglomerateId = coreInfo[i].getHeapConglomerate();
            tc.setCatalogVersion(conglomerateId, catalogVersions.get(i));
        }

        for (int i = 0; i < noncoreInfo.length; ++i) {
            long conglomerateId = getNonCoreTI(i+NUM_CORE).getHeapConglomerate();
            if (conglomerateId > 0) {
                tc.setCatalogVersion(conglomerateId, catalogVersions.get(i + NUM_CORE));
            }
            else {
                SpliceLogUtils.warn(LOG, "Cannot set catalog version for table number %d", i);
            }
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
            createOrUpdateSystemView(tc, "SYSVW", SYSTABLESRowFactory.SYSTABLE_VIEW_NAME);

            SpliceLogUtils.info(LOG, String.format("%s upgraded: added a column: %s.", SYSTABLESRowFactory.SYSTABLE_VIEW_NAME,
                    SYSTABLESRowFactory.MIN_RETENTION_PERIOD));

            // finally, set the minimum retention period for SYS tables to 1 week.
            TabInfoImpl ti=coreInfo[SYSTABLES_CATALOG_NUM];
            faultInTabInfo(ti, tc);

            FormatableBitSet columnToReadSet=new FormatableBitSet(SYSTABLESRowFactory.SYSTABLES_COLUMN_COUNT);
            FormatableBitSet columnToUpdateSet=new FormatableBitSet(SYSTABLESRowFactory.SYSTABLES_COLUMN_COUNT);
            for(int i=0;i<SYSTABLESRowFactory.SYSTABLES_COLUMN_COUNT;i++){
                columnToUpdateSet.set(i);
                if(i+1 == SYSTABLESRowFactory.SYSTABLES_SCHEMAID || i+1 == SYSTABLESRowFactory.SYSTABLES_MIN_RETENTION_PERIOD) {
                    columnToReadSet.set(i);
                }
            }
            /* Set up a couple of row templates for fetching CHARS */
            DataValueDescriptor[] rowTemplate = new DataValueDescriptor[SYSTABLESRowFactory.SYSTABLES_COLUMN_COUNT];
            DataValueDescriptor[] replaceRow= new DataValueDescriptor[SYSTABLESRowFactory.SYSTABLES_COLUMN_COUNT];
            DataValueDescriptor authIdOrderable=new SQLVarchar(sd.getUUID().toString());
            ScanQualifier[][] scanQualifier=exFactory.getScanQualifier(1);
            scanQualifier[0][0].setQualifier(
                    SYSTABLESRowFactory.SYSTABLES_SCHEMAID - 1,    /* to zero-based */
                    authIdOrderable,
                    Orderable.ORDER_OP_EQUALS,
                    false,
                    false,
                    false);
            /* Scan the entire heap */
            try (ScanController sc=
                    tc.openScan(
                            ti.getHeapConglomerate(),
                            false,
                            TransactionController.OPENMODE_FORUPDATE,
                            TransactionController.MODE_TABLE,
                            TransactionController.ISOLATION_REPEATABLE_READ,
                            columnToReadSet,
                            null,
                            ScanController.NA,
                            scanQualifier,
                            null,
                            ScanController.NA)) {

                while (sc.fetchNext(rowTemplate)) {
                    /* Replace the column in the table */
                    for (int i = 0; i < rowTemplate.length; i++) {
                        if (i + 1 == SYSTABLESRowFactory.SYSTABLES_MIN_RETENTION_PERIOD)
                            replaceRow[i] = new SQLLongint(getSystablesMinRetentionPeriod());
                        else
                            replaceRow[i] = rowTemplate[i].cloneValue(false);
                    }
                    sc.replace(replaceRow, columnToUpdateSet);
                }
            }
        }
    }

    public void setJavaClassNameColumnInSysAliases(TransactionController tc) throws StandardException {
        TabInfoImpl ti = getNonCoreTI(SYSALIASES_CATALOG_NUM);
        faultInTabInfo(ti, tc);

        FormatableBitSet columnToReadSet = new FormatableBitSet(SYSALIASESRowFactory.SYSALIASES_COLUMN_COUNT);
        FormatableBitSet columnToUpdateSet = new FormatableBitSet(SYSALIASESRowFactory.SYSALIASES_COLUMN_COUNT);
        for (int i = 0; i < SYSALIASESRowFactory.SYSALIASES_COLUMN_COUNT; i++) {
            // partial row updates do not work properly (DB-9388), therefore, we read all columns and mark them all for
            // update even if this is not necessary for all of them.
            columnToReadSet.set(i);
            columnToUpdateSet.set(i);
        }
        /* Set up a row template for fetching */
        DataValueDescriptor[] rowTemplate = new DataValueDescriptor[SYSALIASESRowFactory.SYSALIASES_COLUMN_COUNT];
        /* Set up another row for replacing the existing row, effectively updating it */
        DataValueDescriptor[] replaceRow = new DataValueDescriptor[SYSALIASESRowFactory.SYSALIASES_COLUMN_COUNT];

        /* Scan the entire heap */
        try (ScanController sc = tc.openScan(
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
                ScanController.NA)) {

            while (sc.fetchNext(rowTemplate)) {
                for (int i = 0; i < rowTemplate.length; i++) {
                    replaceRow[i] = rowTemplate[i].cloneValue(false);
                    /* If JAVACLASSNAME was set to null, rewrite it to "NULL" string literal instead. */
                    if (i + 1 == SYSALIASESRowFactory.SYSALIASES_JAVACLASSNAME && rowTemplate[i].isNull()) {
                        replaceRow[i] = new SQLLongvarchar("NULL");
                    }
                }
                sc.replace(replaceRow, columnToUpdateSet);
            }
        }
    }

    @Override
    public long getSystablesMinRetentionPeriod() {
        return SIDriver.driver().getConfiguration().getSystablesMinRetentionPeriod();
    }

    @Override
    public boolean useTxnAwareCache() {
        return !SpliceClient.isRegionServer;
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "Intentional")
    public void upgradeAddColumnToSystemTable(TransactionController tc, int catalogNumber, int[] colIds, ExecRow templateRow) throws StandardException {
        int lastCol = colIds[colIds.length - 1];
        TabInfoImpl tabInfo = getTabInfoByNumber(catalogNumber);
        try {
            TableDescriptor td = getTableDescriptor(tabInfo.getCatalogRowFactory().getCatalogName(),
                            getSystemSchemaDescriptor(), tc );
            long conglomID = td.getHeapConglomerateId();
            try (ConglomerateController heapCC = tc.openConglomerate(conglomID,
                    false,0,
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ) ) {
                // If upgrade has already been done, and we somehow got here again by
                // mistake, don't re-add the columns to the conglomerate descriptor.
                if (heapCC instanceof HBaseController) {
                    HBaseController hCC = (HBaseController) heapCC;
                    if (hCC.getConglomerate().getFormat_ids().length >= lastCol) {
                        return;
                    }
                }
            }
            upgrade_addColumns(tabInfo.getCatalogRowFactory(), colIds, templateRow, tc);
            SpliceLogUtils.info(LOG, "Catalog upgraded: updated system table %s", tabInfo.getTableName());
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, "Attempt to upgrade %s failed. " +
                    "Please check if it has already been upgraded and contains the correct number of columns: %s.",
                    tabInfo.getTableName(), lastCol);
        }
    }

    public void populateNewSystemTableColumns(TransactionController tc, int catalogNumber, int[] colIds, ExecRow templateRow) throws StandardException {
        TabInfoImpl tabInfo = getTabInfoByNumber(catalogNumber);
        TableDescriptor td = getTableDescriptor(tabInfo.getTableName(), systemSchemaDesc, tc);
        DataValueDescriptor[] fetchedRow = new DataValueDescriptor[templateRow.length()];
        DataValueDescriptor[] newRow = new DataValueDescriptor[templateRow.length()];

        FormatableBitSet columnToUpdateSet=new FormatableBitSet(templateRow.length());
        for(int i=0 ; i < templateRow.length() ; i++) {
            columnToUpdateSet.set(i);
        }

        // Init the heap conglomerate here
        for (ConglomerateDescriptor conglomerateDescriptor : td.getConglomerateDescriptors()) {
            if (!conglomerateDescriptor.isIndex()) {
                tabInfo.setHeapConglomerate(conglomerateDescriptor.getConglomerateNumber());
                break;
            }
        }

        try (ScanController sc=tc.openScan(
                tabInfo.getHeapConglomerate(),
                false,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
                null,
                null,
                0,
                null,
                null,
                0)) {
            while (sc.fetchNext(fetchedRow)) {
                for (int i = 0; i < fetchedRow.length; ++i) {
                    if (ArrayUtils.contains(colIds, i + 1)) {
                        newRow[i] = templateRow.getColumn(i + 1).cloneValue(false);
                    } else {
                        newRow[i] = fetchedRow[i] == null ? null : fetchedRow[i].cloneValue(false);
                    }
                }
                sc.replace(newRow, columnToUpdateSet);
            }
        }

        if (catalogNumber >= NUM_CORE) {
            // reset TI in NonCoreTI array, we only used the heap conglomerate here, so information about the indexes
            // are not fully populated. This TI should not be reused for future operations.
            clearNoncoreTable(catalogNumber - NUM_CORE);
        }
    }

    public void upgradeAddIndexedColumnToSystemTable(TransactionController tc, int catalogNumber, int[] colIds, ExecRow templateRow, int[] indexIds) throws StandardException {
        upgradeAddColumnToSystemTable(tc, catalogNumber, colIds, templateRow);
        populateNewSystemTableColumns(tc, catalogNumber, colIds, templateRow);
        upgradeRecreateIndexesOfSystemTable(tc, catalogNumber, indexIds);
    }

    public void upgradeRecreateIndexesOfSystemTable(TransactionController tc, int catalogNumber, int[] indexIds) throws StandardException {
        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TabInfoImpl tabInfo = getTabInfoByNumber(catalogNumber);
        TableDescriptor td = getTableDescriptor(tabInfo.getTableName(), systemSchemaDesc, tc);
        CatalogRowFactory crf = tabInfo.getCatalogRowFactory();

        // Init the heap conglomerate here
        for (ConglomerateDescriptor conglomerateDescriptor : td.getConglomerateDescriptors()) {
            if (!conglomerateDescriptor.isIndex()) {
                tabInfo.setHeapConglomerate(conglomerateDescriptor.getConglomerateNumber());
                break;
            }
        }


        for (int indexId : indexIds) {
            ConglomerateDescriptor cd = td.getConglomerateDescriptor(crf.getCanonicalIndexUUID(indexId));
            if (cd != null)
                dropConglomerateDescriptor(cd, tc);
            cd = bootstrapOneIndex(systemSchemaDesc, tc, ddg, tabInfo, indexId, tabInfo.getHeapConglomerate());
            addDescriptor(cd, systemSchemaDesc, SYSCONGLOMERATES_CATALOG_NUM, false, tc, false);

            // Cache may have that system table descriptor without the new index info
            dataDictionaryCache.clearNameTdCache();
            dataDictionaryCache.clearOidTdCache();

            CatalogRowFactory rf = tabInfo.getCatalogRowFactory();
            ExecRow outRow = rf.makeEmptyRow();
            try (ScanController scanController = tc.openScan(
                    tabInfo.getHeapConglomerate(),              // conglomerate to open
                    false,                                      // don't hold open across commit
                    0,                                          // for read
                    TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_REPEATABLE_READ,
                    null,                                       // all fields as objects
                    null,                                       // start position - first row
                    0,                                          // startSearchOperation - none
                    null,                                       // scanQualifier,
                    null,                                       // stop position -through last row
                    0)) {                                       // stopSearchOperation - none

                int batch = 1024;
                ExecRow[] rowList = new ExecRow[batch];
                RowLocation[] rowLocationList = new RowLocation[batch];

                int i = 0;
                while (scanController.fetchNext(outRow.getRowArray())) {
                    rowList[i % batch] = outRow.getClone();
                    rowLocationList[i % batch] = scanController.newRowLocationTemplate();
                    scanController.fetchLocation(rowLocationList[i % batch]);
                    i++;
                    if (i % batch == 0) {
                        tabInfo.insertIndexRowListImpl(rowList, rowLocationList, tc, indexId, batch);
                    }
                }
                // insert last batch
                if (i % batch > 0)
                    tabInfo.insertIndexRowListImpl(rowList, rowLocationList, tc, indexId, i % batch);
            }
        }

        if (catalogNumber >= NUM_CORE) {
            // reset TI in NonCoreTI array, we only used some indexes here, so information about the other
            // ones are not fully populated. This TI should not be reused for future operations.
            clearNoncoreTable(catalogNumber - NUM_CORE);
        }
    }
}
