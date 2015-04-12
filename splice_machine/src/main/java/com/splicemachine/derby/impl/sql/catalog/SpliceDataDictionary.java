package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ScanQualifier;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.catalog.*;
import com.splicemachine.db.impl.sql.execute.IndexColumnOrder;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.catalog.upgrade.SpliceCatalogUpgradeScripts;
import com.splicemachine.derby.impl.sql.depend.SpliceDependencyManager;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequenceKey;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.tools.version.ManifestReader;
import com.splicemachine.tools.version.SpliceMachineVersion;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceDataDictionary extends DataDictionaryImpl{

    protected static final Logger LOG=Logger.getLogger(SpliceDataDictionary.class);
    private volatile TabInfoImpl pkTable=null;

    private volatile TabInfoImpl statementHistoryTable=null;
    private volatile TabInfoImpl operationHistoryTable=null;
    private volatile TabInfoImpl backupTable=null;
    private volatile TabInfoImpl backupItemsTable=null;
    private volatile TabInfoImpl backupStatesTable=null;
    private volatile TabInfoImpl backupJobsTable=null;
    private volatile TabInfoImpl taskHistoryTable=null;

    private volatile TabInfoImpl tableStatsTable=null;
    private volatile TabInfoImpl columnStatsTable=null;
    private volatile TabInfoImpl physicalStatsTable=null;

    private Splice_DD_Version spliceSoftwareVersion;
    private HTableInterface spliceSequencesTable;
    private Properties defaultProperties;

    public static final String SPLICE_DATA_DICTIONARY_VERSION="SpliceDataDictionaryVersion";
    private volatile StatisticsStore statsStore;

    @Override
    public SystemProcedureGenerator getSystemProcedures(){
        return new SpliceSystemProcedures(this);
    }

    @Override
    public SubKeyConstraintDescriptor getSubKeyConstraint(UUID constraintId,
                                                          int type) throws StandardException{
        if(type==DataDictionary.PRIMARYKEY_CONSTRAINT){
            DataValueDescriptor constraintIDOrderable=getIDValueAsCHAR(constraintId);

            TabInfoImpl ti=getPkTable();
            faultInTabInfo(ti);

            ScanQualifier[][] scanQualifiers=exFactory.getScanQualifier(1);
            scanQualifiers[0][0].setQualifier(
                    SYSPRIMARYKEYSRowFactory.SYSPRIMARYKEYS_CONSTRAINTID-1,
                    constraintIDOrderable,
                    Orderable.ORDER_OP_EQUALS,
                    false,false,false);
            return (SubKeyConstraintDescriptor)getDescriptorViaHeap(
                    null,scanQualifiers,ti,null,null);
        }
        /*If it's a foreign key or unique constraint, then just do the derby default*/
        return super.getSubKeyConstraint(constraintId,type);
    }

    @Override
    protected void addSubKeyConstraint(KeyConstraintDescriptor descriptor,
                                       TransactionController tc) throws StandardException{
        ExecRow row;
        TabInfoImpl ti;

		    /*
             ** Foreign keys get a row in SYSFOREIGNKEYS, and
		     ** all others get a row in SYSKEYS.
		     */
        if(descriptor.getConstraintType()
                ==DataDictionary.FOREIGNKEY_CONSTRAINT){
            if(SanityManager.DEBUG){
                if(!(descriptor instanceof ForeignKeyConstraintDescriptor)){
                    SanityManager.THROWASSERT("descriptor not an fk descriptor, is "+
                            descriptor.getClass().getName());
                }
            }
            @SuppressWarnings("ConstantConditions") ForeignKeyConstraintDescriptor fkDescriptor=(ForeignKeyConstraintDescriptor)descriptor;

            ti=getNonCoreTI(SYSFOREIGNKEYS_CATALOG_NUM);
            SYSFOREIGNKEYSRowFactory fkkeysRF=(SYSFOREIGNKEYSRowFactory)ti.getCatalogRowFactory();

            row=fkkeysRF.makeRow(fkDescriptor,null);

			      /*
			       ** Now we need to bump the reference count of the
			       ** contraint that this FK references
			       */
            ReferencedKeyConstraintDescriptor refDescriptor=
                    fkDescriptor.getReferencedConstraint();

            refDescriptor.incrementReferenceCount();

            int[] colsToSet=new int[1];
            colsToSet[0]=SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_REFERENCECOUNT;

            updateConstraintDescriptor(refDescriptor,
                    refDescriptor.getUUID(),
                    colsToSet,
                    tc);
        }else if(descriptor.getConstraintType()==DataDictionary.PRIMARYKEY_CONSTRAINT){
            ti=getPkTable();
            faultInTabInfo(ti);
            SYSPRIMARYKEYSRowFactory pkRF=(SYSPRIMARYKEYSRowFactory)ti.getCatalogRowFactory();

            row=pkRF.makeRow(descriptor,null);
        }else{
            ti=getNonCoreTI(SYSKEYS_CATALOG_NUM);
            SYSKEYSRowFactory keysRF=(SYSKEYSRowFactory)ti.getCatalogRowFactory();

            // build the row to be stuffed into SYSKEYS
            row=keysRF.makeRow(descriptor,null);
        }

        // insert row into catalog and all its indices
        ti.insertRow(row,tc);
    }

    public void createStatisticsTables(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchema=getSystemSchemaDescriptor();

        //sys_table_statistics
        TabInfoImpl tableStatsInfo=getTableStatisticsTable();
        ColumnOrdering[] tableStatsOrder=new ColumnOrdering[]{
                new IndexColumnOrder(0),
                new IndexColumnOrder(1)
        };
        addTableIfAbsent(tc,systemSchema,tableStatsInfo,tableStatsOrder);
        TableDescriptor tableStatsDescriptor=getTableDescriptor(tableStatsInfo.getTableName(),systemSchema,tc);

        int[] pks=new int[]{0,1};
        ReferencedKeyConstraintDescriptor tablestatspk=dataDescriptorGenerator.newPrimaryKeyConstraintDescriptor(tableStatsDescriptor,
                "TABLESTATSPK",false,false,pks,uuidFactory.createUUID(),tableStatsDescriptor.getUUID(),systemSchema,true,0);
        addConstraintDescriptor(tablestatspk,tc);

        createSysTableStatsView(tc);

        //sys_column_statistics
        ColumnOrdering[] columnPkOrder=new ColumnOrdering[]{
                new IndexColumnOrder(0),
                new IndexColumnOrder(1),
                new IndexColumnOrder(2)
        };
        TabInfoImpl columnStatsInfo=getColumnStatisticsTable();
        addTableIfAbsent(tc,systemSchema,columnStatsInfo,columnPkOrder);
        TableDescriptor columnStatsDescriptor=getTableDescriptor(columnStatsInfo.getTableName(),systemSchema,tc);
        pks=new int[]{0,1,2};
        ReferencedKeyConstraintDescriptor columnStatsPk=dataDescriptorGenerator.newPrimaryKeyConstraintDescriptor(columnStatsDescriptor,
                "COLUMNSTATSPK",false,false,pks,uuidFactory.createUUID(),columnStatsDescriptor.getUUID(),systemSchema,true,0);
        addConstraintDescriptor(columnStatsPk,tc);

        createSysColumnStatsView(tc);
        //sys_physical_statistics
        ColumnOrdering[] physicalPkOrder=new ColumnOrdering[]{
                new IndexColumnOrder(0)
        };
        TabInfoImpl physicalStatsInfo=getPhysicalStatisticsTable();
        addTableIfAbsent(tc,systemSchema,physicalStatsInfo,physicalPkOrder);
        TableDescriptor physicalStatsDescriptor=getTableDescriptor(physicalStatsInfo.getTableName(),systemSchema,tc);
        pks=new int[]{0};
        ReferencedKeyConstraintDescriptor physicalStatsPk=dataDescriptorGenerator.newPrimaryKeyConstraintDescriptor(physicalStatsDescriptor,
                "PHYSICALSTATSPK",false,false,pks,uuidFactory.createUUID(),physicalStatsDescriptor.getUUID(),systemSchema,true,0);
        addConstraintDescriptor(physicalStatsPk,tc);
    }


    public void createXplainTables(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchemaDescriptor=getSystemSchemaDescriptor();

        //create SYSSTATEMENTHISTORY
        TabInfoImpl stmtHistTabInfo=getStatementHistoryTable();
        addTableIfAbsent(tc,systemSchemaDescriptor,stmtHistTabInfo,null);

        //create SYSOPERATIONHISTORY
        TabInfoImpl opHistTabInfo=getOperationHistoryTable();
        addTableIfAbsent(tc,systemSchemaDescriptor,opHistTabInfo,null);

        //SYSTASKHISTORY
        TabInfoImpl taskHistTabInfo=getTaskHistoryTable();
        addTableIfAbsent(tc,systemSchemaDescriptor,taskHistTabInfo,null);
    }

    private TabInfoImpl getBackupTable() throws StandardException{
        if(backupTable==null){
            backupTable=new TabInfoImpl(new BACKUPRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(backupTable);
        return backupTable;
    }

    private TabInfoImpl getBackupItemsTable() throws StandardException{
        if(backupItemsTable==null){
            backupItemsTable=new TabInfoImpl(new BACKUPITEMSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(backupItemsTable);
        return backupItemsTable;
    }

    private TabInfoImpl getBackupStatesTable() throws StandardException{
        if(backupStatesTable==null){
            backupStatesTable=new TabInfoImpl(new BACKUPFILESETRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(backupStatesTable);
        return backupStatesTable;
    }

    private TabInfoImpl getBackupJobsTable() throws StandardException{
        if(backupJobsTable==null){
            backupJobsTable=new TabInfoImpl(new BACKUPJOBSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(backupJobsTable);
        return backupJobsTable;
    }


    public void createLassenTables(TransactionController tc) throws StandardException{
        SchemaDescriptor systemSchemaDescriptor = getSystemSchemaDescriptor();

        // Create BACKUP table
        TabInfoImpl backupTabInfo = getBackupTable();
        if (getTableDescriptor(backupTabInfo.getTableName(), systemSchemaDescriptor, tc) == null ) {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Creating system table %s.%s", systemSchemaDescriptor.getSchemaName(), backupTabInfo.getTableName()));
            makeCatalog(backupTabInfo, systemSchemaDescriptor, tc);
        } else {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.", systemSchemaDescriptor.getSchemaName(), backupTabInfo.getTableName()));
        }

        // Create BACKUPITEMS
        TabInfoImpl backupItemsTabInfo = getBackupItemsTable();
        if (getTableDescriptor(backupItemsTabInfo.getTableName(), systemSchemaDescriptor, tc) == null ) {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Creating system table %s.%s", systemSchemaDescriptor.getSchemaName(), backupItemsTabInfo.getTableName()));
            makeCatalog(backupItemsTabInfo, systemSchemaDescriptor, tc);
        } else {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.", systemSchemaDescriptor.getSchemaName(), backupItemsTabInfo.getTableName()));
        }

        // Create BACKUPFILESET
        TabInfoImpl backupStatesTabInfo = getBackupStatesTable();
        if (getTableDescriptor(backupStatesTabInfo.getTableName(), systemSchemaDescriptor, tc) == null ) {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Creating system table %s.%s", systemSchemaDescriptor.getSchemaName(), backupStatesTabInfo.getTableName()));
            makeCatalog(backupStatesTabInfo, systemSchemaDescriptor, tc);
        } else {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.", systemSchemaDescriptor.getSchemaName(), backupStatesTabInfo.getTableName()));
        }

        // Create BACKUPJOBS
        TabInfoImpl backupJobsTabInfo = getBackupJobsTable();
        if (getTableDescriptor(backupJobsTabInfo.getTableName(), systemSchemaDescriptor, tc) == null ) {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Creating system table %s.%s", systemSchemaDescriptor.getSchemaName(), backupJobsTabInfo.getTableName()));
            makeCatalog(backupJobsTabInfo, systemSchemaDescriptor, tc);
        } else {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Skipping table creation since system table %s.%s already exists.", systemSchemaDescriptor.getSchemaName(), backupJobsTabInfo.getTableName()));
        }
    }

    @Override
    protected void createDictionaryTables(Properties params,
                                          TransactionController tc,
                                          DataDescriptorGenerator ddg) throws StandardException{
        //create the base dictionary tables
        super.createDictionaryTables(params,tc,ddg);

        //create SYSPRIMARYKEYS
        makeCatalog(getPkTable(),getSystemSchemaDescriptor(),tc);

        createXplainTables(tc);
        createLassenTables(tc);

        //create the Statistics tables
        createStatisticsTables(tc);

    }

    @Override
    protected SystemAggregateGenerator getSystemAggregateGenerator(){
        return new SpliceSystemAggregatorGenerator(this);
    }

    @Override
    public SchemaDescriptor locateSchemaRow(String schemaName,TransactionController tc) throws StandardException{
    	/*
    	Cache cache = SpliceDriver.driver().getCache(SpliceConstants.SYSSCHEMAS_INDEX1_ID_CACHE);
    	Element element;
    	if ( (element = cache.get(schemaName)) != null) {
    		if (tc == null)
    				tc = getTransactionCompile();
    		tc.getActiveStateTxIdString();
    		if (element.getVersion() >= Long.parseLong(tc.getActiveStateTxIdString())) {
    			return (SchemaDescriptor) element.getObjectValue();
    		}
    	}
    	*/
        DataValueDescriptor schemaNameOrderable;
        TabInfoImpl ti=coreInfo[SYSSCHEMAS_CORE_NUM];

        schemaNameOrderable=new SQLVarchar(schemaName);

        ExecIndexRow keyRow=exFactory.getIndexableRow(1);
        keyRow.setColumn(1,schemaNameOrderable);

        // XXX - TODO Cache Lookup

        SchemaDescriptor desc=(SchemaDescriptor)
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

        return desc;
    }

    @Override
    protected void loadDictionaryTables(TransactionController tc,
                                        Properties startParams) throws StandardException{
        super.loadDictionaryTables(tc,startParams);

        // Check splice data dictionary verion to decide if upgrade is necessary
        upgradeIfNecessary(tc);
    }

    /**
     * Overridden so that SQL functions implemented as system procedures
     * will be found if in the SYSFUN schema. Otherwise, the default
     * behavior would be to ignore these and only consider functions
     * implicitly defined in {@link BaseDataDictionary#SYSFUN_FUNCTIONS},
     * which are not actually in the system catalog.
     */
    public List getRoutineList(String schemaID,String routineName,char nameSpace)
            throws StandardException{

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
        defaultProperties=startParams;
        SpliceLogUtils.trace(LOG,"boot with create=%s,startParams=%s",create,startParams);
        SpliceMachineVersion spliceMachineVersion=(new ManifestReader()).createVersion();
        if(!spliceMachineVersion.isUnknown()){
            spliceSoftwareVersion=new Splice_DD_Version(this,spliceMachineVersion.getMajorVersionNumber(),
                    spliceMachineVersion.getMinorVersionNumber(),spliceMachineVersion.getPatchVersionNumber());
        }
        if(create){
            SpliceAccessManager af=(SpliceAccessManager)Monitor.findServiceModule(this,AccessFactory.MODULE);
            SpliceTransactionManager txnManager=(SpliceTransactionManager)af.getTransaction(ContextService.getFactory().getCurrentContextManager());
            ((SpliceTransaction)txnManager.getRawTransaction()).elevate("boot".getBytes());
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
        BaseSpliceTransaction rawTransaction=((SpliceTransactionManager)lcc.getTransactionExecute()).getRawTransaction();
        assert rawTransaction instanceof SpliceTransaction:"Programmer Error: Cannot perform a data dictionary write with a non-SpliceTransaction";
        SpliceTransaction txn=(SpliceTransaction)rawTransaction;
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
        if(!txn.allowsWrites())
            txn.elevate("dictionary".getBytes());
        super.startWriting(lcc);
    }

    @Override
    public void getCurrentValueAndAdvance(String sequenceUUIDstring,NumberDataValue returnValue)
            throws StandardException{

        try{
            RowLocation[] rowLocation=new RowLocation[1];
            SequenceDescriptor[] sequenceDescriptor=new SequenceDescriptor[1];

            LanguageConnectionContext llc=(LanguageConnectionContext)
                    ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);

            TransactionController tc=llc.getTransactionExecute();
            computeSequenceRowLocation(tc,sequenceUUIDstring,rowLocation,sequenceDescriptor);

            byte[] rlBytes=rowLocation[0].getBytes();

            if(spliceSequencesTable==null){
                spliceSequencesTable=SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES);
            }

            long start=sequenceDescriptor[0].getStartValue();
            long increment=sequenceDescriptor[0].getIncrement();

            SpliceSequence sequence=SpliceDriver.driver().getSequencePool().
                    get(new SpliceSequenceKey(spliceSequencesTable,rlBytes,start,increment,1l));

            returnValue.setValue(sequence.getNext());

        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    public void createOrUpdateAllSystemProcedures(TransactionController tc) throws StandardException{
        tc.elevate("dictionary");
        super.createOrUpdateAllSystemProcedures(tc);
    }

    @Override
    public StatisticsStore getStatisticsStore(){
        StatisticsStore store = statsStore; //volatile read
        if(store==null){
            synchronized(this){
                store = statsStore; //2nd volatile read
                if(store!=null){
                    store = statsStore = new SpliceDerbyStatisticsStore(StatisticsStorage.getPartitionStore());
                }
            }
        }
        return store;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    /*Table fetchers for XPLAIN tables*/
    private TabInfoImpl getStatementHistoryTable() throws StandardException{
        if(statementHistoryTable==null){
            statementHistoryTable=new TabInfoImpl(new SYSSTATEMENTHISTORYRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(statementHistoryTable);
        return statementHistoryTable;
    }

    private TabInfoImpl getOperationHistoryTable() throws StandardException{
        if(operationHistoryTable==null){
            operationHistoryTable=new TabInfoImpl(new SYSOPERATIONHISTORYRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(operationHistoryTable);
        return operationHistoryTable;
    }

    private TabInfoImpl getTaskHistoryTable() throws StandardException{
        if(taskHistoryTable==null){
            taskHistoryTable=new TabInfoImpl(new SYSTASKHISTORYRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(taskHistoryTable);
        return taskHistoryTable;
    }

    /*Table fetchers for Statistics tables*/
    private TabInfoImpl getPhysicalStatisticsTable() throws StandardException{
        if(physicalStatsTable==null){
            physicalStatsTable=new TabInfoImpl(new SYSPHYSICALSTATISTICSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(physicalStatsTable);
        return physicalStatsTable;
    }

    private TabInfoImpl getColumnStatisticsTable() throws StandardException{
        if(columnStatsTable==null){
            columnStatsTable=new TabInfoImpl(new SYSCOLUMNSTATISTICSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(columnStatsTable);
        return columnStatsTable;
    }

    private TabInfoImpl getTableStatisticsTable() throws StandardException{
        if(tableStatsTable==null){
            tableStatsTable=new TabInfoImpl(new SYSTABLESTATISTICSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(tableStatsTable);
        return tableStatsTable;
    }

    private TabInfoImpl getPkTable() throws StandardException{
        if(pkTable==null){
            pkTable=new TabInfoImpl(new SYSPRIMARYKEYSRowFactory(uuidFactory,exFactory,dvf));
        }
        initSystemIndexVariables(pkTable);
        return pkTable;
    }

    private void upgradeIfNecessary(TransactionController tc) throws StandardException{

        Splice_DD_Version catalogVersion=(Splice_DD_Version)tc.getProperty(SPLICE_DATA_DICTIONARY_VERSION);
        if(needToUpgrade(catalogVersion)){
            tc.elevate("dictionary");
            SpliceCatalogUpgradeScripts scripts=new SpliceCatalogUpgradeScripts(this,catalogVersion,tc);
            scripts.run();
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
        if(SpliceConstants.upgradeForced){
            LOG.info(String.format("Upgrade has been manually forced from version %s",SpliceConstants.upgradeForcedFromVersion));
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
                                  ColumnOrdering[] columnOrder) throws StandardException{
        if(getTableDescriptor(sysTableToAdd.getTableName(),systemSchema,tc)==null){
            SpliceLogUtils.trace(LOG,String.format("Creating system table %s.%s",systemSchema.getSchemaName(),sysTableToAdd.getTableName()));
            makeCatalog(sysTableToAdd,systemSchema,tc,columnOrder);
        }else{
            SpliceLogUtils.trace(LOG,String.format("Skipping table creation since system table %s.%s already exists",systemSchema.getSchemaName(),sysTableToAdd.getTableName()));
        }
    }

    private void createSysTableStatsView(TransactionController tc) throws StandardException{
        //create statistics views
        SchemaDescriptor sysSchema=getSystemSchemaDescriptor();

        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TableDescriptor view=ddg.newTableDescriptor("SYSTABLESTATISTICS",
                sysSchema,TableDescriptor.VIEW_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY);
        addDescriptor(view,sysSchema,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc);
        UUID viewId=view.getUUID();
        ColumnDescriptor[] tableViewCds=SYSTABLESTATISTICSRowFactory.getViewColumns(view,viewId);
        addDescriptorArray(tableViewCds,view,DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        ColumnDescriptorList viewDl=view.getColumnDescriptorList();
        Collections.addAll(viewDl,tableViewCds);

        ViewDescriptor vd=ddg.newViewDescriptor(viewId,"SYSTABLESTATISTICS",
                SYSTABLESTATISTICSRowFactory.STATS_VIEW_SQL,0,sysSchema.getUUID());
        addDescriptor(vd,sysSchema,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc);
    }

    private void createSysColumnStatsView(TransactionController tc) throws StandardException{
        //create statistics views
        SchemaDescriptor sysSchema=getSystemSchemaDescriptor();

        DataDescriptorGenerator ddg=getDataDescriptorGenerator();
        TableDescriptor view=ddg.newTableDescriptor("SYSCOLUMNSTATISTICS",
                sysSchema,TableDescriptor.VIEW_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY);
        addDescriptor(view,sysSchema,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc);
        UUID viewId=view.getUUID();
        ColumnDescriptor[] tableViewCds=SYSCOLUMNSTATISTICSRowFactory.getViewColumns(view,viewId);
        addDescriptorArray(tableViewCds,view,DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        ColumnDescriptorList viewDl=view.getColumnDescriptorList();
        Collections.addAll(viewDl,tableViewCds);

        ViewDescriptor vd=ddg.newViewDescriptor(viewId,"SYSCOLUMNSTATISTICS",
                SYSCOLUMNSTATISTICSRowFactory.STATS_VIEW_SQL,0,sysSchema.getUUID());
        addDescriptor(vd,sysSchema,DataDictionary.SYSVIEWS_CATALOG_NUM,true,tc);
    }


}
