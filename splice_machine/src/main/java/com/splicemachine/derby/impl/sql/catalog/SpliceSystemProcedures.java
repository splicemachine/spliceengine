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

package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.backup.BackupSystemProcedures;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.services.metadata.MetadataFactoryService;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.catalog.DefaultSystemProcedureGenerator;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;
import com.splicemachine.derby.impl.load.HdfsImport;
import com.splicemachine.derby.impl.storage.SpliceRegionAdmin;
import com.splicemachine.derby.impl.storage.TableSplit;
import com.splicemachine.derby.utils.*;
import com.splicemachine.procedures.external.ExternalTableSystemProcedures;
import com.splicemachine.replication.ReplicationSystemProcedure;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * System procedure generator implementation class that extends
 * base Derby implemention in order to add support for procedures
 * specific to Splice Machine and do not belong in Derby Layer,
 * such as those that assume the presence of HBase, and functions
 * explicitly added to the SYSFUN schema.
 *
 * @author Scott Fines
 */
public class SpliceSystemProcedures extends DefaultSystemProcedureGenerator {

    private static final String IMPORT_DATA_NAME = "SYSCS_IMPORT_DATA";
    private static final String IMPORT_DATA_UNSAFE_NAME = "SYSCS_IMPORT_DATA_UNSAFE";
    public static final String RESTORE_DATABASE_NAME = "SYSCS_RESTORE_DATABASE";
    public static final String BACKUP_DATABASE_NAME = "SYSCS_BACKUP_DATABASE";


    /** Flag to ensure the list of procedures is only initialized once. */
    private static volatile boolean initialized = false;

    public SpliceSystemProcedures(DataDictionary dictionary) {
        super(dictionary);
    }

    @Override
    protected Map/*<UUID,List<Procedure>*/ getProcedures(DataDictionary dictionary, TransactionController tc) throws StandardException {
        @SuppressWarnings("rawtypes")
        Map sysProcedures = super.getProcedures(dictionary, tc);

        // Only initialize the list of system procedures once.
        if (initialized) {
            return sysProcedures;
        }

        // Only one initialization at a time.
        // Synchronize on the parent class since the lists of system procedures are all static members of it.
        synchronized (DefaultSystemProcedureGenerator.class) {

            // Check if the list was initialized while we were waiting.
            if (initialized) {
                return sysProcedures;
            }

            UUID sysUUID = dictionary.getSystemUtilSchemaDescriptor().getUUID();

        	/*
        	 * Our import process is different than Vanilla Derby's, so find that Procedure in
        	 * the map and replace it with our own Procedure
        	 */
            for(Object entry : sysProcedures.entrySet()){
                Object key = ((Map.Entry)entry).getKey();
                @SuppressWarnings("unchecked") List<Procedure> procedures = (List<Procedure>)((Map.Entry)entry).getValue();
                // Iterate through existing procedures to perform modifications
                for(int i=0;i<procedures.size();i++){
                    Procedure sysProc = procedures.get(i);
                    if(IMPORT_DATA_NAME.equalsIgnoreCase(sysProc.getName())){
                    } else if(RESTORE_DATABASE_NAME.equals(sysProc.getName())) {
                        Procedure restore = Procedure.newBuilder().name(RESTORE_DATABASE_NAME)
                                .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                                .varchar("directory", 32672)
                                .bigint("backupId")
                                .arg("validate", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                                .build();
                        procedures.set(i, restore);
                    } else if(BACKUP_DATABASE_NAME.equals(sysProc.getName())) {
                        Procedure backup = Procedure.newBuilder().name(BACKUP_DATABASE_NAME)
                                .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                                .varchar("directory", 32672)
                                .varchar("type", 32672)
                                .build();
                        procedures.set(i, backup);
                    }
                } // End iteration through existing procedures


                //
                // SYS_UTIL schema
                //
                // Create splice utility procedures
                //

                if (key.equals(sysUUID)) {
                    Procedure refreshExternalTable = Procedure.newBuilder().name("SYSCS_REFRESH_EXTERNAL_TABLE")
                            .numOutputParams(0).numResultSets(0).ownerClass(ExternalTableSystemProcedures.class.getCanonicalName())
                            .varchar("schema", 32672)
                            .varchar("table", 32672)
                            .build();
                    procedures.add(refreshExternalTable);

        			/*
        			 * Add a system procedure to enable splitting tables once data is loaded.
        			 *
        			 * We do this here because this way we know we're in the SYSCS procedure set
        			 */
                    Procedure splitProc = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .build();
                    procedures.add(splitProc);

                    Procedure splitProc1 = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE_AT_POINTS")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("splitPoints", 32672)
                            .build();
                    procedures.add(splitProc1);

                    Procedure splitProc2 = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE_EVENLY")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .integer("numSplits")
                            .build();
                    procedures.add(splitProc2);

                    procedures.add(Procedure.newBuilder().name("SYSCS_SPLIT_REGION_AT_POINTS")
                                           .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                                           .catalog("regionName")
                                           .varchar("splitPoints", 32672)
                                           .build());

                    Procedure splitProc3 = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE_OR_INDEX_AT_POINTS")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("splitPoints", 32672)
                            .build();
                    procedures.add(splitProc3);

                    Procedure splitProc4 = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE_OR_INDEX")
                            .numOutputParams(0).numResultSets(1).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("columnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .build();
                    procedures.add(splitProc4);
                    
         			/*
        			 * Procedure get all active services
        			 */
                    Procedure getActiveServers = Procedure.newBuilder().name("SYSCS_GET_ACTIVE_SERVERS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getActiveServers);

        			/*
        			 * Procedure get all active requests
        			 */
                    Procedure getRequests = Procedure.newBuilder().name("SYSCS_GET_REQUESTS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getRequests);


        			/*
        			 * Procedure get stats info for region servers
        			 */
                    Procedure getRegionServerStatsInfo = Procedure.newBuilder().name("SYSCS_GET_REGION_SERVER_STATS_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .build();
                    procedures.add(getRegionServerStatsInfo);

        			/*
        			 * Procedure fetch logical configuration from region servers
        			 */
                    Procedure getRegionServerConfig = Procedure.newBuilder().name("SYSCS_GET_REGION_SERVER_CONFIG_INFO")
                            .varchar("configRoot", 128) // fetch only config props with prefix, or null for fetch all
                            .integer("mode")            // 0 = fetch all, 1 = fetch only props where value not same on all servers
                            .numOutputParams(0)
                            .numResultSets(1)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getRegionServerConfig);

                    /*
                     * Procedure get Splice Machine manifest info
                     */
                    Procedure getVersionInfo = Procedure.newBuilder().name("SYSCS_GET_VERSION_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getVersionInfo);

                    Procedure getVersionInfoLocal = Procedure.newBuilder().name("SYSCS_GET_VERSION_INFO_LOCAL")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getVersionInfoLocal);

        			/*
        			 * Procedure get write pipeline intake info
        			 */
                    Procedure getWriteIntakeInfo = Procedure.newBuilder().name("SYSCS_GET_WRITE_INTAKE_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getWriteIntakeInfo);

                    /*
        			 * Procedure get exec service info
        			 */
                    Procedure getExecServiceInfo = Procedure.newBuilder().name("SYSCS_GET_EXEC_SERVICE_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getExecServiceInfo);

                    /*
        			 * Procedure get each individual cache info
        			 */
                    Procedure getCacheInfo = Procedure.newBuilder().name("SYSCS_GET_CACHE_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getCacheInfo);

                    /*
        			 * Procedure get total cache info
        			 */
                    Procedure getTotalCacheInfo = Procedure.newBuilder().name("SYSCS_GET_TOTAL_CACHE_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getTotalCacheInfo);

        			/*
        			 * Procedures to kill stale transactions
        			 */
                    Procedure killTransaction = Procedure.newBuilder().name("SYSCS_KILL_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .bigint("transactionId")
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(killTransaction);

                    Procedure killStaleTransactions = Procedure.newBuilder().name("SYSCS_KILL_STALE_TRANSACTIONS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .bigint("maximumTransactionId")
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(killStaleTransactions);

//                    Procedure dumpTransactions = Procedure.newBuilder().name("SYSCS_DUMP_TRANSACTIONS")
//                            .numOutputParams(0)
//                            .numResultSets(1)
//                            .ownerClass(TransactionAdmin.class.getCanonicalName())
//                            .build();
//                    procedures.add(dumpTransactions);

                    Procedure currTxn = Procedure.newBuilder().name("SYSCS_GET_CURRENT_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(currTxn);

                    Procedure activeTxn = Procedure.newBuilder().name("SYSCS_GET_ACTIVE_TRANSACTION_IDS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(activeTxn);

                    /*
                     * Statistics procedures
                     */
                    Procedure collectStatsForTable = Procedure.newBuilder().name("COLLECT_TABLE_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("schema",128)
                            .varchar("table",1024)
                            .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(collectStatsForTable);

                    Collection<Procedure> procs = MetadataFactoryService.newMetadataFactory().getProcedures();

                    if (procs != null)
                        procedures.addAll(procs);

                    Procedure collectSampleStatsForTable = Procedure.newBuilder().name("COLLECT_TABLE_SAMPLE_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("schema",128)
                            .varchar("table",1024)
                            .arg("samplePercentage", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE).getCatalogType())
                            .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(collectSampleStatsForTable);

                    Procedure collectNonMergedStatsForTable = Procedure.newBuilder().name("COLLECT_NONMERGED_TABLE_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("schema",128)
                            .varchar("table",1024)
                            .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(collectNonMergedStatsForTable);

                    Procedure collectNonMergedSampleStatsForTable = Procedure.newBuilder().name("COLLECT_NONMERGED_TABLE_SAMPLE_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("schema",128)
                            .varchar("table",1024)
                            .arg("samplePercentage", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE).getCatalogType())
                            .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(collectNonMergedSampleStatsForTable);

                    Procedure importWithBadRecords = Procedure.newBuilder().name("IMPORT_DATA")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .build();
                    procedures.add(importWithBadRecords);

                    Procedure importUnsafe = Procedure.newBuilder().name("IMPORT_DATA_UNSAFE")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .build();
                    procedures.add(importUnsafe);

                   Procedure bulkImportHFile = Procedure.newBuilder().name("BULK_IMPORT_HFILE")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .varchar("bulkImportDirectory", 32672)
                            .varchar("skipSampling", 5)
                            .build();
                    procedures.add(bulkImportHFile);


                    Procedure sampleData = Procedure.newBuilder().name("SAMPLE_DATA")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .varchar("bulkImportDirectory", 32672)
                            .build();
                    procedures.add(sampleData);

                    Procedure computeSplitKey= Procedure.newBuilder().name("COMPUTE_SPLIT_KEY")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("columnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .varchar("outputDirectory", 32672)
                            .build();
                    procedures.add(computeSplitKey);

                    Procedure upport = Procedure.newBuilder().name("UPSERT_DATA_FROM_FILE")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .build();
                    procedures.add(upport);

                    Procedure merge = Procedure.newBuilder().name("MERGE_DATA_FROM_FILE")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .bigint("maxBadRecords")
                            .varchar("badRecordDirectory",32672)
                            .varchar("oneLineRecords",5)
                            .varchar("charset",32672)
                            .build();
                    procedures.add(merge);

                    Procedure getAutoIncLocs = Procedure.newBuilder().name("SYSCS_GET_AUTO_INCREMENT_ROW_LOCATIONS")
                            .numOutputParams(0).numResultSets(1).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .build();
                    procedures.add(getAutoIncLocs);


                    Procedure dropStatsForSchema = Procedure.newBuilder().name("DROP_SCHEMA_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("schema",128)
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(dropStatsForSchema);

                    Procedure dropStatsForTable = Procedure.newBuilder().name("DROP_TABLE_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("schema",128)
                            .varchar("table",1024)
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(dropStatsForTable);

                    Procedure collectStatsForSchema = Procedure.newBuilder().name("COLLECT_SCHEMA_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("schema",128)
                            .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(collectStatsForSchema);

                    Procedure enableStatsForColumn = Procedure.newBuilder().name("ENABLE_COLUMN_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .modifiesSql()
                            .varchar("schema",1024)
                            .varchar("table",1024)
                            .varchar("column",1024)
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(enableStatsForColumn);

                    Procedure disableStatsForColumn = Procedure.newBuilder().name("DISABLE_COLUMN_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .modifiesSql()
                            .varchar("schema",1024)
                            .varchar("table",1024)
                            .varchar("column",1024)
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(disableStatsForColumn);

                    Procedure enableStatsForAllColumns = Procedure.newBuilder().name("ENABLE_ALL_COLUMN_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .modifiesSql()
                            .catalog("schema")
                            .catalog("table")
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(enableStatsForAllColumns);

                    Procedure disableStatsForAllColumns = Procedure.newBuilder().name("DISABLE_ALL_COLUMN_STATISTICS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .modifiesSql()
                            .catalog("schema")
                            .catalog("table")
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(disableStatsForAllColumns);

                    Procedure setStatsExtrapolationForColumn = Procedure.newBuilder().name("SET_STATS_EXTRAPOLATION_FOR_COLUMN")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .modifiesSql()
                            .varchar("schema",1024)
                            .varchar("table",1024)
                            .varchar("column",1024)
                            .smallint("useExtrapolation")
                            .ownerClass(StatisticsAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(setStatsExtrapolationForColumn);

        			/*
        			 * Procedure to get the session details
        			 */
                    Procedure sessionInfo = Procedure.newBuilder().name("SYSCS_GET_SESSION_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(sessionInfo);


        			/*
        			 * Procedure to get a list of running operations
        			 */
                    Procedure runningOperations = Procedure.newBuilder().name("SYSCS_GET_RUNNING_OPERATIONS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(runningOperations);

        			/*
        			 * Procedure to get a list of running operations on the local server
        			 */
                    Procedure runningOperationsLocal = Procedure.newBuilder().name("SYSCS_GET_RUNNING_OPERATIONS_LOCAL")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(runningOperationsLocal);


        			/*
        			 * Procedure to kill an executing operation
        			 */
                    Procedure killOperationLocal = Procedure.newBuilder().name("SYSCS_KILL_OPERATION_LOCAL")
                            .numOutputParams(0)
                            .varchar("uuid",128)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(killOperationLocal);

        			/*
        			 * Procedure to kill an executing operation
        			 */
                    Procedure killOperation = Procedure.newBuilder().name("SYSCS_KILL_OPERATION")
                            .numOutputParams(0)
                            .varchar("uuid",128)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(killOperation);

                    /*
                     * Procedure to kill an executing DRDA operation
                     */
                    Procedure killDrdaOperationLocal = Procedure.newBuilder().name("SYSCS_KILL_DRDA_OPERATION_LOCAL")
                            .numOutputParams(0)
                            .varchar("rdbIntTkn",512)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(killDrdaOperationLocal);

                    /*
                     * Procedure to kill an executing DRDA operation
                     */
                    Procedure killDrdaOperation = Procedure.newBuilder().name("SYSCS_KILL_DRDA_OPERATION")
                            .numOutputParams(0)
                            .varchar("rdbIntTkn",512)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(killDrdaOperation);


                    /*
                     * Procedure to get oldest active transaction id
                     */
                    Procedure getActiveTxn = Procedure.newBuilder().name("SYSCS_GET_OLDEST_ACTIVE_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getActiveTxn);

                    /*
                     * Procedure to delegate HDFS operations
                     */
                    Procedure hdfsOperation = Procedure.newBuilder().name("SYSCS_HDFS_OPERATION")
                            .numOutputParams(0)
                            .varchar("path",128)
                            .varchar("op", 64)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(hdfsOperation);

                    /*
                     * Procedure to delegate HBase operations
                     */
                    Procedure hbaseOperation = Procedure.newBuilder().name("SYSCS_HBASE_OPERATION")
                            .numOutputParams(0)
                            .varchar("table",128)
                            .varchar("op", 64)
                            .blob("request")
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(hbaseOperation);

                    /*
                     * Procedure to get Splice Token
                     */
                    Procedure getToken = Procedure.newBuilder().name("SYSCS_GET_SPLICE_TOKEN")
                            .numOutputParams(0)
                            .varchar("username",256)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getToken);

                    /*
                     * Procedure to cancel a Splice Token
                     */
                    Procedure cancelToken = Procedure.newBuilder().name("SYSCS_CANCEL_SPLICE_TOKEN")
                            .numOutputParams(0)
                            .blob("request")
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(cancelToken);

                    /*
                     * Procedure to elevate a transaction
                     */
                    Procedure elevProc = Procedure.newBuilder().name("SYSCS_ELEVATE_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("tableName", 128) // input
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(elevProc);

                    /*
                     * Procedure to start a child transaction
                     */
                    Procedure childTxnProc = Procedure.newBuilder().name("SYSCS_START_CHILD_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .bigint("parentTransactionId") // input
                            .varchar("tableName", 128) // input
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(childTxnProc);
                    
                    /*
                     * Procedure to commit a child transaction
                     */
                    Procedure commitTxnProc = Procedure.newBuilder().name("SYSCS_COMMIT_CHILD_TRANSACTION")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .bigint("childTransactionId") // input
                            .ownerClass(TransactionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(commitTxnProc);

        			/*
        			 * Procedure to get the log level for the given logger
        			 */
                    Procedure getLoggerLevel = Procedure.newBuilder().name("SYSCS_GET_LOGGER_LEVEL")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("loggerName", 128)
                            .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getLoggerLevel);

                    Procedure getLoggerLevelLocal = Procedure.newBuilder().name("SYSCS_GET_LOGGER_LEVEL_LOCAL")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("loggerName", 128)
                            .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getLoggerLevelLocal);

        			/*
        			 * Procedure to set the log level for the given logger
        			 */
                    Procedure setLoggerLevel = Procedure.newBuilder().name("SYSCS_SET_LOGGER_LEVEL")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("loggerName", 128)
                            .varchar("loggerLevel", 128)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(setLoggerLevel);

                    Procedure setLoggerLevelLocal = Procedure.newBuilder().name("SYSCS_SET_LOGGER_LEVEL_LOCAL")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .varchar("loggerName", 128)
                            .varchar("loggerLevel", 128)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(setLoggerLevelLocal);

        			/*
        			 * Procedure to get all the splice logger names in the system
        			 */
                    Procedure getLoggers = Procedure.newBuilder().name("SYSCS_GET_LOGGERS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getLoggers);

                    Procedure getLoggersLocal = Procedure.newBuilder().name("SYSCS_GET_LOGGERS_LOCAL")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getLoggersLocal);

        			/*
        			 * Procedure get table info in all schema
        			 */
                    Procedure getSchemaInfo = Procedure.newBuilder().name("SYSCS_GET_SCHEMA_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getSchemaInfo);

        			/*
        			 * Procedure to perform major compaction on all tables in a schema
        			 */
                    Procedure majorComactionOnSchema = Procedure.newBuilder().name("SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA")
                            .varchar("schemaName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(majorComactionOnSchema);

        			/*
        			 * Procedure to perform major compaction on a table in a schema
        			 */
                    Procedure majorComactionOnTable = Procedure.newBuilder().name("SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE")
                            .varchar("schemaName", 128)
                            .varchar("tableName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(majorComactionOnTable);

                    /*
                     * Procedure to perform major flush on a table in a schema
                     */
                    Procedure flushTable = Procedure.newBuilder().name("SYSCS_FLUSH_TABLE")
                            .varchar("schemaName", 128)
                            .varchar("tableName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(flushTable);

                    /*
                     * Procedure to delete rows from data dictionary
                     */
                    Procedure dictionaryDelete = Procedure.newBuilder().name("SYSCS_DICTIONARY_DELETE")
                            .integer("conglomerateId")
                            .varchar("rowId", 1024)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(dictionaryDelete);

        			/*
        			 * Procedure to get all the information related to the execution plans of the stored prepared statements (metadata queries).
        			 */
                    Procedure getStoredStatementPlanInfo = Procedure.newBuilder().name("SYSCS_GET_STORED_STATEMENT_PLAN_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getStoredStatementPlanInfo);

        			/*
        			 * Procedure to print all of the properties (JVM, Service, Database, App).
        			 */
                    Procedure getAllSystemProperties = Procedure.newBuilder().name("SYSCS_GET_ALL_PROPERTIES")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getAllSystemProperties);

                    Procedure vacuum = Procedure.newBuilder().name("VACUUM")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(vacuum);

                    /*
                     * Procedure to return timestamp generator info
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_GET_TIMESTAMP_GENERATOR_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(TimestampAdmin.class.getCanonicalName())
                            .build());

                    /*
                     * Procedure to return timestamp request info
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_GET_TIMESTAMP_REQUEST_INFO")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(TimestampAdmin.class.getCanonicalName())
                            .build());

                    /*
                     * Procedure to delete a backup
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_DELETE_BACKUP")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .bigint("backupId")
                            .build());

                    /*
                     * Procedure to delete backups in a time window
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_DELETE_OLD_BACKUPS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .integer("backupWindow")
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_BACKUP_DATABASE_ASYNC")
                            .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .varchar("directory", 32672)
                            .varchar("type", 32672)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_RESTORE_DATABASE_ASYNC")
                            .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .varchar("directory", 32672)
                            .bigint("backupId")
                            .arg("validate", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .build());

                    procedures.add(Procedure.newBuilder().name("VALIDATE_BACKUP")
                            .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .varchar("directory", 32672)
                            .bigint("backupId")
                            .build());

                    procedures.add(Procedure.newBuilder().name("VALIDATE_TABLE_BACKUP")
                            .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("directory", 32672)
                            .bigint("backupId")
                            .build());
                    /*
                     * Procedure to get a database property on all region servers in the cluster.
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_GET_GLOBAL_DATABASE_PROPERTY")
                    		.numOutputParams(0)
                    		.numResultSets(1)
                    		.ownerClass(SpliceAdmin.class.getCanonicalName())
                    		.sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    		.catalog("KEY")
                    		.build());

                    /*
                     * Procedure to set a database property on all region servers in the cluster.
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_SET_GLOBAL_DATABASE_PROPERTY")
                    		.numOutputParams(0)
                    		.numResultSets(0)
                    		.ownerClass(SpliceAdmin.class.getCanonicalName())
                    		.sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    		.catalog("KEY")
                    		.varchar("VALUE", Limits.DB2_VARCHAR_MAXWIDTH)
                    		.build());

                    /*
                     * Procedure to enable splice enterprise version
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_ENABLE_ENTERPRISE")
                    		.numOutputParams(0)
                    		.numResultSets(0)
                    		.ownerClass(SpliceAdmin.class.getCanonicalName())
                    		.sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    		.varchar("VALUE", Limits.DB2_VARCHAR_MAXWIDTH)
                    		.build());

                    /*
                     * Procedure to empty the statement caches on all region servers in the cluster.
                     * Essentially a wrapper around the Derby version of SYSCS_EMPTY_STATEMENT_CACHE
                     * which only operates on a single node.
                     */
                    procedures.add(Procedure.newBuilder().name("SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .sqlControl(RoutineAliasInfo.NO_SQL).returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("GET_ACTIVATION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .returnType(null).isDeterministic(false)
                            .varchar("statement", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build());

                    // Stored procedure that updates the owner (authorization id) of an existing schema.
                    procedures.add(Procedure.newBuilder().name("SYSCS_UPDATE_SCHEMA_OWNER")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA)
                        .returnType(null)
                        .isDeterministic(false)
                        .varchar("schemaName", 128)
                        .varchar("ownerName", 128)
                        .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_GET_RUNNING_BACKUPS")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_CANCEL_BACKUP")
                            .numOutputParams(0)
                            .bigint("backupId")
                            .numResultSets(0)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_RESTORE_DATABASE_OWNER")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .sqlControl(RoutineAliasInfo.NO_SQL).returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_BACKUP_TABLE")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("directory", 32672)
                            .varchar("type", 30)
                            .returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_RESTORE_TABLE")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .catalog("destSchema")
                            .catalog("destTable")
                            .catalog("sourceSchema")
                            .catalog("sourceTable")
                            .varchar("directory", 32672)
                            .bigint("backupId")
                            .arg("validate", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_BACKUP_SCHEMA")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .catalog("schemaName")
                            .varchar("directory", 32672)
                            .varchar("type", 30)
                            .returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_RESTORE_SCHEMA")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .catalog("destSchema")
                            .catalog("sourceSchema")
                            .varchar("directory", 32672)
                            .bigint("backupId")
                            .arg("validate", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .returnType(null).isDeterministic(false)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_RESTORE_DATABASE_TO_TIMESTAMP")
                            .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .varchar("directory", 32672)
                            .bigint("backupId")
                            .arg("validate", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .varchar("pointInTime", 100)
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_RESTORE_DATABASE_TO_TRANSACTION")
                            .numOutputParams(0).numResultSets(1).ownerClass(BackupSystemProcedures.class.getCanonicalName())
                            .varchar("directory", 32672)
                            .bigint("backupId")
                            .arg("validate", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                            .bigint("transactionId")
                            .build());

                    procedures.add(Procedure.newBuilder().name("SYSCS_SAVE_SOURCECODE")
                            .numResultSets(0).numOutputParams(0).modifiesSql()
                            .returnType(null).isDeterministic(false)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("objectName")
                            .varchar("objectType", 32)
                            .varchar("objectForm", 32)
                            .catalog("definerName")
                            .arg("sourceCode", DataTypeDescriptor.getCatalogType(Types.BLOB,64*1024*1024))
                            .build());

                    Procedure purgeDeletedRows = Procedure.newBuilder().name("SET_PURGE_DELETED_ROWS")
                            .varchar("schemaName", 128)
                            .varchar("tableName", 128)
                            .varchar("enable", 5)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(purgeDeletedRows);

                    Procedure snapshotSchema = Procedure.newBuilder().name("SNAPSHOT_SCHEMA")
                            .varchar("schemaName", 128)
                            .varchar("snapshotName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(snapshotSchema);

                    Procedure snapshotTable = Procedure.newBuilder().name("SNAPSHOT_TABLE")
                            .varchar("schemaName", 128)
                            .varchar("tableName", 128)
                            .varchar("snapshotName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(snapshotTable);

                    Procedure deleteSnapshot = Procedure.newBuilder().name("DELETE_SNAPSHOT")
                            .varchar("snapshotName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(deleteSnapshot);

                    Procedure restoreSnapshot = Procedure.newBuilder().name("RESTORE_SNAPSHOT")
                            .varchar("snapshotName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(restoreSnapshot);

                    Procedure getEncodedRegionName = Procedure.newBuilder().name("GET_ENCODED_REGION_NAME")
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("splitKey", 32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceRegionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getEncodedRegionName);

                    Procedure getSplitKey = Procedure.newBuilder().name("GET_START_KEY")
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("encodedRegionName", 128)
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceRegionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getSplitKey);

                    Procedure compactRegion = Procedure.newBuilder().name("COMPACT_REGION")
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("regionName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceRegionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(compactRegion);

                    Procedure majorCompactRegion = Procedure.newBuilder().name("MAJOR_COMPACT_REGION")
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("regionName", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceRegionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(majorCompactRegion);

                    Procedure mergeRegion = Procedure.newBuilder().name("MERGE_REGIONS")
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("regionName1", 128)
                            .varchar("regionName2", 128)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceRegionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(mergeRegion);

                    Procedure getAllRegions = Procedure.newBuilder().name("GET_REGIONS")
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("startKey", 32672)
                            .varchar("endKey", 32672)
                            .varchar("columnDelimiter",5)
                            .varchar("characterDelimiter", 5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672)
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceRegionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(getAllRegions);

                    Procedure deleteRegion = Procedure.newBuilder().name("DELETE_REGION")
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .varchar("regionName", 128)
                            .varchar("merge", 5)
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceRegionAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(deleteRegion);

                    Procedure invalidateLocalCache = Procedure.newBuilder().name("INVALIDATE_DICTIONARY_CACHE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(invalidateLocalCache);

                    Procedure invalidateGlobalCache = Procedure.newBuilder().name("INVALIDATE_GLOBAL_DICTIONARY_CACHE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(invalidateGlobalCache);

                    Procedure checkTable = Procedure.newBuilder().name("CHECK_TABLE")
                            .catalog("schemaName")
                            .catalog("tableName")
                            .catalog("indexName")
                            .integer("level")
                            .varchar("outputFile", 32672)
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(SpliceTableAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(checkTable);

                    Procedure showCreateTable = Procedure.newBuilder().name("SHOW_CREATE_TABLE")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .varchar("schemaName", 128)
                            .varchar("tableName", 128)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(showCreateTable);

                    Procedure addPeer = Procedure.newBuilder().name("ADD_PEER")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .smallint("peerId")
                            .varchar("clusterKey", 32672)
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .build();
                    procedures.add(addPeer);

                    Procedure removePeer = Procedure.newBuilder().name("REMOVE_PEER")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .smallint("peerId")
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .build();
                    procedures.add(removePeer);

                    Procedure enablePeer = Procedure.newBuilder().name("ENABLE_PEER")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .smallint("peerId")
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .build();
                    procedures.add(enablePeer);

                    Procedure disablePeer = Procedure.newBuilder().name("DISABLE_PEER")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .smallint("peerId")
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .build();
                    procedures.add(disablePeer);

                    Procedure setupReplicationSink = Procedure.newBuilder().name("SETUP_REPLICATION_SINK")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .build();
                    procedures.add(setupReplicationSink);

                    Procedure setupReplicationSinkLocal = Procedure.newBuilder().name("SETUP_REPLICATION_SINK_LOCAL")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .build();
                    procedures.add(setupReplicationSinkLocal);

                    Procedure shutdownReplicationSink = Procedure.newBuilder().name("SHUTDOWN_REPLICATION_SINK")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .build();
                    procedures.add(shutdownReplicationSink);

                    Procedure shutdownReplicationSinkLocal = Procedure.newBuilder().name("SHUTDOWN_REPLICATION_SINK_LOCAL")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .build();
                    procedures.add(shutdownReplicationSinkLocal);

                    Procedure enableTableReplication = Procedure.newBuilder().name("ENABLE_TABLE_REPLICATION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .build();
                    procedures.add(enableTableReplication);

                    Procedure disableTableReplication = Procedure.newBuilder().name("DISABLE_TABLE_REPLICATION")
                            .numOutputParams(0)
                            .numResultSets(1)
                            .ownerClass(ReplicationSystemProcedure.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .build();
                    procedures.add(disableTableReplication);

                    Procedure updateAllSystemProcedures = Procedure.newBuilder()
                            .name("SYSCS_UPDATE_ALL_SYSTEM_PROCEDURES")
                            .numOutputParams(0).numResultSets(0).modifiesSql()
                            .returnType(null).isDeterministic(false)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .build();
                    procedures.add(updateAllSystemProcedures);

                    Procedure updateSystemProcedure = Procedure.newBuilder().name("SYSCS_UPDATE_SYSTEM_PROCEDURE")
                            .numOutputParams(0).numResultSets(0).modifiesSql()
                            .returnType(null).isDeterministic(false)
                            .ownerClass(SpliceAdmin.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("procName")
                            .build();
                    procedures.add(updateSystemProcedure);
                }  // End key == sysUUID

            } // End iteration through map keys (schema UUIDs)

            // Initialization was successful.  Mark the class as initialized.
            initialized = true;
        } // end of synchronized block

        return sysProcedures;
    }

    @SuppressWarnings("unchecked")
    protected void augmentProcedureMap(DataDictionary dict, Map procedures) throws StandardException {
        //
        // SYSFUN schema
        //
        // Add SYSFUN schema functions, for functions that are Splice standard
        // but not necessarily ANSI standard. More convenient than adding them
        // to the SQL grammar. Do this in Splice, not Derby, because in Derby,
        // SYSFUN is not intended to actually contain anything.
        //
        UUID sysFunUUID = dict.getSysFunSchemaDescriptor().getUUID();
        procedures.put(sysFunUUID, SYSFUN_PROCEDURES);
    }

    private static final List<Procedure> SYSFUN_PROCEDURES;

    static{
        try {
            SYSFUN_PROCEDURES = new ArrayList<>();
            SYSFUN_PROCEDURES.addAll(Arrays.asList(
                    Procedure.newBuilder().name("INSTR")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.INTEGER))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("SUBSTR", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("INITCAP")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR, Limits.DB2_VARCHAR_MAXWIDTH))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("CONCAT")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR, Limits.DB2_VARCHAR_MAXWIDTH))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .varchar("ARG1", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("ARG2", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("REGEXP_LIKE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BOOLEAN))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .varchar("S", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("REGEXP", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("CHR")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.CHAR,1))
                            .isDeterministic(true).ownerClass(SpliceStringFunctions.class.getCanonicalName())
                            .integer("I")
                            .build()
            		)
            );
            SYSFUN_PROCEDURES.addAll(Arrays.asList(
                    //
                    // Date functions
                    //
                    Procedure.newBuilder().name("ADD_MONTHS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DATE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .integer("NUMOFMONTHS")
                            .build(),
                    Procedure.newBuilder().name("LAST_DAY")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DATE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .build(),
                    Procedure.newBuilder().name("TO_DATE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DATE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("FORMAT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("TO_TIMESTAMP")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.TIMESTAMP))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("FORMAT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("TO_TIME")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.TIME))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .varchar("SOURCE", Limits.DB2_VARCHAR_MAXWIDTH)
                            .varchar("FORMAT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("NEXT_DAY")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DATE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .varchar("WEEKDAY", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("MONTH_BETWEEN")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DOUBLE))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE1", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .arg("SOURCE2", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .build(),
                    Procedure.newBuilder().name("TO_CHAR")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.DATE))
                            .varchar("FORMAT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("TIMESTAMP_TO_CHAR")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("STAMP", DataTypeDescriptor.getCatalogType(Types.TIMESTAMP))
                            .varchar("OUTPUT", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),
                    Procedure.newBuilder().name("TRUNC_DATE")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.TIMESTAMP))
                            .isDeterministic(true).ownerClass(SpliceDateFunctions.class.getCanonicalName())
                            .arg("SOURCE", DataTypeDescriptor.getCatalogType(Types.TIMESTAMP))
                            .varchar("FIELD", Limits.DB2_VARCHAR_MAXWIDTH)
                            .build(),

                    //numeric functions
                    Procedure.newBuilder().name("SCALAR_POW")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .isDeterministic(true).ownerClass(NumericFunctions.class.getCanonicalName())
                            .arg("BASE",DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .arg("SCALE",DataTypeDescriptor.getCatalogType(Types.INTEGER))
                            .build(),
                    Procedure.newBuilder().name("POW")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.DOUBLE))
                            .isDeterministic(true).ownerClass(NumericFunctions.class.getCanonicalName())
                            .arg("BASE",DataTypeDescriptor.getCatalogType(Types.DOUBLE))
                            .arg("SCALE",DataTypeDescriptor.getCatalogType(Types.DOUBLE))
                            .build()

            ));

            SYSFUN_PROCEDURES.addAll(Arrays.asList(
            /*
             * Statistics functions
             */
                    Procedure.newBuilder().name("STATS_CARDINALITY")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_NULL_COUNT")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_NULL_FRACTION")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.REAL))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_QUANTILES")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_FREQUENCIES")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_THETA")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),

                    Procedure.newBuilder().name("STATS_MIN")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_MAX")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("STATS_COL_WIDTH")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.INTEGER))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("SOURCE", SystemColumnImpl
                                    .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                    .getType().getCatalogType())
                            .build(),
                    Procedure.newBuilder().name("PARTITION_EXISTS")
                            .numOutputParams(0)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BOOLEAN))
                            .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                            .arg("CONGLOMERATE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT).getCatalogType())
                            .arg("PARTITION_ID", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR).getCatalogType())
                            .build()
            ));

            SYSFUN_PROCEDURES.addAll(Arrays.asList(
                    //
                    // General functions
                    //
                    Procedure.newBuilder().name("LONG_UUID")
                            .numOutputParams(1)
                            .numResultSets(0)
                            .sqlControl(RoutineAliasInfo.NO_SQL)
                            .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                            .isDeterministic(false).ownerClass(GenericSpliceFunctions.class.getCanonicalName())
                            .build()
            ));
        }catch(StandardException se){
            throw new RuntimeException(se);
        }
    }

}
