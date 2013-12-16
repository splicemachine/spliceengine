package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.impl.load.HdfsImport;
import com.splicemachine.derby.impl.storage.TableSplit;
import com.splicemachine.derby.impl.storage.TempSplit;
import com.splicemachine.derby.utils.SpliceAdmin;
import java.util.List;
import java.util.Map;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.catalog.DefaultSystemProcedureGenerator;
import org.apache.derby.impl.sql.catalog.Procedure;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceSystemProcedures extends DefaultSystemProcedureGenerator {

    private static final String IMPORT_DATA_NAME = "SYSCS_IMPORT_DATA";

    public SpliceSystemProcedures(DataDictionary dictionary) {
        super(dictionary);
    }

    @Override
    protected Map/*<UUID,List<Procedure>*/ getProcedures(DataDictionary dictionary, TransactionController tc) throws StandardException {
        Map sysProcedures =  super.getProcedures(dictionary, tc);

        // SYS_UTIL schema
        UUID sysUUID = dictionary.getSystemUtilSchemaDescriptor().getUUID();

        /*
         * Our import process is different than Vanilla Derby's, so find that Procedure in
         * the map and replace it with our own Procedure
         */
        for(Object key : sysProcedures.keySet()){
            List<Procedure> procedures = (List<Procedure>)sysProcedures.get(key);
            for(int i=0;i<procedures.size();i++){
                Procedure sysProc = procedures.get(i);
                if(IMPORT_DATA_NAME.equalsIgnoreCase(sysProc.getName())){
                    Procedure newImport = Procedure.newBuilder().name("SYSCS_IMPORT_DATA")
                            .numOutputParams(0).numResultSets(0).ownerClass(HdfsImport.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("insertColumnList",32672)
                            .varchar("columnIndexes",32672)
                            .varchar("fileName",32672)
                            .varchar("columnDelimiter",5)
                            .charType("characterDelimiter",5)
                            .varchar("timestampFormat",32672)
                            .varchar("dateFormat",32672)
                            .varchar("timeFormat",32672).build();
                    procedures.set(i,newImport);
                }
            }

            if (key.equals(sysUUID)) {
                // Create splice utility procedures
                /*
                 * Add a system procedure to enable splitting tables once data is loaded.
                 *
                 * We do this here because this way we know we're in the SYSCS procedure set
                 */
                    Procedure splitProc = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("splitPoints", 32672)
                            .build();
                    procedures.add(splitProc);

                    Procedure splitProc2 = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE_EVENLY")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .integer("numSplits")
                            .build();
                    procedures.add(splitProc2);

                /*
                 * Procedure to split TEMP table into 16 evenly distributed buckets
                 */
                    Procedure splitTemp = Procedure.newBuilder().name("SYSCS_SPLIT_TEMP")
                            .numOutputParams(0).numResultSets(0).ownerClass(TempSplit.class.getCanonicalName()).build();
                    procedures.add(splitTemp);

                /*
                 * Procedure get all active services
                 */
                    Procedure getActiveServers = Procedure.newBuilder().name("SYSCS_GET_ACTIVE_SERVERS")
                            .numOutputParams(0)
                            .numResultSets(1)
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
                 * Procedure get info for the write pipeline
                 */
                Procedure getWritePipelineInfo = Procedure.newBuilder().name("SYSCS_GET_WRITE_PIPELINE_INFO")
                        .numOutputParams(0)
                        .numResultSets(1)
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .build();
                procedures.add(getWritePipelineInfo);

                /*
                 * Procedure get task info for region servers
                 */
                Procedure getRegionServerTaskInfo = Procedure.newBuilder().name("SYSCS_GET_REGION_SERVER_TASK_INFO")
                        .numOutputParams(0)
                        .numResultSets(1)
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .build();
                procedures.add(getRegionServerTaskInfo);

                /*
                 * Procedure get stats info for region servers
                 */
                Procedure getRegionServerStatsInfo = Procedure.newBuilder().name("SYSCS_GET_REGION_SERVER_STATS_INFO")
                        .numOutputParams(0)
                        .numResultSets(1)
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .build();
                procedures.add(getRegionServerStatsInfo);

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
                 * Procedure set the max task workers
                 */
                Procedure setMaxTasks = Procedure.newBuilder().name("SYSCS_SET_MAX_TASKS")
                        .numOutputParams(0)
                        .numResultSets(0)
												.integer("workerTier")
                        .integer("maxWorkers")
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .build();
                procedures.add(setMaxTasks);

								Procedure getMaxTasks = Procedure.newBuilder().name("SYSCS_GET_GLOBAL_MAX_TASKS")
												.numOutputParams(0)
												.numResultSets(1)
												.ownerClass(SpliceAdmin.class.getCanonicalName())
												.build();
								procedures.add(getMaxTasks);

                /*
                 * Procedure set the max task workers
                 */
                Procedure getTieredMaxTasks = Procedure.newBuilder().name("SYSCS_GET_MAX_TASKS")
                        .numOutputParams(0)
                        .numResultSets(1)
												.integer("workerTier")
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .build();
                procedures.add(getTieredMaxTasks);

                /*
                 * Procedure set max write thread pool count
                 */
                Procedure setWritePool = Procedure.newBuilder().name("SYSCS_SET_WRITE_POOL")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .integer("writePool")
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .build();
                procedures.add(setWritePool);

                /*
                 * Procedure get the max write pool threads
                 */
                Procedure getWritePool = Procedure.newBuilder().name("SYSCS_GET_WRITE_POOL")
                        .numOutputParams(0)
                        .numResultSets(1)
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .build();
                procedures.add(getWritePool);

                /*
                 * Procedure to perform major compaction on all tables in a schema
                 * TODO: finish impl
                 */
                Procedure majorComactionOnSchema = Procedure.newBuilder().name("SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .catalog("schemaName").build();
                procedures.add(majorComactionOnSchema);

                /*
                 * Procedure to perform major compaction on a table in a schema
                 */
                Procedure majorComactionOnTable = Procedure.newBuilder().name("SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .ownerClass(SpliceAdmin.class.getCanonicalName())
                        .catalog("schemaName")
                        .catalog("tableName").build();
                procedures.add(majorComactionOnTable);
            }
        }

        return sysProcedures;
    }
}
