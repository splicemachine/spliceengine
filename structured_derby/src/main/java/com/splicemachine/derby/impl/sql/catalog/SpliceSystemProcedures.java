package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.impl.load.HdfsImport;
import com.splicemachine.derby.impl.storage.TableSplit;
import com.splicemachine.derby.impl.storage.TempSplit;

import com.splicemachine.derby.utils.SpliceAdmin;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.catalog.DefaultSystemProcedureGenerator;
import org.apache.derby.impl.sql.catalog.Procedure;

import java.util.List;
import java.util.Map;

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

				    /*
				     * Add a system procedure to enable splitting tables once data is loaded.
				     *
				     * We do this here because this way we know we're in the SYSCS procedure set
				     */
                    Procedure splitProc = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .varchar("splitPoints",32672).build();
                    procedures.add(splitProc);

                    Procedure splitProc2 = Procedure.newBuilder().name("SYSCS_SPLIT_TABLE_EVENLY")
                            .numOutputParams(0).numResultSets(0).ownerClass(TableSplit.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName")
                            .integer("numSplits").build();
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
                            .numOutputParams(0).numResultSets(1).ownerClass(SpliceAdmin.class.getCanonicalName()).build();
                    procedures.add(getActiveServers);

                    /*
                     * Procedure get all active requests
                     */
                    Procedure getRequests = Procedure.newBuilder().name("SYSCS_GET_REQUESTS")
                            .numOutputParams(0).numResultSets(1).ownerClass(SpliceAdmin.class.getCanonicalName()).build();
                    procedures.add(getRequests);

                    /*
                     * Procedure to perform major compaction on all tables in a schema
                     * TODO: finish ipml
                     */
                    Procedure majorComactionOnSchema = Procedure.newBuilder().name("SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA")
                            .numOutputParams(0).numResultSets(0).ownerClass(SpliceAdmin.class.getCanonicalName()).catalog("schemaName").build();
                    procedures.add(majorComactionOnSchema);

                    /*
                     * Procedure to perform major compaction on a table in a schema
                     */
                    Procedure majorComactionOnTable = Procedure.newBuilder().name("SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE")
                            .numOutputParams(0).numResultSets(0).ownerClass(SpliceAdmin.class.getCanonicalName())
                            .catalog("schemaName")
                            .catalog("tableName").build();
                    procedures.add(majorComactionOnTable);
                }
            }
        }


        return sysProcedures;
    }
}
