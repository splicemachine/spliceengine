package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.impl.load.HdfsImport;
import com.splicemachine.derby.impl.storage.TableSplit;
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
                            .charType("characterDelimiter",1)
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
                }
            }
        }


        return sysProcedures;
    }
}
