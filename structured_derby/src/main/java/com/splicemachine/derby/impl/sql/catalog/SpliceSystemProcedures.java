package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.impl.job.load.load.HdfsImport;
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
                            .varchar("timestampFormat",32672).build();
                    procedures.set(i,newImport);
                }
            }
        }
        return sysProcedures;
    }
}
