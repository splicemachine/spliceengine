package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.IndexDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.CreateConstraintConstantAction;
import org.apache.derby.impl.sql.execute.CreateTableConstantAction;

import java.util.Properties;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class CreateTableAction extends CreateTableConstantAction {
    /**
     * Make the ConstantAction for a CREATE TABLE statement.
     *
     * @param schemaName           name for the schema that table lives in.
     * @param tableName            Name of table.
     * @param tableType            Type of table (e.g., BASE, global temporary table).
     * @param columnInfo           Information on all the columns in the table.
     *                             (REMIND tableDescriptor ignored)
     * @param constraintActions    CreateConstraintConstantAction[] for constraints
     * @param properties           Optional table properties
     * @param lockGranularity      The lock granularity.
     * @param onCommitDeleteRows   If true, on commit delete rows else on commit preserve rows of temporary table.
     * @param onRollbackDeleteRows If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
     */
    public CreateTableAction(String schemaName, String tableName, int tableType, ColumnInfo[] columnInfo, CreateConstraintConstantAction[] constraintActions, Properties properties, char lockGranularity, boolean onCommitDeleteRows, boolean onRollbackDeleteRows) {
        super(schemaName, tableName, tableType, columnInfo, constraintActions, properties, lockGranularity, onCommitDeleteRows, onRollbackDeleteRows);
    }

    @Override
    protected ConglomerateDescriptor getTableConglomerateDescriptor(TableDescriptor td, long conglomId, SchemaDescriptor sd, DataDescriptorGenerator ddg) throws StandardException {
        /*
         * If there is a PrimaryKey Constraint, build an IndexRowGenerator that returns the Primary Keys,
         * otherwise, do whatever Derby does by default
         */
        if(constraintActions ==null)
            return super.getTableConglomerateDescriptor(td,conglomId,sd,ddg);

        for(CreateConstraintConstantAction constantAction:constraintActions){
            if(constantAction.getConstraintType()== DataDictionary.PRIMARYKEY_CONSTRAINT){
                int [] pkColumns = constantAction.genColumnPositions(td,true);
                boolean[] ascending = new boolean[pkColumns.length];
                for(int i=0;i<ascending.length;i++){
                   ascending[i] = true;
                }

                IndexDescriptor descriptor = new IndexDescriptorImpl("PRIMARYKEY",true,false,pkColumns,ascending,pkColumns.length);
                IndexRowGenerator irg = new IndexRowGenerator(descriptor);
                return ddg.newConglomerateDescriptor(conglomId,null,false,irg,false,null,td.getUUID(),sd.getUUID());
            }
        }
        return super.getTableConglomerateDescriptor(td,conglomId,sd,ddg);
    }
}
