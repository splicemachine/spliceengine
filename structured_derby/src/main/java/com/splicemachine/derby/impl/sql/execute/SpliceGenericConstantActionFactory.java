package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.actions.CreatePrimaryKeyConstraintConstantAction;
import com.splicemachine.derby.impl.sql.execute.actions.CreateTableAction;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.impl.sql.execute.*;

import java.util.Properties;

/**
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class SpliceGenericConstantActionFactory extends GenericConstantActionFactory {

    @Override
    public CreateConstraintConstantAction getCreateConstraintConstantAction(String constraintName,
                                                                            int constraintType,
                                                                            boolean forCreateTable,
                                                                            String tableName,
                                                                            UUID tableId,
                                                                            String schemaName,
                                                                            String[] columnNames,
                                                                            IndexConstantAction indexAction,
                                                                            String constraintText,
                                                                            boolean enabled,
                                                                            ConstraintInfo otherConstraint,
                                                                            ProviderInfo[] providerInfo) {
        if(constraintType== DataDictionary.PRIMARYKEY_CONSTRAINT){
            return super.getCreateConstraintConstantAction(constraintName, constraintType,
                    forCreateTable, tableName, tableId,
                    schemaName, columnNames, null,
                    constraintText, enabled, otherConstraint, providerInfo);
        }else
            return super.getCreateConstraintConstantAction(constraintName, constraintType,
                    forCreateTable, tableName, tableId,
                    schemaName, columnNames, indexAction,
                    constraintText, enabled, otherConstraint, providerInfo);
    }

    @Override
    public ConstantAction getCreateTableConstantAction(String schemaName, String tableName,
                                                       int tableType, ColumnInfo[] columnInfo,
                                                       CreateConstraintConstantAction[] constraintActions,
                                                       Properties properties, char lockGranularity,
                                                       boolean onCommitDeleteRows, boolean onRollbackDeleteRows) {
        return new CreateTableAction(schemaName,tableName,tableType,columnInfo,constraintActions,properties,lockGranularity,
                onCommitDeleteRows,onRollbackDeleteRows);
    }
}
