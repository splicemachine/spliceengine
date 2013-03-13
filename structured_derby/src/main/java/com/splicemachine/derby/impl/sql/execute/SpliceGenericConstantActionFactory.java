package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.actions.CreateTableAction;
import com.splicemachine.derby.impl.sql.execute.actions.DropIndexOperation;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.impl.sql.execute.*;
import com.splicemachine.derby.impl.sql.execute.actions.CreateIndexOperation;

import java.util.Properties;

/**
 * @author Scott Fines
 * Created on: 3/1/13
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
                                                                            ConstantAction indexAction,
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
        return new CreateTableAction(schemaName,tableName,tableType,columnInfo,
                constraintActions,properties,lockGranularity,
                onCommitDeleteRows,onRollbackDeleteRows);
    }

    @Override
    public ConstantAction getCreateIndexConstantAction(boolean forCreateTable,
                                                       boolean unique,
                                                       boolean uniqueWithDuplicateNulls,
                                                       String indexType, String schemaName,
                                                       String indexName, String tableName,
                                                       UUID tableId, String[] columnNames,
                                                       boolean[] isAscending, boolean isConstraint,
                                                       UUID conglomerateUUID, Properties properties) {
        return new CreateIndexOperation(schemaName,indexName,tableName,columnNames,isAscending,tableId,
                conglomerateUUID,unique,indexType,properties);
    }

    @Override
    public ConstantAction getDropIndexConstantAction(String fullIndexName,
                                                     String indexName,
                                                     String tableName,
                                                     String schemaName,
                                                     UUID tableId,
                                                     long tableConglomerateId) {
        return new DropIndexOperation(fullIndexName,indexName,
                tableName,schemaName,tableId,tableConglomerateId);
    }
}
