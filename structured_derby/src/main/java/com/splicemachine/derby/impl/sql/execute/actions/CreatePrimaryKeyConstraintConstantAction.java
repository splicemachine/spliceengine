package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.impl.sql.execute.ConstraintInfo;
import org.apache.derby.impl.sql.execute.CreateConstraintConstantAction;
import org.apache.derby.impl.sql.execute.IndexConstantAction;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class CreatePrimaryKeyConstraintConstantAction extends CreateConstraintConstantAction{
    /**
     * Make one of these puppies.
     *
     * @param constraintName  Constraint name.
     * @param constraintType  Constraint type.
     * @param forCreateTable  Constraint is being added for a CREATE TABLE
     * @param tableName       Table name.
     * @param tableId         UUID of table.
     * @param schemaName      the schema that table and constraint lives in.
     * @param columnNames     String[] for column names
     * @param indexAction     IndexConstantAction for constraint (if necessary)
     * @param constraintText  Text for check constraint
     *                        RESOLVE - the next parameter should go away once we use UUIDs
     *                        (Generated constraint names will be based off of uuids)
     * @param enabled         Should the constraint be created as enabled
     *                        (enabled == true), or disabled (enabled == false).
     * @param otherConstraint information about the constraint that this references
     * @param providerInfo    Information on all the Providers
     */
    public CreatePrimaryKeyConstraintConstantAction(String constraintName,
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
        super(constraintName, constraintType, forCreateTable, tableName, tableId, schemaName, columnNames, indexAction, constraintText, enabled, otherConstraint, providerInfo);
    }
}
