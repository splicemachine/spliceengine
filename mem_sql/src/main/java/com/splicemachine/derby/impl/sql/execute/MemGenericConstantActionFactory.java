package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class MemGenericConstantActionFactory extends SpliceGenericConstantActionFactory{
    @Override
    public ConstantAction getDropIndexConstantAction(String fullIndexName,String indexName,String tableName,String schemaName,UUID tableId,long tableConglomerateId){
        return new MemDropIndexConstantOperation(fullIndexName, indexName, tableName, schemaName, tableId, tableConglomerateId);
    }
}
