package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HbaseGenericConstantActionFactory extends SpliceGenericConstantActionFactory{
    @Override
    public ConstantAction getDropIndexConstantAction(String fullIndexName,String indexName,String tableName,String schemaName,UUID tableId,long tableConglomerateId){
        return new HBaseDropIndexConstantOperation(fullIndexName,indexName,tableName,schemaName,tableId,tableConglomerateId);
    }
}
