package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.txn.TxnView;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class MemDropIndexConstantOperation extends AbstractDropIndexConstantOperation{
    /**
     * Make the ConstantAction for a DROP INDEX statement.
     *
     * @param tableId             UUID for table
     * @param tableConglomerateId heap Conglomerate Id for table
     * @param    fullIndexName        Fully qualified index name
     * @param    indexName            Index name.
     * @param    tableName            The table name
     * @param    schemaName            Schema that index lives in.
     */
    public MemDropIndexConstantOperation(String fullIndexName,String indexName,String tableName,String schemaName,UUID tableId,long tableConglomerateId){
        super(fullIndexName,indexName,tableName,schemaName,tableId,tableConglomerateId);
    }

    @Override
    public void dropIndexTrigger(long tableConglomId,long indexConglomId,TxnView userTxn) throws StandardException{
        //no-op, since we use the DDLChange even to remove the index from the CFL
    }
}
