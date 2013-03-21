package com.splicemachine.derby.utils;

import com.splicemachine.constants.ITransactionGetsPuts;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.hbase.txn.ZkTransactionGetsPuts;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.txn.SiGetsPuts;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 3/21/13
 */
public class Mutations {

    private Mutations(){}

    public static Mutation getDeleteOp(String transactionId,byte[] row) throws IOException{
        if(SpliceUtils.useSi){
            final ClientTransactor clientTransactor = TransactorFactory.getDefaultClientTransactor();
            return (Mutation)clientTransactor.newDeletePut(clientTransactor.transactionIdFromString(transactionId),row);
        }else{
            Delete delete = new Delete(row);
            if(transactionId!=null)
                delete.setAttribute(TxnConstants.TRANSACTION_ID,transactionId.getBytes());
            return delete;
        }
    }

    public static Mutation translateToDelete(Mutation mutation, byte[] newRowKey) throws IOException {
        String txnId = mutation instanceof Put? SpliceUtils.getTransactionGetsPuts().getTransactionIdForPut((Put)mutation):
                SpliceUtils.getTransactionGetsPuts().getTransactionIdForDelete((Delete)mutation);
        return getDeleteOp(txnId,newRowKey);
    }

    public static Put translateToPut(Mutation mutation, byte[] newRowKey){
        Put put = newRowKey!=null?new Put(newRowKey): new Put(mutation.getRow());
        String txnId = SpliceUtils.getTransactionId(mutation);
        SpliceUtils.getTransactionGetsPuts().prepPut(txnId,put);
        return put;
    }
}
