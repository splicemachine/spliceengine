package com.splicemachine.derby.utils;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.si.api.ClientTransactor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * Utilities for operating with Mutations.
 *
 * @author Scott Fines
 * Created on: 3/21/13
 */
public class Mutations {

    private Mutations(){}

    /**
     * Create an actual Delete instance.
     *
     * Unlike {@link #getDeleteOp(String, byte[])}, this method is guaranteed to return
     * a Delete instance that is properly managed transactionally.
     *
     * @param transactionId the transaction id for the delete
     * @param row the row to delete
     * @return a transactionally-aware delete.
     */
    public static Delete createDelete(String transactionId, byte[] row) {
        Delete delete = new Delete(row);
        SpliceUtils.attachTransaction(delete,transactionId);
        return delete;
    }

    /**
     * Create a Delete operation.
     *
     * Because of HBase locking semantics, it's not always possible to use an actual HBase Delete
     * option. In that case, we must use a Put that is "tagged" and interpreted as a delete.
     * This method provides a "delete" operation that is either a Delete or a Put depending
     * on the transactional type.
     *
     * @param transactionId the transaction id to attach.
     * @param row the row to delete
     * @return a transactionally-aware delete operation, although not necessary a Delete
     * instance. E.g. (getDeleteOp() instanceof Delete) is not guaranteed to be true
     * @throws IOException if something goes wrong.
     */
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

    /**
     * Convert the specified mutation into a delete operation.
     *
     * @param mutation the mutation to convert
     * @param newRowKey the row key to delete, or {@code null} if the row of the passed in
     *                  mutation is to be used.
     * @return a transactionally-aware delete operation, although not necessarily a Delete
     * instance. (e.g. (translateToDelete() instanceof Delete) is not guaranteeed to be true.
     * @throws IOException if something goes wrong.
     */
    public static Mutation translateToDelete(Mutation mutation, byte[] newRowKey) throws IOException {
        String txnId = mutation instanceof Put? SpliceUtils.getTransactionGetsPuts().getTransactionIdForPut((Put)mutation):
                SpliceUtils.getTransactionGetsPuts().getTransactionIdForDelete((Delete)mutation);
        return getDeleteOp(txnId,newRowKey!=null?newRowKey:mutation.getRow());
    }

    /**
     * Convert the specified mutation into a Put.
     *
     * @param mutation the mutation to convert
     * @param newRowKey the row key to use for the Put, or {@code null} if the row of the
     *                  passed in mutation is to be used.
     * @return a transactionally-aware Put operation.
     */
    public static Put translateToPut(Mutation mutation, byte[] newRowKey){
        Put put = newRowKey!=null?new Put(newRowKey): new Put(mutation.getRow());
        String txnId = SpliceUtils.getTransactionId(mutation);
        SpliceUtils.getTransactionGetsPuts().prepPut(txnId,put);
        return put;
    }
}
