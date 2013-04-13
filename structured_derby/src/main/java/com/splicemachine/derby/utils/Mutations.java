package com.splicemachine.derby.utils;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.si.api.ClientTransactor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;

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
    public static Delete createDelete(String transactionId, byte[] row) throws IOException {
        return SpliceUtils.createDelete(transactionId, row);
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
    public static Mutation getDeleteOp(String transactionId, byte[] row) throws IOException{
        return SpliceUtils.createDeletePut(transactionId, row);
    }

    public static Mutation getDeleteOp(Mutation mutation, byte[] row) throws IOException{
        return SpliceUtils.createDeletePut(mutation, row);
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
        final byte[] rowKey = newRowKey != null ? newRowKey : mutation.getRow();
        return getDeleteOp(mutation, rowKey);
    }

    /**
     * Convert the specified mutation into a Put.
     *
     * @param mutation the mutation to convert
     * @param newRowKey the row key to use for the Put, or {@code null} if the row of the
     *                  passed in mutation is to be used.
     * @return a transactionally-aware Put operation.
     */
    public static Put translateToPut(Mutation mutation, byte[] newRowKey) throws IOException {
        byte[] rowKey = newRowKey==null ? mutation.getRow() : newRowKey;
        return SpliceUtils.createPut(rowKey, mutation);
    }

    public static boolean isDelete(Mutation mutation){
        return SpliceUtils.isDelete(mutation);
    }

    public static Collection<Mutation> filterDeletes(Collection<Mutation> mutations){
        return Collections2.filter(mutations, stripDeletes);
    }

    private static final Predicate<? super Mutation> stripDeletes = new Predicate<Mutation>() {
        @Override
        public boolean apply(@Nullable Mutation input) {
            return !Mutations.isDelete(input);
        }
    };
}
