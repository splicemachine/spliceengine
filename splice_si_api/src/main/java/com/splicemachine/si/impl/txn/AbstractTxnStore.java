package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by jleach on 10/17/17.
 */
public abstract class AbstractTxnStore implements TxnSupplier {

    public TxnView getComparableTxn(TxnView txn, long txnId) {
     //   System.out.println("txn -> " + txn + " : " + txnId);
        if (txn == null)
            return txn;
        long subId = txnId & SIConstants.SUBTRANSANCTION_ID_MASK;
        if (subId == 0) return txn;
        else if (txn.getRolledback() != null && txn.getRolledback().contains(subId))
            return getRolledbackTxn(txnId,txn);
        else return getSubTransaction(txn, txnId);
    }

    protected TxnView getRolledbackTxn(long realTxnId,final TxnView txn){
        return new WrappedTxnView(txn) {

            @Override
            public long getTxnId() {
                return realTxnId;
            }

            @Override
            public Txn.State getEffectiveState() {
                return Txn.State.ROLLEDBACK;
            }

            @Override
            public long getEffectiveCommitTimestamp() {
                return -1L;
            }

            @Override
            public Txn.State getState() {
                return Txn.State.ROLLEDBACK;
            }
        };

    }

    private TxnView getSubTransaction(TxnView txn, long subTxnId) {
        return new WrappedTxnView(txn) {
            @Override
            public long getTxnId() {
                return subTxnId;
            }
        };
    }

    @Override
    public HashMap<Long, TxnView> getTransactions(TxnView currentTxn, Set<Long> txnIds) throws IOException {
        return getTransactions(currentTxn, txnIds);
    }

    @Override
    public TxnView getBaseTransaction(TxnView currentTxn, long txnId) throws IOException {
        return getBaseTransaction(currentTxn, txnId,false);
    }

    @Override
    public TxnView getTransaction(TxnView currentTxn,long txnId) throws IOException{
        if(txnId==-1)
            return Txn.ROOT_TRANSACTION;
        return getTransaction(currentTxn, txnId,false);
    }

    @Override
    @SuppressFBWarnings("SF_SWITCH_NO_DEFAULT") //intentional
    public TxnView getTransaction(TxnView currentTxn, long txnId,boolean getDestinationTables) throws IOException{
        TxnView txn = getBaseTransaction(currentTxn, txnId & SIConstants.TRANSANCTION_ID_MASK,getDestinationTables);
        return getComparableTxn(txn,txnId);

    }

    @Override
    public HashMap<Long,TxnView> getBaseTransactions(TxnView currentTxn, Set<Long> txnIds) throws IOException {
        HashMap<Long, TxnView> txns = new HashMap<>(txnIds.size());
        for (long txnId: txnIds) {
            txns.put(txnId,getBaseTransaction(currentTxn, txnId));
        }
        return txns;
    }

}
