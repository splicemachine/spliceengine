package com.splicemachine.derby.stream.spark;

import com.google.common.collect.Maps;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dgomezferro on 1/14/16.
 */
public class ActivationHolder implements Externalizable {
    private Map<Integer, SpliceOperation> operationsMap = Maps.newHashMap();
    private List<SpliceOperation> operationsList = new ArrayList<>();
    private Activation activation;
    private SpliceObserverInstructions soi;
    private TxnView txn;

    public ActivationHolder() {

    }

    public ActivationHolder(Activation activation) {
        this.activation = activation;
        addSubOperations(operationsMap, (SpliceOperation) activation.getResultSet());
        if (activation.getResultSet() != null) {
            operationsList.add((SpliceOperation) activation.getResultSet());
        }

        for (Field field : activation.getClass().getDeclaredFields()) {
            if(!field.getType().isAssignableFrom(SpliceOperation.class)) continue; //ignore qualifiers

            boolean isAccessible = field.isAccessible();
            if(!isAccessible)
                field.setAccessible(true);

            try {
                SpliceOperation so = (SpliceOperation) field.get(activation);
                if (so == null) {
                    continue;
                }
                if (!operationsMap.containsKey(so.resultSetNumber())) {
                    addSubOperations(operationsMap, so);
                    operationsList.add(so);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        soi = SpliceObserverInstructions.create(this);
        txn = getTransaction(activation);
    }

    private TxnView getTransaction(Activation activation) {
        try {
            TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
            Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
            return ((BaseSpliceTransaction) rawStoreXact).getActiveStateTxn();
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    private void addSubOperations(Map<Integer, SpliceOperation> operationsMap, SpliceOperation operation) {
        if (operation == null)
            return;

        operationsMap.put(operation.resultSetNumber(), operation);
        for (SpliceOperation subOp : operation.getSubOperations()) {
            addSubOperations(operationsMap, subOp);
        }
    }

    public Activation getActivation() {
        return activation;
    }

    public Map<Integer, SpliceOperation> getOperationsMap() {
        return operationsMap;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(operationsList);
        out.writeObject(soi);
        TransactionOperations.getOperationFactory().writeTxn(txn, out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        operationsList = (List<SpliceOperation>) in.readObject();
        operationsMap = Maps.newHashMap();
        for (SpliceOperation so : operationsList) {
            addSubOperations(operationsMap, so);
        }
        soi = (SpliceObserverInstructions) in.readObject();
        txn = TransactionOperations.getOperationFactory().readTxn(in);
        SpliceTransactionResourceImpl impl = null;
        boolean prepared = false;
        try {
            impl = new SpliceTransactionResourceImpl();
            impl.prepareContextManager();
            prepared = true;
            impl.marshallTransaction(txn);
            activation = soi.getActivation(this, impl.getLcc());

            SpliceOperationContext context = SpliceOperationContext.newContext(activation);
            for (SpliceOperation so : operationsList) {
                so.init(context);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (prepared) {
                impl.popContextManager();
            }
        }
    }

    public void setActivation(Activation activation) {
        this.activation = activation;
    }

    public TxnView getTxn() {
        return txn;
    }
}
