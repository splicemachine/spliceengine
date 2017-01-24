/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.reference.ContextId;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Maps;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.serialization.SpliceObserverInstructions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class used to serialize (and reference) the operation tree and the activation for Spark. It references
 * operations by resultSetNumber, it only serializes the roots (resultSet, subqueries, etc.) and maintains
 * references consistent when they reference the same operation from different fields in the activation.
 *
 * Created by dgomezferro on 1/14/16.
 */
@NotThreadSafe
public class ActivationHolder implements Externalizable {
    private static final Logger LOG = Logger.getLogger(ActivationHolder.class);

    private Map<Integer, SpliceOperation> operationsMap = Maps.newHashMap();
    private List<SpliceOperation> operationsList = new ArrayList<>();
    private Activation activation;
    private SpliceObserverInstructions soi;
    private TxnView txn;
    private boolean initialized=false;
    private SpliceTransactionResourceImpl impl;
    private boolean prepared = false;

    public ActivationHolder() {

    }

    public ActivationHolder(Activation activation, SpliceOperation operation) {
        this.activation = activation;
        this.initialized = true;
        addSubOperations(operationsMap, (SpliceOperation) activation.getResultSet());
        if(activation.getResultSet()!=null){
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
        try {
            txn = operation != null ? operation.getCurrentTransaction() : getTransaction(activation);
        } catch (StandardException e) {
            LOG.warn("Exception getting transaction from " + operation + ", falling back to activation");
            txn = getTransaction(activation);
        }
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
        init();
        return activation;
    }

    public Map<Integer, SpliceOperation> getOperationsMap() {
        return operationsMap;
    }

    @Override
    public synchronized void writeExternal(ObjectOutput out) throws IOException {
        if(soi==null){
            soi = SpliceObserverInstructions.create(this);
        }
        out.writeObject(operationsList);
        out.writeObject(soi);
        SIDriver.driver().getOperationFactory().writeTxn(txn,out);
    }

    public void init(){
        init(txn);
    }

    public synchronized void init(TxnView txn){
        if(initialized)
            return;
        initialized = true;
        try {
            impl = new SpliceTransactionResourceImpl();
            prepared =  impl.marshallTransaction(txn);
            activation = soi.getActivation(this, impl.getLcc());

            SpliceOperationContext context = SpliceOperationContext.newContext(activation);
            for(SpliceOperation so: operationsList){
                so.init(context);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (prepared) {
                impl.close();
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        operationsList = (List<SpliceOperation>) in.readObject();
        operationsMap = Maps.newHashMap();
        for (SpliceOperation so : operationsList) {
            addSubOperations(operationsMap, so);
        }
        soi = (SpliceObserverInstructions) in.readObject();
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
    }

    public void setActivation(Activation activation) {
        this.activation = activation;
    }

    public TxnView getTxn() {
        return txn;
    }

    public void reinitialize(TxnView otherTxn) {
        reinitialize(otherTxn, true);
    }

    public void reinitialize(TxnView otherTxn, boolean reinit) {
        TxnView txnView = otherTxn!=null ? otherTxn : this.txn;
        initialized = true;
        try {
            impl = new SpliceTransactionResourceImpl();
            prepared =  impl.marshallTransaction(txnView);
            activation = soi.getActivation(this, impl.getLcc());

            Context statementContext = ContextService.getContext(ContextId.LANG_STATEMENT);
            EmbedConnection internalConnection = (EmbedConnection)EngineDriver.driver().getInternalConnection();
            internalConnection.getContextManager().pushContext(statementContext);

            if (reinit) {
                SpliceOperationContext context = SpliceOperationContext.newContext(activation);
                for (SpliceOperation so : operationsList) {
                    so.init(context);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (prepared) {
            impl.close();
            prepared = false;
        }

        EmbedConnection internalConnection=(EmbedConnection)EngineDriver.driver().getInternalConnection();
        internalConnection.getContextManager().popContext();
    }
}