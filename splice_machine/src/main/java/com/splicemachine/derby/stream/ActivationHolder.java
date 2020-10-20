/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedConnectionContext;
import com.splicemachine.db.impl.sql.catalog.ManagedCache;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.serialization.SpliceObserverInstructions;
import com.splicemachine.derby.utils.StatisticsOperation;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Optional;
import splice.com.google.common.collect.Maps;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.sql.SQLException;
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
    private boolean initialized = false;
    private static ThreadLocal<SpliceTransactionResourceImpl> impl = new ThreadLocal<>();
    private String currentUser;
    private List<String> groupUsers = null;
    private ManagedCache<String, Optional<String>> propertyCache = null;

    public ActivationHolder() {

    }

    public ActivationHolder(Activation activation, SpliceOperation operation) {
        this.activation = activation;
        this.initialized = true;
        addSubOperations(operationsMap, operation);
        addSubOperations(operationsMap, (SpliceOperation) activation.getResultSet());
        if(activation.getResultSet()!=null){
            operationsList.add((SpliceOperation) activation.getResultSet());
        }
        if (operation instanceof StatisticsOperation) {
            // special case for StatisticsOperation
            operationsList.add(operation);
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
        this.currentUser = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        this.groupUsers = activation.getLanguageConnectionContext().getCurrentGroupUser(activation);
    }

    public void newTxnResource() throws StandardException{
        try {
            SpliceTransactionResourceImpl txnResource = impl.get();
            boolean needToSet = txnResource == null;
            if (needToSet)
                txnResource = new SpliceTransactionResourceImpl();
            else
                txnResource.close();
            LanguageConnectionContext lcc = getActivation().getLanguageConnectionContext();
            txnResource.marshallTransaction(txn,
                                            lcc.getDataDictionary().getDataDictionaryCache().getPropertyCache(),
                                            lcc.getTransactionExecute(), lcc.getUserName(), lcc.getInstanceNumber());
            if (needToSet)
                impl.set(txnResource);
        }
        catch (SQLException e) {
            throw Exceptions.parseException(e);
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
        if (operation.getSubOperations() != null) {
            for (SpliceOperation subOp : operation.getSubOperations()) {
                addSubOperations(operationsMap, subOp);
            }
        }
    }

    public synchronized Activation getActivation() {
        // Only directly instantiated ActivationHolders have initialized == true
        // Those deserialized will check if impl.get() and activation are both set
        if (!initialized) {
            init(txn, true);
        }
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
        SIDriver.driver().getOperationFactory().writeTxnStack(txn,out);
        if (currentUser != null) {
            out.writeBoolean(true);
            out.writeObject(currentUser);
        } else
            out.writeBoolean(false);
        if (groupUsers != null) {
            out.writeBoolean(true);
            out.writeObject(groupUsers);
        } else
            out.writeBoolean(false);
        out.writeObject(getActivation().getLanguageConnectionContext().getDataDictionary().getDataDictionaryCache().getPropertyCache());
    }

    public LanguageConnectionContext getLCC() {
        SpliceTransactionResourceImpl txnResource = impl.get();
        return txnResource.getLcc();
    }

    private void init(TxnView txn, boolean reinit){
        try {
            SpliceTransactionResourceImpl txnResource = impl.get();
            if (txnResource != null) {
                if (activation != null) return;
                txnResource.close();
            }

            txnResource = new SpliceTransactionResourceImpl();
            txnResource.marshallTransaction(txn, propertyCache);
            impl.set(txnResource);
            if (soi == null)
                soi = SpliceObserverInstructions.create(this);
            activation = soi.getActivation(this, txnResource.getLcc());
            activation.getLanguageConnectionContext().setCurrentUser(activation, currentUser);
            activation.getLanguageConnectionContext().setCurrentGroupUser(activation, groupUsers);

            // Push internal connection to the current context manager
            EmbedConnection internalConnection = (EmbedConnection)EngineDriver.driver().getInternalConnection();
            new EmbedConnectionContext(ContextService.getService().getCurrentContextManager(), internalConnection);

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

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        operationsList = (List<SpliceOperation>) in.readObject();
        operationsMap = Maps.newHashMap();
        for (SpliceOperation so : operationsList) {
            addSubOperations(operationsMap, so);
        }
        soi = (SpliceObserverInstructions) in.readObject();
        txn = SIDriver.driver().getOperationFactory().readTxnStack(in);
        if (in.readBoolean()) {
            currentUser = (String)in.readObject();
        } else
            currentUser = null;
        if (in.readBoolean()) {
            groupUsers = (List<String>)in.readObject();
        } else
            groupUsers = null;
        propertyCache = (ManagedCache<String, Optional<String>>) in.readObject();
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

    public synchronized void reinitialize(TxnView otherTxn, boolean reinit) {
        close();
        init(otherTxn != null ? otherTxn : txn, reinit);
    }

    public void close() {
        SpliceTransactionResourceImpl txnResource = impl.get();
        if (txnResource != null) {
            txnResource.close();
            impl.set(null);
        }
    }
}
