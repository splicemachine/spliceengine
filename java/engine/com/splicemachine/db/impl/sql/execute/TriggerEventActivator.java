/*

   Derby - Class org.apache.derby.impl.sql.execute.TriggerEventActivator

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.*;

/**
 * Responsible for firing a trigger or set of triggers based on an event.
 */
public class TriggerEventActivator {

    private LanguageConnectionContext lcc;
    private TriggerInfo triggerInfo;
    private InternalTriggerExecutionContext tec;
    private Map<TriggerEvent, List<GenericTriggerExecutor>> statementExecutorsMap = new HashMap<>();
    private Map<TriggerEvent, List<GenericTriggerExecutor>> rowExecutorsMap = new HashMap<>();
    private Activation activation;
    private ConnectionContext cc;
    private String statementText;
    private UUID tableId;
    private String tableName;

    /**
     * Basic constructor
     *
     * @param lcc         the lcc
     * @param tc          the xact controller
     * @param triggerInfo the trigger information
     * @param activation  the activation.
     * @param aiCounters  vector of ai counters
     */
    public TriggerEventActivator(LanguageConnectionContext lcc,
                                 TransactionController tc,
                                 UUID tableId,
                                 TriggerInfo triggerInfo,
                                 Activation activation,
                                 Vector<AutoincrementCounter> aiCounters) throws StandardException {
        if (triggerInfo == null) {
            return;
        }
        // extrapolate the table name from the triggerdescriptors
        this.tableName = triggerInfo.getTriggerDescriptors()[0].getTableDescriptor().getQualifiedName();
        this.lcc = lcc;
        this.activation = activation;
        this.tableId = tableId;
        this.triggerInfo = triggerInfo;
        this.cc = (ConnectionContext) lcc.getContextManager().getContext(ConnectionContext.CONTEXT_ID);
        this.statementText = lcc.getStatementContext().getStatementText();

        initTriggerExecContext(aiCounters);
        setupExecutors(triggerInfo);
    }

    private void initTriggerExecContext(Vector<AutoincrementCounter> aiCounters) throws StandardException {
        GenericExecutionFactory executionFactory = (GenericExecutionFactory) lcc.getLanguageConnectionFactory().getExecutionFactory();
        this.tec = executionFactory.getTriggerExecutionContext(
                lcc, cc, statementText, triggerInfo.getColumnIds(), triggerInfo.getColumnNames(),
                tableId, tableName, aiCounters);
    }

    /**
     * Reopen the trigger activator.  Just creates a new trigger execution context.  Note that close() still must be
     * called when you are done -- you cannot just do a reopen() w/o a first doing a close.
     */
    void reopen() throws StandardException {
        initTriggerExecContext(null);
        setupExecutors(triggerInfo);
    }

    private void setupExecutors(TriggerInfo triggerInfo) throws StandardException {
        for (TriggerDescriptor td : triggerInfo.getTriggerDescriptors()) {
            TriggerEvent event = td.getTriggerEvent();
            if (td.isRowTrigger()) {
                addToMap(rowExecutorsMap, event, new RowTriggerExecutor(tec, td, activation, lcc));
            } else {
                addToMap(statementExecutorsMap, event, new StatementTriggerExecutor(tec, td, activation, lcc));
            }
        }
    }

    /**
     * Handle the given statement event.
     *
     * @param event a trigger event
     */
    public void notifyStatementEvent(TriggerEvent event) throws StandardException {

        if (statementExecutorsMap.isEmpty()) {
            return;
        }
        List<GenericTriggerExecutor> triggerExecutors = statementExecutorsMap.get(event);
        if (triggerExecutors == null || triggerExecutors.isEmpty()) {
            return;
        }

        tec.setCurrentTriggerEvent(event);
        try {
            lcc.pushExecutionStmtValidator(tec);
            for (GenericTriggerExecutor triggerExecutor : triggerExecutors) {
                // Reset the AI counters to the beginning before firing next trigger.
                tec.resetAICounters(true);
                // Fire the statement or row trigger.
                triggerExecutor.fireTrigger(event, null, null, null);
            }
        } finally {
            lcc.popExecutionStmtValidator(tec);
            tec.clearCurrentTriggerEvent();
        }
    }

    /**
     * Handle the given row event.
     *
     * @param event             a trigger event
     * @param brs               the before result set.  Typically a TemporaryRowHolderResultSet but sometimes a BulkTableScanResultSet
     * @param ars               the after result set. Typically a TemporaryRowHolderResultSet but sometimes a BulkTableScanResultSet
     * @param colsReadFromTable columns required from the trigger table by the triggering sql
     */
    public void notifyRowEvent(TriggerEvent event,
                               CursorResultSet brs,
                               CursorResultSet ars,
                               int[] colsReadFromTable) throws StandardException {

        if (rowExecutorsMap.isEmpty()) {
            return;
        }
        List<GenericTriggerExecutor> triggerExecutors = rowExecutorsMap.get(event);
        if (triggerExecutors == null || triggerExecutors.isEmpty()) {
            return;
        }

        tec.setCurrentTriggerEvent(event);
        try {
            lcc.pushExecutionStmtValidator(tec);
            for (GenericTriggerExecutor triggerExecutor : triggerExecutors) {
                // Reset the AI counters to the beginning before firing next trigger.
                tec.resetAICounters(true);
                // Fire the statement or row trigger.
                triggerExecutor.fireTrigger(event, brs, ars, colsReadFromTable);
            }
        } finally {
            lcc.popExecutionStmtValidator(tec);
            tec.clearCurrentTriggerEvent();
        }
    }

    /**
     * Clean up and release resources.
     */
    public void cleanup() throws StandardException {
        if (tec != null) {
            tec.cleanup();
        }
    }

    private static void addToMap(Map<TriggerEvent, List<GenericTriggerExecutor>> map, TriggerEvent event, GenericTriggerExecutor executor) {
        List<GenericTriggerExecutor> genericTriggerExecutors = map.get(event);
        if (genericTriggerExecutors == null) {
            genericTriggerExecutors = new ArrayList<>();
            map.put(event, genericTriggerExecutors);
        }
        genericTriggerExecutors.add(executor);
    }
}
