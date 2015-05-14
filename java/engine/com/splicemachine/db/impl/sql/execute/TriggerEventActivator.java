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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.*;

import static com.splicemachine.db.impl.sql.execute.TriggerEvent.*;

/**
 * Responsible for firing a trigger or set of triggers based on an event.
 */
public class TriggerEventActivator {

    private LanguageConnectionContext lcc;
    private TriggerInfo triggerInfo;
    private InternalTriggerExecutionContext tec;
    private Map<TriggerEvent, List<GenericTriggerExecutor>> executors = new HashMap<>();
    private Activation activation;
    private ConnectionContext cc;
    private String statementText;
    private int dmlType;
    private UUID tableId;
    private String tableName;

    /**
     * Basic constructor
     *
     * @param lcc         the lcc
     * @param tc          the xact controller
     * @param triggerInfo the trigger information
     * @param dmlType     Type of DML for which this trigger is being fired.
     * @param activation  the activation.
     * @param aiCounters  vector of ai counters
     */
    public TriggerEventActivator(LanguageConnectionContext lcc,
                                 TransactionController tc,
                                 UUID tableId,
                                 TriggerInfo triggerInfo,
                                 int dmlType,
                                 Activation activation,
                                 Vector aiCounters) throws StandardException {

        if (triggerInfo == null) {
            return;
        }

        // extrapolate the table name from the triggerdescriptors
        tableName = triggerInfo.getTriggerDescriptors()[0].getTableDescriptor().getQualifiedName();

        this.lcc = lcc;
        this.activation = activation;
        this.tableId = tableId;
        this.dmlType = dmlType;
        this.triggerInfo = triggerInfo;

        cc = (ConnectionContext) lcc.getContextManager().getContext(ConnectionContext.CONTEXT_ID);

        this.statementText = lcc.getStatementContext().getStatementText();

        this.tec = ((GenericExecutionFactory) lcc.getLanguageConnectionFactory().getExecutionFactory()).
                getTriggerExecutionContext(lcc, cc, statementText,
                        dmlType, triggerInfo.getColumnIds(), triggerInfo.getColumnNames(), tableId, tableName, aiCounters);
        setupExecutors(triggerInfo);
    }

    /**
     * Reopen the trigger activator.  Just creates a new trigger execution context.  Note that close() still must be
     * called when you are done -- you cannot just do a reopen() w/o a first doing a close.
     */
    void reopen() throws StandardException {
        this.tec = ((GenericExecutionFactory) lcc.getLanguageConnectionFactory().getExecutionFactory()).
                getTriggerExecutionContext(
                        lcc,
                        cc,
                        statementText,
                        dmlType,
                        triggerInfo.getColumnIds(),
                        triggerInfo.getColumnNames(),
                        tableId,
                        tableName, null);
        setupExecutors(triggerInfo);
    }

    private void setupExecutors(TriggerInfo triggerInfo) throws StandardException {
        Map<TriggerEvent, List<TriggerDescriptor>> descriptorMap = new HashMap<>();
        for (TriggerEvent event : values()) {
            descriptorMap.put(event, new ArrayList<TriggerDescriptor>());
        }

        for (TriggerDescriptor td : triggerInfo.getTriggerDescriptors()) {
            switch (td.getTriggerEventMask()) {
                case TriggerDescriptor.TRIGGER_EVENT_INSERT:
                    if (td.isBeforeTrigger()) {
                        descriptorMap.get(BEFORE_INSERT).add(td);
                    } else {
                        descriptorMap.get(AFTER_INSERT).add(td);
                    }
                    break;


                case TriggerDescriptor.TRIGGER_EVENT_DELETE:
                    if (td.isBeforeTrigger()) {
                        descriptorMap.get(BEFORE_DELETE).add(td);
                    } else {
                        descriptorMap.get(AFTER_DELETE).add(td);
                    }
                    break;

                case TriggerDescriptor.TRIGGER_EVENT_UPDATE:
                    if (td.isBeforeTrigger()) {
                        descriptorMap.get(BEFORE_UPDATE).add(td);
                    } else {
                        descriptorMap.get(AFTER_UPDATE).add(td);
                    }
                    break;
                default:
                    if (SanityManager.DEBUG) {
                        SanityManager.THROWASSERT("bad trigger event " + td.getTriggerEventMask());
                    }
            }
        }

        for (TriggerEvent event : values()) {
            executors.put(event, new ArrayList<GenericTriggerExecutor>());
            for (TriggerDescriptor td : descriptorMap.get(event)) {
                GenericTriggerExecutor e = (td.isRowTrigger()) ?
                        new RowTriggerExecutor(tec, td, activation, lcc) :
                        new StatementTriggerExecutor(tec, td, activation, lcc);
                executors.get(event).add(e);
            }
        }
    }

    /**
     * Handle the given event.
     *
     * @param event             a trigger event
     * @param brs               the before result set.  Typically a TemporaryRowHolderResultSet but sometimes a BulkTableScanResultSet
     * @param ars               the after result set. Typically a TemporaryRowHolderResultSet but sometimes a BulkTableScanResultSet
     * @param colsReadFromTable columns required from the trigger table by the triggering sql
     */
    public void notifyEvent(TriggerEvent event,
                            CursorResultSet brs,
                            CursorResultSet ars,
                            int[] colsReadFromTable) throws StandardException {

        if (executors.isEmpty()) {
            return;
        }
        List<GenericTriggerExecutor> triggerExecutors = executors.get(event);
        if (triggerExecutors == null || triggerExecutors.isEmpty()) {
            return;
        }

        tec.setCurrentTriggerEvent(event);
        try {
            if (brs != null) {
                brs.open();
            }
            if (ars != null) {
                ars.open();
            }

            lcc.pushExecutionStmtValidator(tec);
            for (int i = 0; i < triggerExecutors.size(); i++) {
                if (i > 0) {

                    if (brs != null) {
                        ((NoPutResultSet) brs).reopenCore();
                    }
                    if (ars != null) {
                        ((NoPutResultSet) ars).reopenCore();
                    }
                }
                // Reset the AI counters to the beginning before firing next trigger.
                tec.resetAICounters(true);
                // Fire the statement or row trigger.
                GenericTriggerExecutor genericTriggerExecutor = triggerExecutors.get(i);
                genericTriggerExecutor.fireTrigger(event, brs, ars, colsReadFromTable);
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
}
