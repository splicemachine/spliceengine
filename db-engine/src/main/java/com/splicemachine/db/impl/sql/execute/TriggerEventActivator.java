/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.splicemachine.db.impl.sql.execute.TriggerExecutionContext.pushLanguageConnectionContextToCM;

/**
 * Responsible for firing a trigger or set of triggers based on an event.
 */
public class TriggerEventActivator {

    private TriggerInfo triggerInfo;
    private TriggerExecutionContext tec;
    private Map<TriggerEvent, List<GenericTriggerExecutor>> statementExecutorsMap = new HashMap<>();
    private Map<TriggerEvent, List<TriggerDescriptor>> rowExecutorsMap = new HashMap<>();
    private Map<TriggerEvent, List<TriggerDescriptor>> rowConcurrentExecutorsMap = new HashMap<>();
    private Activation activation;
    private ConnectionContext connectionContext;
    private String statementText;
    private UUID tableId;
    private String tableName;
    private boolean triggerExecutionContextPushed;
    private boolean executionStmtValidatorPushed;
    private boolean fromSparkExecution;

    // getLcc() may return a different LanguageConnectionContext at the time
    // we pop the triggerExecutionContext and executionStmtValidator, versus
    // at the time they were pushed.  Saving the original "LCC" at the time of
    // the push ensures that we pop from the correct LCC.  We sometimes need to
    // save two versions LCCs because we may push to the LCC in the activation,
    // and also to the LCC stored in the current ContextManager, if they happen
    // to be different.  Trigger execution requires that both the activation and
    // the current ContextManager both have a triggerExecutionContext.
    private LanguageConnectionContext esvLCC1;
    private LanguageConnectionContext esvLCC2;
    private LanguageConnectionContext tecLCC1;
    private LanguageConnectionContext tecLCC2;
    private FormatableBitSet heapList;
    private ExecutorService executorService;
    private Function<Function<LanguageConnectionContext,Void>, Callable> withContext;

    /**
     * Basic constructor
     * @param tableId     UUID of the target table
     * @param triggerInfo the trigger information
     * @param activation  the activation.
     * @param aiCounters  vector of ai counters
     * @param executorService
     * @param withContext
     */
    public TriggerEventActivator(UUID tableId,
                                 TriggerInfo triggerInfo,
                                 Activation activation,
                                 Vector<AutoincrementCounter> aiCounters, 
                                 ExecutorService executorService, 
                                 Function<Function<LanguageConnectionContext,Void>, Callable> withContext,
                                 FormatableBitSet heapList,
                                 boolean fromSparkExecution) throws StandardException {
        if (triggerInfo == null) {
            return;
        }
        // extrapolate the table name from the triggerdescriptors
        this.tableName = triggerInfo.getTriggerDescriptors()[0].getTableDescriptor().getQualifiedName();
        this.activation = activation;
        this.tableId = tableId;
        this.triggerInfo = triggerInfo;

        StatementContext context = getLcc().getStatementContext();
        if (context != null) {
            this.statementText = context.getStatementText();
        }
        this.heapList = heapList;
        connectionContext = (ConnectionContext) getLcc().getContextManager().getContext(ConnectionContext.CONTEXT_ID);
        // fromSparkExecution has to be set before initTriggerExecContext() as the latter uses this variable
        this.fromSparkExecution = fromSparkExecution;
        initTriggerExecContext(aiCounters);
        setupExecutors(triggerInfo);
        this.executorService = executorService;
        this.withContext = withContext;

    }

    private void pushExecutionStmtValidator() throws StandardException {
        if(!executionStmtValidatorPushed)
        {
            esvLCC1 = getLcc();
            esvLCC1.pushExecutionStmtValidator(tec);
            executionStmtValidatorPushed = true;
            if (activation.getLanguageConnectionContext() != esvLCC1) {
                esvLCC2 = activation.getLanguageConnectionContext();
                esvLCC2.pushExecutionStmtValidator(tec);
            }
            else
                esvLCC2 = null;
        }
    }

    private void popExecutionStmtValidator() throws StandardException {
        if(executionStmtValidatorPushed)
        {
            esvLCC1.popExecutionStmtValidator(tec);
            if (esvLCC2 != null)
                esvLCC2.popExecutionStmtValidator(tec);
            executionStmtValidatorPushed = false;
            esvLCC1 = null;
            esvLCC2 = null;
        }
    }

    private void pushTriggerExecutionContext() throws StandardException {
        if(!triggerExecutionContextPushed)
        {
            tecLCC1 = getLcc();
            tecLCC1.pushTriggerExecutionContext(tec);
            triggerExecutionContextPushed = true;
            if (activation.getLanguageConnectionContext() != tecLCC1) {
                tecLCC2 = activation.getLanguageConnectionContext();
                tecLCC2.pushTriggerExecutionContext(tec);
            }
            else
                tecLCC2 = null;
        }
    }

    private void popTriggerExecutionContext() throws StandardException {
        if(triggerExecutionContextPushed)
        {
            tecLCC1.popTriggerExecutionContext(tec);
            if (tecLCC2 != null)
                tecLCC2.popTriggerExecutionContext(tec);
            tecLCC1 = null;
            tecLCC2 = null;
            triggerExecutionContextPushed = false;
        }
    }

    private void initTriggerExecContext(Vector<AutoincrementCounter> aiCounters) throws StandardException {
        GenericExecutionFactory executionFactory = (GenericExecutionFactory) getLcc().getLanguageConnectionFactory().getExecutionFactory();
        this.tec = executionFactory.getTriggerExecutionContext(
        connectionContext, statementText, triggerInfo.getColumnIds(), triggerInfo.getColumnNames(),
                tableId, tableName, aiCounters, heapList, fromSparkExecution);
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
        // The SET statement cannot be executed in parallel.
        String regex    =   "(^set[\\s]+)|([\\s]+set[\\s]+)";
        Pattern pattern =   Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

        for (TriggerDescriptor td : triggerInfo.getTriggerDescriptors()) {
            TriggerEvent event = td.getTriggerEvent();
            if (td.isRowTrigger()) {
                // disable concurrent trigger for spark execution due to the high overhead
                // to create lcc and TriggerExecutionContext for each row and each trigger,
                // each make non-cached dictionary calls for
                // defaultroles, defaultSchema, storedPreparedStatement ...
                boolean runConcurrently = !fromSparkExecution;
                if (runConcurrently) {
                    for (String sql : td.getTriggerDefinitionList()) {
                        sql = sql.toUpperCase();
                        Matcher matcher = pattern.matcher(sql);
                        if ((sql.contains("INSERT") && sql.contains("INTO")) ||
                                (sql.contains("UPDATE") && sql.contains("SET")) ||
                                (sql.contains("DELETE") && sql.contains("FROM")) ||
                                matcher.find()) {
                            runConcurrently = false;
                        }
                    }
                }
                if (runConcurrently) {
                    addToMap(rowConcurrentExecutorsMap, event, td);
                } else {
                    addToMap(rowExecutorsMap, event, td);
                }
            } else {
                addToStatementTriggersMap(statementExecutorsMap, event, new StatementTriggerExecutor(tec, td, activation, getLcc()));
            }
        }
    }

    /**
     * Handle the given statement event.
     *
     * @param event a trigger event
     */
    public void notifyStatementEvent(TriggerEvent event,
                                     CursorResultSet triggeringResultSet,
                                     boolean deferCleanup) throws StandardException {

        if (statementExecutorsMap.isEmpty()) {
            return;
        }
        List<GenericTriggerExecutor> triggerExecutors = statementExecutorsMap.get(event);
        if (triggerExecutors == null || triggerExecutors.isEmpty()) {
            return;
        }

        try {
            pushExecutionStmtValidator();
            pushTriggerExecutionContext();

            for (GenericTriggerExecutor triggerExecutor : triggerExecutors) {
                // Reset the AI counters to the beginning before firing next trigger.
                tec.resetAICounters(true);
                // Fire the statement or row trigger.
                triggerExecutor.fireTrigger(event, triggeringResultSet, null, deferCleanup);
            }
        } finally {
            popExecutionStmtValidator();
            popTriggerExecutionContext();
        }
    }

    /**
     * Handle the given row event.
     *
     * @param event             a trigger event
     * @param rs                the triggering result set.  Typically a TemporaryRowHolderResultSet but sometimes a BulkTableScanResultSet
     * @param colsReadFromTable columns required from the trigger table by the triggering sql
     */
    public void notifyRowEvent(TriggerEvent event,
                               CursorResultSet rs,
                               int[] colsReadFromTable,
                               boolean deferCleanup) throws StandardException {
        if (rowExecutorsMap.isEmpty()) {
            return;
        }
        List<TriggerDescriptor> triggerExecutors = rowExecutorsMap.get(event);

        boolean sequential = triggerExecutors != null && !triggerExecutors.isEmpty();
        if (!sequential) {
            return;
        }

        try {
            pushExecutionStmtValidator();
            pushTriggerExecutionContext();

            for (TriggerDescriptor td : triggerExecutors) {
                RowTriggerExecutor triggerExecutor = new RowTriggerExecutor(tec, td, activation, getLcc());

                // Reset the AI counters to the beginning before firing next trigger.
                tec.resetAICounters(true);
                // Fire the statement or row trigger.
                triggerExecutor.fireTrigger(event, rs, colsReadFromTable, deferCleanup);
            }
        } finally {
            popExecutionStmtValidator();
            popTriggerExecutionContext();
        }
    }

    /**
     * Handle the given row event.
     *
     * @param event             a trigger event
     * @param rs                the triggering result set.  Typically a TemporaryRowHolderResultSet but sometimes a BulkTableScanResultSet
     * @param colsReadFromTable columns required from the trigger table by the triggering sql
     */
    public List<Future<Void>> notifyRowEventConcurrent(TriggerEvent event,
                               CursorResultSet rs,
                               int[] colsReadFromTable) throws StandardException {

        if (rowConcurrentExecutorsMap.isEmpty()) {
            return Collections.emptyList();
        }
        List<TriggerDescriptor> concurrentTriggerExecutors = rowConcurrentExecutorsMap.get(event);

        boolean concurrent = concurrentTriggerExecutors != null && !concurrentTriggerExecutors.isEmpty();
        if (!concurrent) {
            return Collections.emptyList();
        }

        List<Future<Void>> futures = new ArrayList<>();
        GenericExecutionFactory executionFactory = (GenericExecutionFactory) getLcc().getLanguageConnectionFactory().getExecutionFactory();
        for (TriggerDescriptor td : concurrentTriggerExecutors) {
            futures.add(executorService.submit(withContext.apply(new Function<LanguageConnectionContext,Void>() {
                @Override
                public Void apply(LanguageConnectionContext lcc) {
                    com.splicemachine.db.iapi.services.context.ContextManager cm = null;
                    boolean newContextManager = false;
                    boolean lccPushed = false;
                    boolean tecPushed = false;

                    try {
                        cm = ContextService.getFactory().getCurrentContextManager();
                        if (cm == null) {
                            newContextManager = true;
                            cm = ContextService.getFactory().newContextManager();
                            ContextService.getFactory().setCurrentContextManager(cm);
                        }
                        lccPushed = pushLanguageConnectionContextToCM(lcc, cm);
                        TriggerExecutionContext tec = executionFactory.getTriggerExecutionContext(connectionContext,
                                statementText, triggerInfo.getColumnIds(), triggerInfo.getColumnNames(),
                                tableId, tableName, null, heapList, fromSparkExecution);
                        RowTriggerExecutor triggerExecutor = new RowTriggerExecutor(tec, td, activation, lcc);

                        try {

                            lcc.pushExecutionStmtValidator(tec);
                            lcc.pushTriggerExecutionContext(tec);
                            tecPushed = true;

                            // Reset the AI counters to the beginning before firing next trigger.
                            tec.resetAICounters(true);
                            // Fire the statement or row trigger.
                            triggerExecutor.fireTrigger(event, rs, colsReadFromTable, false);
                        } finally {
                            if (tecPushed) {
                                lcc.popExecutionStmtValidator(tec);
                                lcc.popTriggerExecutionContext(tec);

                            }
                            tec.clearTrigger(false);

                            if (cm != null) {
                                if (lccPushed)
                                    cm.popContext();
                                if (newContextManager)
                                    ContextService.getFactory().resetCurrentContextManager(cm);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }
            })));
        }

        return futures;
    }


    /**
     * Clean up and release resources.
     */
    public void cleanup(boolean deferCleanup) throws StandardException {
        if (tec != null) {
            tec.clearTrigger(deferCleanup);
            popExecutionStmtValidator();
            popTriggerExecutionContext();
        }
    }

    public LanguageConnectionContext getLcc()
    {
        LanguageConnectionContext lcc = null;
        try {
            lcc = ConnectionUtil.getCurrentLCC();
        }
        catch (SQLException e) {
            // Avoid the exception here so we can get a more meaningful
            // stacktrace later on, to help us debug the issue better.
            return activation.getLanguageConnectionContext();
        }
        return lcc;
    }

    private static void addToMap(Map<TriggerEvent, List<TriggerDescriptor>> map, TriggerEvent event, TriggerDescriptor td) {
        List<TriggerDescriptor> genericTriggerExecutors = map.get(event);
        if (genericTriggerExecutors == null) {
            genericTriggerExecutors = new ArrayList<>();
            map.put(event, genericTriggerExecutors);
        }
        genericTriggerExecutors.add(td);
    }

    private static void addToStatementTriggersMap(Map<TriggerEvent, List<GenericTriggerExecutor>> map, TriggerEvent event, GenericTriggerExecutor executor) {
        List<GenericTriggerExecutor> genericTriggerExecutors = map.get(event);
        if (genericTriggerExecutors == null) {
            genericTriggerExecutors = new ArrayList<>();
            map.put(event, genericTriggerExecutors);
        }
        genericTriggerExecutors.add(executor);
    }

    public TriggerExecutionContext getTriggerExecutionContext() { return tec; }
}
