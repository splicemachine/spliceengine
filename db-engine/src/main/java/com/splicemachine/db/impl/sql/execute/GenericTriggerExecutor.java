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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.SPSDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.StatementContext;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.impl.sql.GenericPreparedStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A trigger executor is an object that executes a trigger.  It is subclassed by row and statement executors.
 */
public abstract class GenericTriggerExecutor {

    protected TriggerExecutionContext tec;
    protected TriggerDescriptor triggerd;
    protected Activation activation;
    protected LanguageConnectionContext lcc;

    private boolean whenClauseRetrieved;
    private List<Boolean> actionRetrievedList;
    private SPSDescriptor whenClause;
    private List<SPSDescriptor> actionList;

    // Cached prepared statement and activation for WHEN clause and
    // trigger action.
    private ExecPreparedStatement       whenPS;
    private Activation                  spsWhenActivation;
    private List<ExecPreparedStatement> actionPSList;
    private List<Activation>            spsActionActivationList;

    /**
     * Constructor
     *
     * @param tec        the execution context
     * @param triggerd   the trigger descriptor
     * @param activation the activation
     * @param lcc        the lcc
     */
    GenericTriggerExecutor(TriggerExecutionContext tec,
                           TriggerDescriptor triggerd,
                           Activation activation,
                           LanguageConnectionContext lcc) {
        this.tec = tec;
        this.triggerd = triggerd;
        this.activation = activation;
        this.lcc = lcc;
        int actionSize = triggerd.getTriggerDefinitionSize();
        this.actionRetrievedList = new ArrayList<>(Collections.nCopies(actionSize, false));
        this.actionList = new ArrayList<>(Collections.nCopies(actionSize, null));
        this.actionPSList = new ArrayList<>(Collections.nCopies(actionSize, null));
        this.spsActionActivationList = new ArrayList<>(Collections.nCopies(actionSize, null));
    }

    /**
     * Fire the trigger based on the event.
     *
     * @param event             the trigger event
     * @param rs                the triggering result set
     * @param colsReadFromTable columns required from the trigger table by the triggering sql
     */
    abstract void fireTrigger(TriggerEvent event,
                              CursorResultSet rs,
                              int[] colsReadFromTable,
                              boolean deferCleanup) throws StandardException;

    private SPSDescriptor getWhenClause() throws StandardException {
        if (!whenClauseRetrieved) {
            whenClauseRetrieved = true;
            whenClause = triggerd.getWhenClauseSPS(lcc, tec.getSpsCache());
        }
        return whenClause;
    }

    private SPSDescriptor getAction(int index) throws StandardException {
        if (!actionRetrievedList.get(index)) {
            actionRetrievedList.set(index, true);
            actionList.set(index, triggerd.getActionSPS(lcc, index, tec.getSpsCache()));
        }
        return actionList.get(index);
    }

    /**
     * Execute the given stored prepared statement.  We
     * just grab the prepared statement from the spsd,
     * get a new activation holder and let er rip.
     *
     * @param sps the SPS to execute
     * @param index {@code -1} if the SPS is for the WHEN clause,
     *               {@code >= 0} for one of the ACTIONS
     * @return {@code true} if the SPS is for a WHEN clause and it evaluated
     *         to {@code TRUE}, {@code false} otherwise
     * @exception StandardException on error
     */
    protected boolean executeSPS(SPSDescriptor sps, int index) throws StandardException {
        boolean recompile = false;
        boolean whenClauseWasTrue = false;
        boolean isWhen = index == -1;

        // The prepared statement and the activation may already be available
        // if the trigger has been fired before in the same statement. (Only
        // happens with row triggers that are triggered by a statement that
        // touched multiple rows.) The WHEN clause and the trigger action have
        // their own prepared statement and activation. Fetch the correct set.
        ExecPreparedStatement ps = isWhen ? whenPS : actionPSList.get(index);
        Activation spsActivation = isWhen
                ? spsWhenActivation : spsActionActivationList.get(index);

        while (true) {
            /*
            ** Only grab the ps the 1st time through.  This
            ** way a row trigger doesn't do any unnecessary
            ** setup work.
            */
            if (ps == null || spsActivation == null || recompile) {

                compile(sps, ps, index);
                spsActivation = isWhen ? spsWhenActivation : spsActionActivationList.get(index);
                ps = isWhen ? whenPS : actionPSList.get(index);
            }

            // save the active statement context for exception handling purpose
            StatementContext active_sc = lcc.getStatementContext();
            
            /*
            ** Execute the activation.  If we have an error, we
            ** are going to go to some extra work to pop off
            ** our statement context.  This is because we are
            ** a nested statement (we have 2 activations), but
            ** we aren't a nested connection, so we have to
            ** pop off our statementcontext to get error handling    
            ** to work correctly.  This is normally a no-no, but
            ** we are an unusual case.
            */
            try {
                // This is a substatement; for now, we do not set any timeout
                // for it. We might change this behaviour later, by linking
                // timeout to its parent statement's timeout settings.
                ((GenericPreparedStatement)ps).setNeedsSavepoint(false);
                ResultSet rs = ps.executeSubStatement(activation, spsActivation, false, 0L);
                if (isWhen)
                {
                    // This is a WHEN clause. Expect a single BOOLEAN value
                    // to be returned.
                    ExecRow row = rs.getNextRow();
                    if (SanityManager.DEBUG && row.nColumns() != 1) {
                        SanityManager.THROWASSERT(
                            "Expected WHEN clause to have exactly "
                            + "one column, found: " + row.nColumns());
                    }

                    DataValueDescriptor value = row.getColumn(1);
                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(value instanceof SQLBoolean);
                    }

                    whenClauseWasTrue =
                            !value.isNull() && value.getBoolean();

                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(rs.getNextRow() == null,
                                "WHEN clause returned more than one row");
                    }
                }
                else if (rs.returnsRows()) {
                    // Fetch all the data to ensure that functions in the select list or values statement will
                    // be evaluated and side effects will happen. Why else would the trigger action return
                    // rows, but for side effects?
                    // The result set was opened in ps.execute()
                    while (rs.getNextRow() != null) {
                    }
                }
                rs.close();
            } catch (StandardException e) {
                /* 
                ** When a trigger SPS action is executed and results in 
                ** an exception, the system needs to clean up the active 
                ** statement context(SC) and the trigger execution context
                ** (TEC) in language connection context(LCC) properly (e.g.:  
                ** "Maximum depth triggers exceeded" exception); otherwise, 
                ** this will leave old TECs lingering and may result in
                ** subsequent statements within the same connection to throw 
                ** the same exception again prematurely.  
                **    
                ** A new statement context will be created for the SPS before
                ** it is executed.  However, it is possible for some 
                ** StandardException to be thrown before a new statement 
                ** context is pushed down to the context stack; hence, the 
                ** trigger executor needs to ensure that the current active SC 
                ** is associated with the SPS, so that it is cleaning up the 
                ** right statement context in LCC. 
                **
                ** It is also possible that the error has already been handled
                ** on a lower level, especially if the trigger re-enters the
                ** JDBC layer. In that case, the current SC will be null.
                **    
                ** When the active SC is cleaned up, the TEC will be removed
                ** from LCC and the SC object will be popped off from the LCC 
                ** as part of cleanupOnError logic.  
                 */
                
                /* retrieve the current active SC */
                StatementContext sc = lcc.getStatementContext();
                
                /* make sure that the cleanup is on the new SC */
                if (sc != null && active_sc != sc) {
                    sc.cleanupOnError(e);
                }
                
                /* Handle dynamic recompiles */
                if (e.getMessageId().equals(SQLState.LANG_STATEMENT_NEEDS_RECOMPILE)) {
                    recompile = true;
                    sps.revalidate(lcc);
                    continue;
                }

                spsActivation.close();
                throw e;
            }

            /* Done with execution without any recompiles */
            return whenClauseWasTrue;
        }
    }

    /**
     * Most of the time this will just retrieve the action's prepared statement and save a reference as a field
     * in this class.  When the statement is marked as invalid in the database (because of DDL that changed
     * its referenced table(s), etc) this method causes the statement to be recompiled.  Splice added this method
     * for use during initialization of TriggerEventActivator so that row trigger statements are always compiled
     * on the control side.  Statement triggers only execute control side so they are not relevant here.  Before
     * we did this region side triggers would find invalid statements, try to recompile them, and fail because
     * nested transactions could not be created with the SpliceTransactionView provided by SinkTask.  If SinkTask
     * goes away post Lassen we can consider removing this method. Or maybe someone can find a better way.
     */
    public void forceCompile() throws StandardException {

        if (getWhenClause() != null) {
            if (spsWhenActivation == null)
                spsWhenActivation = activation;
            compile(getWhenClause(), whenPS, -1);
        }
        for (int i = 0; i < actionList.size(); ++i) {
            if (getAction(i) != null) {
                if (spsActionActivationList.get(i) == null)
                    spsActionActivationList.set(i, activation);
                compile(getAction(i), actionPSList.get(i), i);
            }
        }
    }

    private void compile(SPSDescriptor sps,
                         ExecPreparedStatement ps,
                         int index) throws StandardException {
        boolean isWhen = index == -1;

        // The SPS activation will set its parent activation from
        // the statement context. Reset it to the original parent
        // activation first so that it doesn't use the activation of
        // the previously executed SPS as parent. DERBY-6348.
        lcc.getStatementContext().setActivation(activation);

        ps = sps.getPreparedStatement();
        /*
         * We need to clone the prepared statement so we don't
         * wind up marking that ps that is tied to sps as finished
         * during the course of execution.
         */
        ps = ps.getClone();

        // it should be valid since we've just prepared for it
        ps.setValid();
        Activation spsActivation = ps.getActivation(lcc, false);

        /*
         * Normally, we want getSource() for an sps invocation
         * to be EXEC STATEMENT xxx, but in this case, since
         * we are executing the SPS in our own fashion, we want
         * the text to be the trigger action.  So set it accordingly.
         */
        ps.setSource(sps.getText());
        ps.setSPSAction();

        // Cache the prepared statement and activation in case the
        // trigger fires multiple times.
        if (isWhen) {
            whenPS = ps;
            spsWhenActivation = spsActivation;
        } else {
            actionPSList.set(index, ps);
            spsActionActivationList.set(index, spsActivation);
        }
    }

    /**
     * Cleanup after executing an sps.
     */
    protected void clearSPS() throws StandardException {
        for (int i = 0; i < spsActionActivationList.size(); ++i) {
            if (spsActionActivationList.get(i) != null) {
                spsActionActivationList.get(i).close();
            }
            actionPSList.set(i, null);
            spsActionActivationList.set(i, null);
        }

        if (spsWhenActivation != null) {
            spsWhenActivation.close();
        }
        whenPS = null;
        spsWhenActivation = null;
    }

    /**
     * <p>
     * Execute the WHEN clause SPS and the trigger action SPS.
     * </p>
     *
     * <p>
     * If there is no WHEN clause, the trigger action should always be
     * executed. If there is a WHEN clause, the trigger action should only
     * be executed if the WHEN clause returns TRUE.
     * </p>
     *
     * @throws StandardException if trigger execution fails
     */
    final void executeWhenClauseAndAction() throws StandardException {
        SPSDescriptor whenClauseDescriptor = getWhenClause();
        if (whenClauseDescriptor == null ||
                executeSPS(whenClauseDescriptor, -1)) {
            for (int i = 0; i < actionList.size(); ++i) {
                executeSPS(getAction(i), i);
            }
        }
    }
} 
