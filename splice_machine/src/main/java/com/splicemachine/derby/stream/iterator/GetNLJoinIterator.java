/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.derby.stream.function.NLJoinFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.Pair;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Created by jyuan on 10/10/16.
 */
public abstract class GetNLJoinIterator implements Callable<Pair<OperationContext, Iterator<ExecRow>>> {

    protected ExecRow locatedRow;
    protected OperationContext operationContext;
    protected boolean initialized;
    ContextManager cm;
    boolean newContextManager, lccPushed;

    public GetNLJoinIterator() {}

    public GetNLJoinIterator(OperationContext operationContext, ExecRow locatedRow) {
        this.operationContext = operationContext;
        this.locatedRow = locatedRow;
    }

    public abstract Pair<OperationContext, Iterator<ExecRow>> call() throws Exception;

    private boolean needsLCCInContext() {
        if (operationContext != null) {
            Activation activation = operationContext.getActivation();
            if (activation != null) {
                LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
                if (lcc != null) {
                    TriggerExecutionContext tec = lcc.getTriggerExecutionContext();
                    return tec.currentTriggerHasReferencingClause();
                }
            }
        }
        return false;
    }

    // Push the LanguageConnectionContext to the current Context Manager
    // if we're executing a trigger with a referencing clause.
    protected void init() {
        initialized = true;
        if (!needsLCCInContext())
            return;
        cm = ContextService.getFactory().getCurrentContextManager();
        if (cm == null) {
            newContextManager = true;
            cm = ContextService.getFactory().newContextManager();
            ContextService.getFactory().setCurrentContextManager(cm);
        }
        if (cm != null) {
            try {
                ConnectionUtil.getCurrentLCC();
            }
            catch (SQLException e) {
                // If the current LCC is not available in the context,
                // push it now.
                if (operationContext != null) {
                    Activation activation = operationContext.getActivation();
                    if (activation != null && activation.getLanguageConnectionContext() != null) {
                        lccPushed = true;
                        cm.pushContext(activation.getLanguageConnectionContext());
                    }
                }
            }
        }
    }

    protected void cleanup() {
        if (cm != null) {
            if (lccPushed)
                cm.popContext();
            if (newContextManager)
                ContextService.getFactory().resetCurrentContextManager(cm);
            cm = null;
        }
        newContextManager = false;
        lccPushed = false;
        initialized = false;
    }

    public static GetNLJoinIterator makeGetNLJoinIterator(NLJoinFunction.JoinType joinType,
                                                   OperationContext operationContext,
                                                          ExecRow locatedRow) {
        switch (joinType) {
            case INNER:
                return new GetNLJoinInnerIterator(operationContext, locatedRow);

            case LEFT_OUTER:
                return new GetNLJoinLeftOuterIterator(operationContext, locatedRow);

            case ANTI:
                return new GetNLJoinAntiIterator(operationContext, locatedRow);

            case ONE_ROW_INNER:
                return new GetNLJoinOneRowIterator(operationContext, locatedRow);

            default:
                throw new RuntimeException("Unrecognized nested loop join type");
        }
    }
}
