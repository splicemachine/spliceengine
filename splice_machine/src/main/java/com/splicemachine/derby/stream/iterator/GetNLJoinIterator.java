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

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.derby.stream.function.NLJoinFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.Pair;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.splicemachine.db.impl.sql.execute.TriggerExecutionContext.pushTriggerExecutionContextFromActivation;

/**
 * Created by jyuan on 10/10/16.
 */
public abstract class GetNLJoinIterator implements Callable<Pair<OperationContext, Iterator<ExecRow>>> {

    protected ExecRow locatedRow;
    protected Supplier<OperationContext> operationContext;
    protected boolean initialized;
    protected ContextManager cm;
    protected boolean newContextManager, lccPushed;
    private   OperationContext ctx;

    public GetNLJoinIterator() {}

    public GetNLJoinIterator(Supplier<OperationContext> operationContext, ExecRow locatedRow) {
        this.operationContext = operationContext;
        this.locatedRow = locatedRow;
    }

    public abstract Pair<OperationContext, Iterator<ExecRow>> call() throws Exception;

    protected OperationContext getCtx() {
        if (ctx == null)
            ctx = operationContext.get();
        return ctx;
    }

    private boolean needsLCCInContext() {
        OperationContext ctx = getCtx();
        if (ctx != null) {
            Activation activation = ctx.getActivation();
            if (activation != null) {
                LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
                if (lcc != null) {
                    TriggerExecutionContext tec = lcc.getTriggerExecutionContext();
                    if (tec != null)
                        return tec.currentTriggerHasReferencingClause();
                }
            }
        }
        return false;
    }

    // Push the LanguageConnectionContext to the current Context Manager
    // if we're executing a trigger with a referencing clause.
    protected void init() throws Exception {
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
            OperationContext ctx = getCtx();
            if (ctx != null) {
                lccPushed = pushTriggerExecutionContextFromActivation(ctx.getActivation(), cm);
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
                                                   Supplier<OperationContext> operationContextSupplier,
                                                          ExecRow locatedRow) {
        switch (joinType) {
            case INNER:
                return new GetNLJoinInnerIterator(operationContextSupplier, locatedRow);

            case LEFT_OUTER:
                return new GetNLJoinLeftOuterIterator(operationContextSupplier, locatedRow);

            case ANTI:
                return new GetNLJoinAntiIterator(operationContextSupplier, locatedRow);

            case ONE_ROW_INNER:
                return new GetNLJoinOneRowIterator(operationContextSupplier, locatedRow);

            default:
                throw new RuntimeException("Unrecognized nested loop join type");
        }
    }
}
