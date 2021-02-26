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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.function.NLJoinFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.Pair;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Created by jyuan on 10/10/16.
 */
public abstract class GetNLJoinIterator implements AutoCloseable, Callable<Pair<OperationContext, Iterator<ExecRow>>> {

    protected ExecRow locatedRow;
    protected Supplier<OperationContext> operationContext;
    protected boolean initialized;
    protected ContextManager cm;
    private   OperationContext ctx;
    private   ActivationHolder ah;

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

    private void marshallTransactionForTrigger() throws Exception{
        OperationContext ctx = getCtx();
        if (ctx != null) {
            Activation activation = ctx.getActivation();
            if (activation != null) {
                LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
                if (lcc != null) {
                    TriggerExecutionContext tec = lcc.getTriggerExecutionContext();
                    if (tec != null && tec.currentTriggerHasReferencingClause()) {
                            ah = new ActivationHolder(activation, null);
                            LanguageConnectionContext newLCC = null;
                            try {
                                LanguageConnectionContext oldLCC;
                                oldLCC = lcc;
                                ah.newTxnResource();
                                newLCC = ah.getLCC();
                                newLCC.pushStatementContext(true, oldLCC.isReadOnly(),
                                       oldLCC.getOrigStmtTxt(), null, false, 0L);
                                newLCC.pushTriggerExecutionContext(tec);
                            } catch (Exception e) {
                                close();
                                throw new RuntimeException(e);
                            }
                    }
                }
            }
        }
    }

    protected void init() throws Exception {
        if (!initialized)
            marshallTransactionForTrigger();

        initialized = true;
    }

    @Override
    public void close() throws Exception {
        if (ah != null)
            ah.close();
        ah = null;
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
