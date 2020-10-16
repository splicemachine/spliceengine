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

package com.splicemachine.pipeline.foreignkey;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.foreignkey.actions.Action;
import com.splicemachine.pipeline.foreignkey.actions.ActionFactory;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Intercepts deletes from a parent table (primary key or unique index) and sends the rowKey over to the referencing
 * indexes to check for its existence and updating child table(s) if the FK has ON DELETE SET NULL.
 */
@NotThreadSafe
public class ForeignKeyParentInterceptWriteHandler implements WriteHandler{

    private boolean failed;

    private final List<Long> referencingIndexConglomerateIds;
    private boolean shouldRefreshActions;
    private final List<DDLMessage.FKConstraintInfo> constraintInfos;
    private final ForeignKeyViolationProcessor violationProcessor;
    private TxnOperationFactory txnOperationFactory;
    private String parentTableName;

    private Map<Pair<Long, Long>, Action> actions;

    public ForeignKeyParentInterceptWriteHandler(String parentTableName,
                                                 List<Long> referencingIndexConglomerateIds,
                                                 List<DDLMessage.FKConstraintInfo> constraintInfos,
                                                 PipelineExceptionFactory exceptionFactory
                                                 ) {
        this.shouldRefreshActions = true;
        this.referencingIndexConglomerateIds = referencingIndexConglomerateIds;
        this.violationProcessor = new ForeignKeyViolationProcessor(exceptionFactory);
        this.constraintInfos = constraintInfos;
        this.txnOperationFactory = SIDriver.driver().getOperationFactory();
        this.parentTableName = parentTableName;
        this.actions = new HashMap<>(referencingIndexConglomerateIds.size());
    }

    /** We exist to prevent updates/deletes of rows from the parent table which are referenced by a child.
     * Since we are a WriteHandler on a primary-key or unique-index we can just handle deletes.
     */
    private boolean isForeignKeyInterceptNecessary(KVPair.Type type) {
        /* We exist to prevent updates/deletes of rows from the parent table which are referenced by a child.
         * Since we are a WriteHandler on a primary-key or unique-index we can just handle deletes. */
        return type == KVPair.Type.DELETE;
    }

    private void ensureBuffers(WriteContext context) throws Exception {
        if (shouldRefreshActions) {
            assert referencingIndexConglomerateIds.size() == constraintInfos.size();
            for (int i = 0; i < referencingIndexConglomerateIds.size(); i++) {
                Pair<Long, Long> needle = new Pair<>(constraintInfos.get(i).getChildTable().getConglomerate(), referencingIndexConglomerateIds.get(i));
                if(!actions.containsKey(needle)) {
                    actions.put(needle, ActionFactory.createAction(referencingIndexConglomerateIds.get(i), constraintInfos.get(i), context,
                            parentTableName, txnOperationFactory, violationProcessor));
                }
            }
            shouldRefreshActions = false;
        }
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (isForeignKeyInterceptNecessary(mutation.getType())) {
            if(failed) {
                ctx.notRun(mutation);
                return;
            }
            try {
                ensureBuffers(ctx);
            } catch (Exception e) {
                ctx.failed(mutation, WriteResult.failed(e.getMessage()));
                return;
            }
            for(Action action : actions.values()) {
                action.next(mutation, ctx);
                if(action.hasFailed()) {
                    failed = true;
                    ctx.failed(mutation, action.getFailedWriteResult());
                    break;
                }
            }
        }
        if(!failed) {
            ctx.sendUpstream(mutation);
        }
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        try {
            for (Action action : actions.values()) {
                action.flush(ctx);
            }
        } catch (IOException e) {
            // it is a programming error to enter this code path.
            throw new IOException("unexpected, inner actions should handle exceptions ", e);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        for(Action action : actions.values()) {
            action.close(ctx);
        }
    }
}
