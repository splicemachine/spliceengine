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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.sql.execute.TriggerEvent;
import com.splicemachine.db.impl.sql.execute.TriggerEventActivator;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import com.splicemachine.derby.iapi.sql.execute.SingleRowCursorResultSet;
import com.splicemachine.derby.impl.sql.execute.actions.WriteCursorConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.DMLWriteInfo;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.tools.EmbedConnectionMaker;
import org.spark_project.guava.collect.Lists;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Used by DMLOperation to initialize the derby classes necessary for firing row/statement triggers.  Also provides
 * convenient methods for firing.
 * <p/>
 * Instances are intended to be used by a single thread.
 */
public class TriggerHandler {

    /**
     * When this many rows have been passed for firing AFTER row triggers then we actually fire.
     */
    private static final int AFTER_ROW_BUFFER_SIZE = 1000;

    private TriggerEventActivator triggerActivator;
    private ResultDescription resultDescription;
    private TriggerEvent beforeEvent;
    private TriggerEvent afterEvent;
    private List<ExecRow> pendingAfterRows;

    private final boolean hasBeforeRow;
    private final boolean hasBeforeStatement;
    private final boolean hasAfterRow;
    private final boolean hasAfterStatement;

    private Activation activation;
    private Function<Function<LanguageConnectionContext,Void>, Callable> withContext;

    public TriggerHandler(TriggerInfo triggerInfo,
                          DMLWriteInfo writeInfo,
                          Activation activation,
                          TriggerEvent beforeEvent,
                          TriggerEvent afterEvent) throws StandardException {
        WriteCursorConstantOperation constantAction = (WriteCursorConstantOperation) writeInfo.getConstantAction();
        initConnectionContext(activation.getLanguageConnectionContext());

        this.beforeEvent = beforeEvent;
        this.afterEvent = afterEvent;
        this.activation = activation;
        this.resultDescription = activation.getResultDescription();
        this.pendingAfterRows = Lists.newArrayListWithCapacity(AFTER_ROW_BUFFER_SIZE);

        this.hasBeforeRow = triggerInfo.hasBeforeRowTrigger();
        this.hasAfterRow = triggerInfo.hasAfterRowTrigger();
        this.hasBeforeStatement = triggerInfo.hasBeforeStatementTrigger();
        this.hasAfterStatement = triggerInfo.hasAfterStatementTrigger();

        initTriggerActivator(activation, constantAction);
    }

    private void initTriggerActivator(Activation activation, WriteCursorConstantOperation constantAction) throws StandardException {
        try {
            withContext = new Function<Function<LanguageConnectionContext,Void>, Callable>() {
                @Override
                public Callable apply(Function<LanguageConnectionContext,Void> f) {
                    return new Callable() {
                        @Override
                        public Void call() throws Exception {
                            boolean prepared = false;
                            ActivationHolder ah = new ActivationHolder(activation, null);
                            try {
                                ah.reinitialize(null, false);

                                f.apply(ah.getActivation().getLanguageConnectionContext());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                if (prepared)
                                    ah.close();
                            }
                            return null;
                        }
                    };
                }
            };
            this.triggerActivator = new TriggerEventActivator(constantAction.getTargetUUID(),
                    constantAction.getTriggerInfo(),
                    activation,
                    null, SIDriver.driver().getExecutorService(), withContext);
        } catch (StandardException e) {
            popAllTriggerExecutionContexts(activation.getLanguageConnectionContext());
            throw e;
        }
    }

    /* If we throw a StandardException during initialization of the TEC then we are done not just with this
     * trigger, but with the entire recursive trigger hierarchy, so remove all TEC from the context stack and then
     * re-throw.  I believe the only StandardException we expect here is max-trigger-recursion-depth exception. */
    private void popAllTriggerExecutionContexts(LanguageConnectionContext lcc) throws StandardException {
        if (lcc.hasTriggers()) {
            lcc.popAllTriggerExecutionContexts();
        }
    }

    /* We have a trigger and we are on a region-side node where the LCC has no connection context.  Add one.
     * Is there a better way to do this? */
    private void initConnectionContext(LanguageConnectionContext lcc) throws StandardException {
        ConnectionContext existingContext = (ConnectionContext) lcc.getContextManager().getContext(ConnectionContext.CONTEXT_ID);
        if (existingContext == null) {
            try {
                Connection connection = new EmbedConnectionMaker().createNew(new Properties());
                Context newContext = ((EmbedConnection) connection).getContextManager().getContext(ConnectionContext.CONTEXT_ID);
                lcc.getContextManager().pushContext(newContext);
            } catch (SQLException e) {
                throw StandardException.plainWrapException(e);
            }
        }
    }

    public void cleanup() throws StandardException {
        if (triggerActivator != null) {
            triggerActivator.cleanup();
        }
    }

    public void fireBeforeRowTriggers(ExecRow row) throws StandardException {
        if (row != null && hasBeforeRow) {
            SingleRowCursorResultSet triggeringResultSet = new SingleRowCursorResultSet(resultDescription, row);
            List<Future<Void>> futures = triggerActivator.notifyRowEventConcurrent(beforeEvent, triggeringResultSet, null);
            triggerActivator.notifyRowEvent(beforeEvent, triggeringResultSet, null, null);
            for (Future<Void> f : futures) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof StandardException) {
                        throw (StandardException) e.getCause();
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void fireAfterRowTriggers(ExecRow row, Callable<Void> flushCallback) throws Exception {
        pendingAfterRows.add(row.getClone());
        if (pendingAfterRows.size() == AFTER_ROW_BUFFER_SIZE) {
            firePendingAfterTriggers(flushCallback);
        }
    }

    public void firePendingAfterTriggers(Callable<Void> flushCallback) throws Exception {
        /* If there are any un-flushed rows that would cause a constraint violation then this callback will throw.
         * Which is what we want. Check constraints before firing after triggers. */
        try {
            flushCallback.call();
        } catch (Exception e) {
            pendingAfterRows.clear();
            throw e;
        }
        List<Future<Void>> futures = new ArrayList<>();

        if (pendingAfterRows.size() <= 1) {
            for (ExecRow flushedRow : pendingAfterRows) {
                futures.addAll(fireAfterRowConcurrentTriggers(flushedRow));
                fireAfterRowTriggers(flushedRow, activation.getLanguageConnectionContext());
            }
        } else {
            Object lock = new Object();
            // work concurrently
            List<Future<Void>> rowFutures = new ArrayList<>();
            for (ExecRow flushedRow : pendingAfterRows) {
                rowFutures.add(SIDriver.driver().getExecutorService().submit(withContext.apply(new Function<LanguageConnectionContext,Void>() {
                    @Override
                    public Void apply(LanguageConnectionContext lcc) {
                        try {
                            List<Future<Void>> f = fireAfterRowConcurrentTriggers(flushedRow);
                            synchronized (lock) {
                                futures.addAll(f);
                            }
                        } catch (StandardException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    }
                })));
            }

            for (ExecRow flushedRow : pendingAfterRows) {
                fireAfterRowTriggers(flushedRow, activation.getLanguageConnectionContext());
            }
            for (Future<Void> f : rowFutures) {
                f.get(); // bubble up any exceptions
            }
        }
        for (Future<Void> f : futures) {
            f.get(); // bubble up any exceptions
        }
        pendingAfterRows.clear();
    }

    private void fireAfterRowTriggers(ExecRow row, LanguageConnectionContext lcc) throws StandardException {
        if (row != null && hasAfterRow) {
            SingleRowCursorResultSet triggeringResultSet = new SingleRowCursorResultSet(resultDescription, row);
            triggerActivator.notifyRowEvent(afterEvent, triggeringResultSet, null, lcc);
        }
    }

    private List<Future<Void>> fireAfterRowConcurrentTriggers(ExecRow row) throws StandardException {
        if (row != null && hasAfterRow) {
            SingleRowCursorResultSet triggeringResultSet = new SingleRowCursorResultSet(resultDescription, row);
            return triggerActivator.notifyRowEventConcurrent(afterEvent, triggeringResultSet, null);
        }
        return Collections.emptyList();
    }


    public void fireBeforeStatementTriggers() throws StandardException {
        if (hasBeforeStatement) {
            triggerActivator.notifyStatementEvent(beforeEvent);
        }
    }

    public void fireAfterStatementTriggers() throws StandardException {
        if (hasAfterStatement) {
            triggerActivator.notifyStatementEvent(afterEvent);
        }
    }

    public LanguageConnectionContext getLcc() {
        return triggerActivator.getLcc();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // Static convenience methods for invoking methods in this class on an instance that may be null.
    //
    // We now fire triggers from multiple locations and most of the time triggerHandler will be null
    // (most of the time there are no triggers involved in DML operations).  These methods are here
    // just so that we can take the null checks out of the DMLOperation code and make that code a bit
    // more concise (one line to fire, etc), no null check.
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    public static void fireBeforeRowTriggers(TriggerHandler triggerHandler, ExecRow row) throws StandardException {
        if (triggerHandler != null) {
            triggerHandler.fireBeforeRowTriggers(row);
        }
    }

    public static void fireAfterRowTriggers(TriggerHandler triggerHandler, ExecRow row, Callable<Void> flushCallback) throws Exception {
        if (triggerHandler != null) {
            triggerHandler.fireAfterRowTriggers(row, flushCallback);
        }
    }

    public static void firePendingAfterTriggers(TriggerHandler triggerHandler, Callable<Void> flushCallback) throws Exception {
        if (triggerHandler != null) {
            triggerHandler.firePendingAfterTriggers(flushCallback);
        }
    }

    public static Callable<Void> flushCallback(final CallBuffer callBuffer) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                callBuffer.flushBufferAndWait();
                return null;
            }
        };
    }

}
