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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.EngineDriver;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.sql.execute.TriggerEvent;
import com.splicemachine.db.impl.sql.execute.TriggerEventActivator;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import com.splicemachine.derby.iapi.sql.execute.SingleRowCursorResultSet;
import com.splicemachine.derby.impl.sql.execute.TriggerRowHolderImpl;
import com.splicemachine.derby.impl.sql.execute.actions.WriteCursorConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.DMLWriteInfo;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.tools.EmbedConnectionMaker;
import splice.com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
    private final boolean hasStatementTriggerWithReferencingClause;
    private FormatableBitSet heapList;
    private ExecRow templateRow;
    private String tableVersion;

    private DMLWriteInfo writeInfo;
    private Activation activation;
    private TriggerRowHolderImpl triggerRowHolder;
    private boolean isSpark;
    private TxnView txn;
    private byte[] token;

    private Function<Function<LanguageConnectionContext,Void>, Callable> withContext;

    public TriggerHandler(TriggerInfo triggerInfo,
                          DMLWriteInfo writeInfo,
                          Activation activation,
                          TriggerEvent beforeEvent,
                          TriggerEvent afterEvent,
                          FormatableBitSet heapList,
                          ExecRow templateRow,
                          String tableVersion) throws StandardException {
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
        this.hasStatementTriggerWithReferencingClause = triggerInfo.hasStatementTriggerWithReferencingClause();
        this.heapList = heapList;
        this.templateRow = templateRow;
        this.tableVersion = tableVersion;
        this.activation = activation;
        this.writeInfo = writeInfo;
        initTriggerActivator(activation, constantAction);
    }

    public Callable<Void> getTriggerTempTableflushCallback () {
        if (triggerRowHolder != null)
             return triggerRowHolder.getTriggerTempTableflushCallback();
        return null;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "DB-9844")
    public void setTxn(TxnView txn) {
        this.txn = txn;
        if (triggerRowHolder != null)
            triggerRowHolder.setTxn(txn);
    }

    public boolean hasStatementTrigger() {
        return hasBeforeStatement || hasAfterStatement;
    }

    public boolean hasStatementTriggerWithReferencingClause() {
        return this.hasStatementTriggerWithReferencingClause;
    }

    public void addRowToNewTableRowHolder(ExecRow row, KVPair encode) throws StandardException {
        if (triggerRowHolder != null)
            triggerRowHolder.insert(row, encode);
    }

    public void setIsSpark(boolean isSpark) {
        this.isSpark = isSpark;
    }

    public boolean isSpark() {
        return this.isSpark;
    }

    public long getTriggerConglomID() {
        if (triggerRowHolder != null) {
            return triggerRowHolder.getConglomerateId();
        }
        return 0;
    }

    @SuppressFBWarnings(value = {"EI_EXPOSE_REP2","URF_UNREAD_FIELD"}, justification = "DB-9844")
    public void initTriggerRowHolders(boolean isSpark, TxnView txn, byte[] token, long ConglomID) throws StandardException {
        this.isSpark = isSpark;
        this.txn = txn;
        this.token = token;
        Properties properties = new Properties();
        if (hasStatementTriggerWithReferencingClause) {
            // Use the smaller of ControlExecutionRowLimit or 1000000 to determine when to switch to spark execution.
            // Hard cap at 1 million despite the setting of controlExecutionRowLimit since we don't want to exhaust
            // memory if the sysadmin cranked this setting really high.
            int switchToSparkThreshold = EngineDriver.driver().getConfiguration().getControlExecutionRowLimit() <= Integer.MAX_VALUE ?
            (int) EngineDriver.driver().getConfiguration().getControlExecutionRowLimit() : Integer.MAX_VALUE;

            if (switchToSparkThreshold < 0)
                switchToSparkThreshold = 0;
            else if (switchToSparkThreshold > 1000000)
                switchToSparkThreshold = 1000000;


            long doubleDetermineSparkRowThreshold = 2 * getLcc().getOptimizerFactory().getDetermineSparkRowThreshold();
            int inMemoryLimit = switchToSparkThreshold;

            // Pick the larger of switchToSparkThreshold or 2*determineSparkRowThreshold as the
            // threshold for creating a conglomerate.  By design this will cause the trigger rows
            // to always be held in memory when executing on control, for performance.
            // But the interface also supports values of switchToSparkThreshold which are larger than
            // overflowToConglomThreshold, in which case we could create a temporary conglomerate while
            // executing in control.
            int overflowToConglomThreshold =
                 doubleDetermineSparkRowThreshold > inMemoryLimit ? (int)doubleDetermineSparkRowThreshold :
                 doubleDetermineSparkRowThreshold < 0 ? 0 :
                 inMemoryLimit;

            // Spark doesn't support use of an in-memory TriggerRowHolderImpl, so we
            // set overflowToConglomThreshold to zero and always use a conglomerate.
            if (isSpark) {
                switchToSparkThreshold = 0;
                overflowToConglomThreshold = 0;
            }

            triggerRowHolder =
                new TriggerRowHolderImpl(activation, properties, writeInfo.getResultDescription(),
                                         overflowToConglomThreshold, switchToSparkThreshold,
                                         templateRow, tableVersion, isSpark, txn, token, ConglomID,
                                          this.getTriggerExecutionContext());
        }

    }

    public TriggerExecutionContext getTriggerExecutionContext() { return this.triggerActivator.getTriggerExecutionContext(); }

    private void initTriggerActivator(Activation activation, WriteCursorConstantOperation constantAction) throws StandardException {
        try {
            withContext = new Function<Function<LanguageConnectionContext,Void>, Callable>() {
                @Override
                public Callable apply(Function<LanguageConnectionContext,Void> f) {
                    return new Callable() {
                        @Override
                        public Void call() throws Exception {
                            ActivationHolder ah = new ActivationHolder(activation, null);
                            try {
                                LanguageConnectionContext oldLCC, newLCC;
                                oldLCC = getLcc();
                                // Use a new LCC for this thread, but
                                // allow the transaction to remain elevatable.
                                // The alternative would be to call 'ah.reinitialize(null, false)',
                                // but that creates a SpliceTransactionView, which we
                                // cannot elevate.
                                ah.newTxnResource();
                                newLCC = ah.getLCC();
                                newLCC.pushStatementContext(true, oldLCC.isReadOnly(),
                                       oldLCC.getOrigStmtTxt(), null, false, 0L);
                                f.apply(ah.getLCC());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        }
                    };
                }
            };
            this.triggerActivator = new TriggerEventActivator(constantAction.getTargetUUID(),
                    constantAction.getTriggerInfo(),
                    activation,
                    null, SIDriver.driver().getExecutorService(), withContext, heapList, !SpliceClient.isRegionServer);
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
            triggerActivator.cleanup(false);
        }
        if (triggerRowHolder != null)
            triggerRowHolder.close();;
    }

    public void fireBeforeRowTriggers(ExecRow row) throws StandardException {
        if (row != null && hasBeforeRow) {
            SingleRowCursorResultSet triggeringResultSet = new SingleRowCursorResultSet(resultDescription, row);
            List<Future<Void>> futures = triggerActivator.notifyRowEventConcurrent(beforeEvent, triggeringResultSet, null);
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
            triggerActivator.notifyRowEvent(beforeEvent, triggeringResultSet, null, hasStatementTriggerWithReferencingClause);
        }
    }

    public void fireAfterRowTriggers(ExecRow row, Callable<Void> flushCallback) throws Exception {
        if (!hasAfterRow)
            return;
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
            if (getTriggerTempTableflushCallback() != null)
                getTriggerTempTableflushCallback().call();
        } catch (Exception e) {
            pendingAfterRows.clear();
            throw e;
        }

        if (!hasAfterRow)
            return;

        List<Future<Void>> futures = new ArrayList<>();

        // The LCC can't be shared amongst threads, so
        // only use one level of concurrency for now.
        if (true || pendingAfterRows.size() <= 1) {
            for (ExecRow flushedRow : pendingAfterRows)
                futures.addAll(fireAfterRowConcurrentTriggers(flushedRow));
            for (ExecRow flushedRow : pendingAfterRows)
                fireAfterRowTriggers(flushedRow);
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
            for (ExecRow flushedRow : pendingAfterRows)
                fireAfterRowTriggers(flushedRow);

            for (Future<Void> f : rowFutures) {
                f.get(); // bubble up any exceptions
            }
        }
        for (Future<Void> f : futures) {
            f.get(); // bubble up any exceptions
        }
        pendingAfterRows.clear();
    }

    private void fireAfterRowTriggers(ExecRow row) throws StandardException {
        if (row != null && hasAfterRow) {
            SingleRowCursorResultSet triggeringResultSet = new SingleRowCursorResultSet(resultDescription, row);
            triggerActivator.notifyRowEvent(afterEvent, triggeringResultSet, null, hasStatementTriggerWithReferencingClause);
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
            CursorResultSet triggeringResultSet = triggerRowHolder == null ? null : triggerRowHolder.getResultSet();
            triggerActivator.notifyStatementEvent(beforeEvent, triggeringResultSet, hasStatementTriggerWithReferencingClause);
        }
    }

    public void fireAfterStatementTriggers() throws StandardException {
        if (hasAfterStatement) {
            CursorResultSet triggeringResultSet = triggerRowHolder == null ? null : triggerRowHolder.getResultSet();
            triggerActivator.notifyStatementEvent(afterEvent, triggeringResultSet, hasStatementTriggerWithReferencingClause);
        }
    }

    public LanguageConnectionContext getLcc() throws StandardException {
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
