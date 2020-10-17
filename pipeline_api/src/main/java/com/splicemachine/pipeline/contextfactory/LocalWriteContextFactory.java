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

package com.splicemachine.pipeline.contextfactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.splicemachine.pipeline.constraint.NoOpConstraintChecker;
import com.splicemachine.si.impl.driver.SIDriver;
import splice.com.google.common.base.Function;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Lists;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.access.configuration.PipelineConfiguration;
import com.splicemachine.access.util.CachedPartitionFactory;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.constraint.BatchConstraintChecker;
import com.splicemachine.pipeline.constraint.ChainConstraintChecker;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.writehandler.PartitionWriteHandler;
import com.splicemachine.pipeline.writehandler.SharedCallBufferFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;


/**
 * One instance of this class will exist per conglomerate number per JVM.  Holds state about whether its conglomerate
 * has indexes and constraints, has columns that have been dropped, etc-- necessary information for performing writes
 * on the represented table/index.
 *
 * @author Scott Fines
 *         Created on: 4/30/13
 */
class LocalWriteContextFactory<TableInfo> implements WriteContextFactory<TransactionalRegion> {
    private static final Logger LOG = Logger.getLogger(LocalWriteContextFactory.class);

    private final long startupLockBackoffPeriod;
    private final PipelineExceptionFactory pipelineExceptionFactory;
    /* Known as "conglomerate number" in the sys tables, this is the number of the physical table (hbase table) that
     * users of this context write to. It may represent a base table or any one of several index tables for a base
     * table. */
    private final long conglomId;
    private Function<TableInfo, String> tableInfoParseFunction;

    /* These factories create WriteHandlers that intercept writes to the htable for this context (a base table)
     * transform them, and send them to a remote index table.. */
    private volatile WriteFactoryGroup indexFactories;

    /* These create WriteHandlers that enforce constrains on the htable users of this context write to. */
    private volatile Set<ConstraintFactory> constraintFactories;

    private volatile WriteFactoryGroup ddlFactories;

    /* Holds all of the WriteFactor instances related to foreign keys */
    private WriteFactoryGroup fkGroup;

    private final ReentrantLock initializationLock = new ReentrantLock();

    /* Latch for blocking writes during the updating of table metadata (adding constraints, indices, etc). */
    private final ResettableCountDownLatch tableWriteLatch = new ResettableCountDownLatch(1);

    private final PartitionFactory<TableInfo> basePartitionFactory;

    protected final AtomicReference<State> state = new AtomicReference<>(State.WAITING_TO_START);
    private final ContextFactoryLoader factoryLoader;

    public LocalWriteContextFactory(long conglomId,
                                    long startupLockBackoffPeriod,
                                    PartitionFactory<TableInfo> basePartitionFactory,
                                    PipelineExceptionFactory pipelineExceptionFactory,
                                    Function<TableInfo, String> tableInfoParseFunction,
                                    ContextFactoryLoader factoryLoader) {
        this.conglomId = conglomId;
        this.tableInfoParseFunction=tableInfoParseFunction;
        this.factoryLoader=factoryLoader;
        tableWriteLatch.countDown();
        this.startupLockBackoffPeriod=startupLockBackoffPeriod;
        this.pipelineExceptionFactory = pipelineExceptionFactory;
        this.basePartitionFactory = basePartitionFactory;
    }

    @Override
    public void prepare() {
        state.compareAndSet(State.WAITING_TO_START, State.READY_TO_START);
    }

    @Override
    public WriteContext create(SharedCallBufferFactory indexSharedCallBuffer,
                               TxnView txn, TransactionalRegion rce,
                               ServerControl env) throws IOException, InterruptedException {
        CachedPartitionFactory<TableInfo> pf = new CachedPartitionFactory<TableInfo>(basePartitionFactory){
            @Override protected String infoAsString(TableInfo tableName){ return tableInfoParseFunction.apply(tableName); }
        };
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer,pf, txn, null, rce, false, false, false, false, env,pipelineExceptionFactory);
        BatchConstraintChecker checker = buildConstraintChecker(txn, rce);
        context.addLast(new PartitionWriteHandler(rce, tableWriteLatch, checker));
        addWriteHandlerFactories(1000, context);
        return context;
    }

    @Override
    public WriteContext create(SharedCallBufferFactory indexSharedCallBuffer,
                               TxnView txn, byte[] token, TransactionalRegion region, int expectedWrites,
                               boolean skipIndexWrites, boolean skipConflictDetection, boolean skipWAL, boolean rollforward,
                               ServerControl env) throws IOException, InterruptedException {
        CachedPartitionFactory<TableInfo> pf = new CachedPartitionFactory<TableInfo>(basePartitionFactory){
            @Override protected String infoAsString(TableInfo tableName){ return tableInfoParseFunction.apply(tableName); }
        };
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer,
                pf,txn, token, region, skipIndexWrites,skipConflictDetection, skipWAL, rollforward, env,pipelineExceptionFactory);
        BatchConstraintChecker checker = buildConstraintChecker(txn, region);
        context.addLast(new PartitionWriteHandler(region, tableWriteLatch, checker));
        addWriteHandlerFactories(expectedWrites, context);
        return context;
    }

    private BatchConstraintChecker buildConstraintChecker(TxnView txn, TransactionalRegion region) throws IOException, InterruptedException{
        isInitialized(txn);
        switch (state.get()) {
            case NOT_MANAGED:
                assert region.getTableName().matches("[0-9]+");
                return new NoOpConstraintChecker(SIDriver.driver().getOperationStatusLib());
            case RUNNING:
                if(constraintFactories.isEmpty()){
                    return null;
                }
                List<BatchConstraintChecker> checkers= Lists.newArrayListWithCapacity(constraintFactories.size());
                for(ConstraintFactory factory : constraintFactories){
                    checkers.add(factory.getConstraintChecker());
                }
                return new ChainConstraintChecker(checkers);
        }
        return null;
    }


    @Override
    public WriteContext createPassThrough(SharedCallBufferFactory indexSharedCallBuffer,
                                          TxnView txn,
                                          TransactionalRegion region,
                                          int expectedWrites,
                                          ServerControl env) throws IOException, InterruptedException {
        CachedPartitionFactory<TableInfo> pf = new CachedPartitionFactory<TableInfo>(basePartitionFactory){
            @Override protected String infoAsString(TableInfo tableName){ return tableInfoParseFunction.apply(tableName); }
        };
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer, pf,txn, null, region, false, false, false, false, env,pipelineExceptionFactory);
        addWriteHandlerFactories(expectedWrites, context);
        return context;
    }

    @Override
    public void addDDLChange(DDLChange ddlChange) {
        factoryLoader.ddlChange(ddlChange); //TODO -sf- remove this?
    }

    @Override
    public void close() {
        //no-op
    }

    @Override
    public boolean hasDependentWrite(TxnView txn) throws IOException, InterruptedException{
        isInitialized(txn);
        return indexFactories != null &&!indexFactories.isEmpty();
    }

    public static <TableInfo> LocalWriteContextFactory<TableInfo> unmanagedContextFactory(PartitionFactory<TableInfo> partitionFactory,
                                                                   PipelineExceptionFactory pef,
                                                                   Function<TableInfo,String>tableInfoParser) {
        return new LocalWriteContextFactory<TableInfo>(-1, PipelineConfiguration.DEFAULT_STARTUP_LOCK_PERIOD, partitionFactory, pef, tableInfoParser, UnmanagedFactoryLoader.INSTANCE){
            @Override
            public void prepare() {
                state.set(State.NOT_MANAGED);
            }
        };
    }

    private void isInitialized(TxnView txn) throws IOException, InterruptedException {
        switch (state.get()) {
            case READY_TO_START:
                SpliceLogUtils.trace(LOG, "Index management for conglomerate %d has not completed, attempting to start now", conglomId);
                start(txn);
                break;
            case STARTING:
                SpliceLogUtils.trace(LOG, "Index management is starting up");
                start(txn);
                break;
            case FAILED_SETUP:
                //since we haven't done any writes yet, it's safe to just explore
                throw pipelineExceptionFactory.doNotRetry("Failed write setup for conglomerate " + conglomId);
            case SHUTDOWN:
                throw new IOException("management for conglomerate " + conglomId + " is shutdown");
        }
    }

    private void addWriteHandlerFactories(int expectedWrites, PipelineWriteContext context) throws IOException, InterruptedException {
        isInitialized(context.getTxn());
        //only add constraints and indices when we are in a RUNNING state
        if (state.get() == State.RUNNING) {
            //add Constraint checks before anything else
            for (ConstraintFactory constraintFactory : constraintFactories) {
                context.addLast(constraintFactory.create(expectedWrites));
            }

            //add index handlers
            indexFactories.addFactories(context,true,expectedWrites);

            ddlFactories.addFactories(context,true,expectedWrites);

            // FK - child intercept (of inserts/updates)
            fkGroup.addFactories(context,false,expectedWrites);
        }
    }

    private void start(TxnView txn) throws IOException, InterruptedException {
        /*
         * Ready to Start => switch to STARTING
         * STARTING => continue through to block on the lock
         * Any other state => return, because we don't need to perform this method.
         */
        if (!state.compareAndSet(State.READY_TO_START, State.STARTING)) {
            if (state.get() != State.STARTING) return;
        }

        //someone else may have initialized this if so, we don't need to repeat, so return
        if (state.get() != State.STARTING) return;

        SpliceLogUtils.debug(LOG, "Setting up index for conglomerate %d", conglomId);

        if (!initializationLock.tryLock(startupLockBackoffPeriod, TimeUnit.MILLISECONDS)) {
            throw new IndexNotSetUpException("Unable to initialize index management for table " + conglomId
                    + " within a sufficient time frame. Please wait a bit and try again");
        }
        if(state.get()!=State.STARTING) return; //we got here in a weird way
        try{
            factoryLoader.load(txn);

            indexFactories = factoryLoader.getIndexFactories();
            ddlFactories = factoryLoader.getDDLFactories();
            fkGroup = factoryLoader.getForeignKeyFactories();
            constraintFactories = factoryLoader.getConstraintFactories();
            state.set(State.RUNNING);
        }catch(IndexNotSetUpException inse){
            state.set(State.READY_TO_START);
            throw inse;
        }catch(Exception e){
            state.set(State.FAILED_SETUP);
            throw e;
        }finally{
            initializationLock.unlock();
        }
    }


}
