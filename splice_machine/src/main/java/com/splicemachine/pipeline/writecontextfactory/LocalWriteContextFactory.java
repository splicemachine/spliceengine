package com.splicemachine.pipeline.writecontextfactory;

import com.google.common.collect.Lists;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.derby.hbase.StatisticsWatcher;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.constraint.*;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.ddl.TentativeDDLDesc;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.IndexCallBufferFactory;
import com.splicemachine.pipeline.writehandler.RegionWriteHandler;
import com.splicemachine.pipeline.writehandler.StatisticsWriteHandler;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.dictionary.*;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One instance of this class will exist per conglomerateId per JVM.  Holds state about whether its conglomerate
 * has indexes and constraints, has columns that have been dropped, etc-- necessary information for performing writes
 * on the represented table/index.
 *
 * @author Scott Fines
 *         Created on: 4/30/13
 */
class LocalWriteContextFactory implements WriteContextFactory<TransactionalRegion> {
    private static final Logger LOG = Logger.getLogger(LocalWriteContextFactory.class);

    private static final long STARTUP_LOCK_BACKOFF_PERIOD = SpliceConstants.startupLockWaitPeriod;

    private final long conglomId;
    private final List<LocalWriteFactory> indexFactories = new CopyOnWriteArrayList<>();
    private final Set<ConstraintFactory> constraintFactories = new CopyOnWriteArraySet<>();
    private final Set<AlterTableWriteFactory> alterTableWriteFactories = new CopyOnWriteArraySet<>();

    private ForeignKeyChildInterceptWriteFactory foreignKeyChildInterceptWriteFactory;
    private ForeignKeyChildCheckWriteFactory foreignKeyChildCheckWriteFactory;
    private ForeignKeyParentInterceptWriteFactory foreignKeyParentInterceptWriteFactory;
    private ForeignKeyParentCheckWriteFactory foreignKeyParentCheckWriteFactory;

    private final ReentrantLock initializationLock = new ReentrantLock();

    /* Latch for blocking writes during the updating of table metadata (adding constraints, indices, etc). */
    private final ResettableCountDownLatch tableWriteLatch = new ResettableCountDownLatch(1);

    protected final AtomicReference<State> state = new AtomicReference<>(State.WAITING_TO_START);

    public LocalWriteContextFactory(long conglomId) {
        this.conglomId = conglomId;
        tableWriteLatch.countDown();
    }

    @Override
    public void prepare() {
        state.compareAndSet(State.WAITING_TO_START, State.READY_TO_START);
    }

    @Override
    public WriteContext create(IndexCallBufferFactory indexSharedCallBuffer,
                               TxnView txn, TransactionalRegion rce,
                               RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer, txn, rce, env);
        BatchConstraintChecker checker = buildConstraintChecker();
        context.addLast(new RegionWriteHandler(rce, tableWriteLatch, checker));
        addIndexAndForeignKeyWriteHandlers(1000, context);
        return context;
    }

    @Override
    public WriteContext create(IndexCallBufferFactory indexSharedCallBuffer,
                               TxnView txn, TransactionalRegion region, int expectedWrites,
                               RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer, txn, region, env);
        BatchConstraintChecker checker = buildConstraintChecker();
        context.addLast(new RegionWriteHandler(region, tableWriteLatch, checker));
        addIndexAndForeignKeyWriteHandlers(expectedWrites, context);
        return context;
    }

    private BatchConstraintChecker buildConstraintChecker() {
        if (constraintFactories.isEmpty()) {
            return null;
        }
        List<BatchConstraintChecker> checkers = Lists.newArrayListWithCapacity(constraintFactories.size());
        for (ConstraintFactory factory : constraintFactories) {
            checkers.add(factory.getConstraintChecker());
        }
        return new ChainConstraintChecker(checkers);
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
                throw new DoNotRetryIOException("Failed write setup for conglomerate " + conglomId);
            case SHUTDOWN:
                throw new IOException("management for conglomerate " + conglomId + " is shutdown");
        }
    }

    private void addIndexAndForeignKeyWriteHandlers(int expectedWrites, PipelineWriteContext context) throws IOException, InterruptedException {
        isInitialized(context.getTxn());
        //only add constraints and indices when we are in a RUNNING state
        if (state.get() == State.RUNNING) {
            //add Constraint checks before anything else
            if (SpliceConstants.constraintsEnabled) {
                for (ConstraintFactory constraintFactory : constraintFactories) {
                    context.addLast(constraintFactory.create(expectedWrites));
                }
            }

            //add index handlers
            for (LocalWriteFactory indexFactory : indexFactories) {
                indexFactory.addTo(context, true, expectedWrites);
            }

            for (AlterTableWriteFactory alterTableWriteFactory : alterTableWriteFactories) {
                alterTableWriteFactory.addTo(context, true, expectedWrites);
            }

            // FK - child intercept (of inserts/updates)
            if (foreignKeyChildInterceptWriteFactory != null) {
                foreignKeyChildInterceptWriteFactory.addTo(context, false, expectedWrites);
            }

            // FK - child existence check (upon parent update/delete)
            if (foreignKeyChildCheckWriteFactory != null) {
                foreignKeyChildCheckWriteFactory.addTo(context, false, expectedWrites);
            }

            // FK - parent intercept (of deletes/updates)
            if (foreignKeyParentInterceptWriteFactory != null) {
                foreignKeyParentInterceptWriteFactory.addTo(context, false, expectedWrites);
            }

            // FK - parent existence check (upon child insert/update)
            if (foreignKeyParentCheckWriteFactory != null) {
                foreignKeyParentCheckWriteFactory.addTo(context, false, expectedWrites);
            }

        }
    }

    @Override
    public WriteContext createPassThrough(IndexCallBufferFactory indexSharedCallBuffer, TxnView txn, TransactionalRegion region, int expectedWrites, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer, txn, region, env);
        addIndexAndForeignKeyWriteHandlers(expectedWrites, context);
        return context;
    }

    @Override
    public void dropIndex(long indexConglomId, TxnView txn) {
        //ensure that all writes that need to be paused are paused
        synchronized (tableWriteLatch) {
            tableWriteLatch.reset();

            /*
             * Drop the index. We cannot outright drop it, because
             * existing transactions may be still using it. Instead,
             * we replace it with a wrapped transaction
             */
            try {
                synchronized (indexFactories) {
                    for (int i = 0; i < indexFactories.size(); i++) {
                        LocalWriteFactory factory = indexFactories.get(i);
                        if (factory.getConglomerateId() == indexConglomId) {
                            DropIndexFactory wrappedFactory = new DropIndexFactory(txn, factory, indexConglomId);
                            indexFactories.set(i, wrappedFactory);
                            return;
                        }
                    }
                    //it hasn't been added yet, so make sure that we add the index
                    indexFactories.add(new DropIndexFactory(txn, null, indexConglomId));
                }
            } finally {
                tableWriteLatch.countDown();
            }
        }
    }

    private void replace(LocalWriteFactory newFactory) {
        synchronized (indexFactories) {
            for (int i = 0; i < indexFactories.size(); i++) {
                LocalWriteFactory localWriteFactory = indexFactories.get(i);
                if (localWriteFactory.equals(newFactory)) {
                    if (localWriteFactory instanceof DropIndexFactory) {
                        DropIndexFactory dropIndexFactory = (DropIndexFactory) localWriteFactory;
                        if (dropIndexFactory.getDelegate() == null)
                            dropIndexFactory.setDelegate(newFactory);
                    } else
                        indexFactories.set(i, newFactory);

                    return;
                }
            }
            indexFactories.add(newFactory);
        }
    }

    @Override
    public void addIndex(DDLChange ddlChange, int[] columnOrdering, int[] formatIds) {
        synchronized (tableWriteLatch) {
            tableWriteLatch.reset();
            try {
                IndexFactory index = IndexFactory.create(ddlChange, columnOrdering, formatIds);
                replace(index);
            } finally {
                tableWriteLatch.countDown();
            }
        }
    }

    @Override
    public void addForeignKeyParentCheckWriteFactory(int[] backingIndexFormatIds) {
        /* One instance handles all FKs that reference this primary key or unique index */
        this.foreignKeyParentCheckWriteFactory = new ForeignKeyParentCheckWriteFactory(backingIndexFormatIds);
    }

    @Override
    public synchronized void addForeignKeyParentInterceptWriteFactory(String parentTableName, List<Long> backingIndexConglomIds) {
        /* One instance handles all FKs that reference this primary key or unique index */
        if (this.foreignKeyParentInterceptWriteFactory == null) {
            this.foreignKeyParentInterceptWriteFactory = new ForeignKeyParentInterceptWriteFactory(parentTableName, backingIndexConglomIds);
        }
    }

    @Override
    public void addDDLChange(DDLChange ddlChange) {

        DDLChangeType ddlChangeType = ddlChange.getChangeType();
        synchronized (tableWriteLatch) {
            tableWriteLatch.reset();
            try {
                switch (ddlChangeType) {
                    case DROP_COLUMN:
                    case ADD_COLUMN:
                        alterTableWriteFactories.add(AlterTableWriteFactory.create(ddlChange));
                        break;
                    default:
                        break;
                }
            } finally {
                tableWriteLatch.countDown();
            }
        }
    }


    @Override
    public void close() {
        //no-op
    }

    public static LocalWriteContextFactory unmanagedContextFactory() {
        return new LocalWriteContextFactory(-1) {
            @Override
            public void prepare() {
                state.set(State.NOT_MANAGED);
            }
        };
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

        if (!initializationLock.tryLock(STARTUP_LOCK_BACKOFF_PERIOD, TimeUnit.MILLISECONDS)) {
            throw new IndexNotSetUpException("Unable to initialize index management for table " + conglomId
                    + " within a sufficient time frame. Please wait a bit and try again");
        }
        ContextManager currentCm = ContextService.getFactory().getCurrentContextManager();
        SpliceTransactionResourceImpl transactionResource;
        try {
            transactionResource = new SpliceTransactionResourceImpl();
            transactionResource.prepareContextManager();
            try {
                transactionResource.marshallTransaction(txn);

                DataDictionary dataDictionary = transactionResource.getLcc().getDataDictionary();
                ConglomerateDescriptor conglomerateDescriptor = dataDictionary.getConglomerateDescriptor(conglomId);

                if (conglomerateDescriptor != null) {
                    dataDictionary.getExecutionFactory().newExecutionContext(ContextService.getFactory().getCurrentContextManager());
                    //Hbase scan
                    TableDescriptor td = dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());

                    if (td != null) {
                        startDirect(dataDictionary, td, conglomerateDescriptor);
                    }
                }
                state.set(State.RUNNING);
            } catch (SQLException e) {
                SpliceLogUtils.error(LOG, "Unable to acquire a database connection, aborting write, but backing" +
                        "off so that other writes can try again", e);
                state.set(State.READY_TO_START);
                throw new IndexNotSetUpException(e);
            } catch (StandardException | IOException e) {
                SpliceLogUtils.error(LOG, "Unable to set up index management for table " + conglomId + ", aborting", e);
                state.set(State.FAILED_SETUP);
            } finally {
                initializationLock.unlock();

                transactionResource.resetContextManager();
            }
        } catch (SQLException e) {
            SpliceLogUtils.error(LOG, "Unable to acquire a database connection, aborting write, but backing" +
                    "off so that other writes can try again", e);
            state.set(State.READY_TO_START);
            throw new IndexNotSetUpException(e);
        } finally {
            if (currentCm != null)
                ContextService.getFactory().setCurrentContextManager(currentCm);
        }
    }

    private void startDirect(DataDictionary dataDictionary, TableDescriptor td, ConglomerateDescriptor cd) throws StandardException, IOException {
        boolean isSysConglomerate = td.getSchemaDescriptor().getSchemaName().equals("SYS");
        if (isSysConglomerate) {
            SpliceLogUtils.trace(LOG, "Index management for SYS tables disabled, relying on external index management");
            state.set(State.NOT_MANAGED);
            return;
        }

        //get primary key constraint
        //-sf- Hbase scan
        int[] columnOrdering = null;
        int[] formatIds = DataDictionaryUtils.getFormatIds(td.getColumnDescriptorList());
        ConstraintDescriptorList constraintDescriptors = dataDictionary.getConstraintDescriptors(td);
        for (int i = 0; i < constraintDescriptors.size(); i++) {
            ConstraintDescriptor cDescriptor = constraintDescriptors.elementAt(i);
            com.splicemachine.db.catalog.UUID conglomerateId = cDescriptor.getConglomerateId();
            if (conglomerateId != null && td.getConglomerateDescriptor(conglomerateId).getConglomerateNumber() != conglomId)
                continue;

            switch (cDescriptor.getConstraintType()) {
                case DataDictionary.PRIMARYKEY_CONSTRAINT:
                    int[] referencedColumns = cDescriptor.getReferencedColumns();
                    if (referencedColumns != null && referencedColumns.length > 0) {
                        columnOrdering = new int[referencedColumns.length];

                        for (int j = 0; j < referencedColumns.length; ++j) {
                            columnOrdering[j] = referencedColumns[j] - 1;
                        }
                    }
                    constraintFactories.add(buildPrimaryKey(cDescriptor));
                    buildForeignKeyCheckWriteFactory((ReferencedKeyConstraintDescriptor) cDescriptor);
                    break;
                case DataDictionary.UNIQUE_CONSTRAINT:
                    buildUniqueConstraint(cDescriptor);
                    buildForeignKeyCheckWriteFactory((ReferencedKeyConstraintDescriptor) cDescriptor);
                    break;
                case DataDictionary.FOREIGNKEY_CONSTRAINT:
                    ForeignKeyConstraintDescriptor fkConstraintDescriptor = (ForeignKeyConstraintDescriptor) cDescriptor;
                    buildForeignKeyInterceptWriteFactory(dataDictionary, fkConstraintDescriptor);
                    break;
                default:
                    LOG.warn("Unknown Constraint on table " + conglomId + ": type = " + cDescriptor.getConstraintType());
            }
        }

        //get Constraints list
        ConglomerateDescriptorList congloms = td.getConglomerateDescriptorList();
        for (ConglomerateDescriptor conglomDesc : congloms) {
            if (conglomDesc.isIndex()) {
                if (conglomDesc.getConglomerateNumber() == conglomId) {
                    //we are an index, so just map a constraint rather than an attached index
                    addIndexConstraint(td, conglomDesc);
                    indexFactories.clear(); //safe to clear here because we don't chain indices
                    break;
                } else {
                    replace(buildIndex(conglomDesc, constraintDescriptors, columnOrdering, formatIds));
                }
            }
        }

        //check ourself for any additional constraints, but only if it's not present already
        if (!congloms.contains(cd) && cd.isIndex()) {
            //if we have a constraint, use it
            addIndexConstraint(td, cd);
            indexFactories.clear();
        }

        // check tentative indexes
        for (DDLChange ddlChange : DDLCoordinationFactory.getWatcher().getTentativeDDLs()) {
            TentativeDDLDesc ddlDesc = ddlChange.getTentativeDDLDesc();
            TxnView txn = ddlChange.getTxn();
            if (txn.getEffectiveState().isFinal()) {
                DDLCoordinationFactory.getController().finishMetadataChange(ddlChange.getChangeId());
            } else {
                assert ddlDesc != null : "Cannot have a null ddl descriptor!";
                switch (ddlChange.getChangeType()) {
                    case CHANGE_PK:
                    case ADD_CHECK:
                    case CREATE_FK:
                    case ADD_NOT_NULL:
                    case DROP_TABLE:
                        break; //TODO -sf- implement
                    case CREATE_INDEX:
                        if (ddlDesc.getBaseConglomerateNumber() == conglomId)
                            replace(IndexFactory.create(ddlChange, columnOrdering, formatIds));
                        break;
                    case DROP_COLUMN:
                    case ADD_COLUMN:
                        if (ddlDesc.getBaseConglomerateNumber() == conglomId)
                            alterTableWriteFactories.add(AlterTableWriteFactory.create(ddlChange));
                        break;
                    case DROP_INDEX:
                        if (ddlDesc.getBaseConglomerateNumber() == conglomId)
                            dropIndex(ddlDesc.getConglomerateNumber(), txn);

                }
            }
        }
    }

    /* Add factories for intercepting writes to FK backing indexes. */
    private void buildForeignKeyInterceptWriteFactory(DataDictionary dataDictionary, ForeignKeyConstraintDescriptor fkConstraintDesc) throws StandardException {
        ReferencedKeyConstraintDescriptor referencedConstraint = fkConstraintDesc.getReferencedConstraint();
        long keyConglomerateId;
        // FK references unique constraint.
        if (referencedConstraint.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT) {
            keyConglomerateId = referencedConstraint.getIndexConglomerateDescriptor(dataDictionary).getConglomerateNumber();
        }
        // FK references primary key constraint.
        else {
            keyConglomerateId = referencedConstraint.getTableDescriptor().getHeapConglomerateId();
        }
        byte[] hbaseTableNameBytes = Bytes.toBytes(String.valueOf(keyConglomerateId));
        foreignKeyChildInterceptWriteFactory = new ForeignKeyChildInterceptWriteFactory(hbaseTableNameBytes, fkConstraintDesc);
        foreignKeyChildCheckWriteFactory = new ForeignKeyChildCheckWriteFactory(fkConstraintDesc);
    }

    /* Add factories for *checking* existence of FK referenced primary-key or unique-index rows. */
    private void buildForeignKeyCheckWriteFactory(ReferencedKeyConstraintDescriptor cDescriptor) throws StandardException {
        ConstraintDescriptorList fks = cDescriptor.getForeignKeyConstraints(ConstraintDescriptor.ENABLED);
        if (fks.isEmpty()) {
            return;
        }
        ColumnDescriptorList backingIndexColDescriptors = cDescriptor.getColumnDescriptors();
        int backingIndexFormatIds[] = DataDictionaryUtils.getFormatIds(backingIndexColDescriptors);

        addForeignKeyParentCheckWriteFactory(backingIndexFormatIds);
        String parentTableName = cDescriptor.getTableDescriptor().getName();
        List<Long> backingIndexConglomIds = DataDictionaryUtils.getBackingIndexConglomerateIdsForForeignKeys(fks);
        addForeignKeyParentInterceptWriteFactory(parentTableName, backingIndexConglomIds);
    }

    private void buildUniqueConstraint(ConstraintDescriptor cd) throws StandardException {
        ConstraintContext cc = ConstraintContext.unique(cd);
        constraintFactories.add(new ConstraintFactory(new UniqueConstraint(cc)));
    }

    private void addIndexConstraint(TableDescriptor td, ConglomerateDescriptor conglomDesc) {
        IndexDescriptor indexDescriptor = conglomDesc.getIndexDescriptor().getIndexDescriptor();
        if (indexDescriptor.isUnique()) {
            //make sure it's not already in the constraintFactories
            for (ConstraintFactory constraintFactory : constraintFactories) {
                if (constraintFactory.getLocalConstraint().getType() == Constraint.Type.UNIQUE) {
                    return; //we've found a local unique constraint, don't need to add it more than once
                }
            }
            ConstraintContext cc = ConstraintContext.unique(td, conglomDesc);
            constraintFactories.add(new ConstraintFactory(new UniqueConstraint(cc)));
        }
    }

    private IndexFactory buildIndex(ConglomerateDescriptor conglomDesc, ConstraintDescriptorList constraints,
                                    int[] columnOrdering, int[] typeIds) {
        IndexRowGenerator irg = conglomDesc.getIndexDescriptor();
        IndexDescriptor indexDescriptor = irg.getIndexDescriptor();
        if (indexDescriptor.isUnique())
            return IndexFactory.create(conglomDesc.getConglomerateNumber(), indexDescriptor, columnOrdering, typeIds);

        /*
         * just because the conglom descriptor doesn't claim it's unique, doesn't mean that it isn't
         * actually unique. You also need to check the ConstraintDescriptor (if there is one) to see
         * if it has the proper type
         */
        for (Object constraint1 : constraints) {
            ConstraintDescriptor constraint = (ConstraintDescriptor) constraint1;
            UUID conglomerateId = constraint.getConglomerateId();
            if (constraint.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT &&
                    (conglomerateId != null && conglomerateId.equals(conglomDesc.getUUID()))) {
                return IndexFactory.create(conglomDesc.getConglomerateNumber(), indexDescriptor,
                        true, false, columnOrdering, typeIds);
            }
        }

        return IndexFactory.create(conglomDesc.getConglomerateNumber(), indexDescriptor, columnOrdering, typeIds);
    }

    private ConstraintFactory buildPrimaryKey(ConstraintDescriptor cDescriptor) {
        ConstraintContext cc = ConstraintContext.primaryKey(cDescriptor);
        return new ConstraintFactory(new PrimaryKeyConstraint(cc));
    }

    @Override
    public boolean hasDependentWrite(TxnView txn) throws IOException, InterruptedException {
        isInitialized(txn);
        return !indexFactories.isEmpty();
    }

}