package com.splicemachine.pipeline.writecontextfactory;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static com.splicemachine.pipeline.writecontextfactory.ConglomerateDescriptors.isIndex;
import static com.splicemachine.pipeline.writecontextfactory.ConglomerateDescriptors.isUniqueIndex;
import static com.splicemachine.pipeline.writecontextfactory.ConglomerateDescriptors.numberFunction;

/**
 * One instance of this class will exist per conglomerate number per JVM.  Holds state about whether its conglomerate
 * has indexes and constraints, has columns that have been dropped, etc-- necessary information for performing writes
 * on the represented table/index.
 *
 * @author Scott Fines
 *         Created on: 4/30/13
 */
class LocalWriteContextFactory implements WriteContextFactory<TransactionalRegion> {

    private static final Logger LOG = Logger.getLogger(LocalWriteContextFactory.class);
    private static final long STARTUP_LOCK_BACKOFF_PERIOD = SpliceConstants.startupLockWaitPeriod;

    /* Known as "conglomerate number" in the sys tables, this is the number of the physical table (hbase table) that
     * users of this context write to. It may represent a base table or any one of several index tables for a base
     * table. */
    private final long conglomId;

    /* These factories create WriteHandlers that intercept writes to the htable for this context (a base table)
     * transform them, and send them to a remote index table.. */
    private final List<LocalWriteFactory> indexFactories = new CopyOnWriteArrayList<>();

    /* These create WriteHandlers that enforce constrains on the htable users of this context write to. */
    private final Set<ConstraintFactory> constraintFactories = new CopyOnWriteArraySet<>();

    private final Set<AlterTableWriteFactory> alterTableWriteFactories = new CopyOnWriteArraySet<>();

    /**
     * Foreign key WriteHandlers intercept writes to parent/child tables and send them to the corresponding parent/child
     * table for existence checks. There is only one of each (rather than a collection) because these are only
     * present when the enclosing context is for a FK backing index and there is always exactly one index for one FK
     * constraint.
     */
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
        addWriteHandlerFactories(1000, context);
        return context;
    }

    @Override
    public WriteContext create(IndexCallBufferFactory indexSharedCallBuffer,
                               TxnView txn, TransactionalRegion region, int expectedWrites,
                               RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer, txn, region, env);
        BatchConstraintChecker checker = buildConstraintChecker();
        context.addLast(new RegionWriteHandler(region, tableWriteLatch, checker));
        addWriteHandlerFactories(expectedWrites, context);
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

    private void addWriteHandlerFactories(int expectedWrites, PipelineWriteContext context) throws IOException, InterruptedException {
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
        addWriteHandlerFactories(expectedWrites, context);
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

    /**
     * This is the write context for a base table and we are adding or replacing an IndexFactory for one specific
     * index.  IndexFactory equality is based on the index conglomerate number so we end up with at most one IndexFactory
     * per index on the base table (and at most one DropIndexFactory per index).
     */
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
                    case ADD_PRIMARY_KEY:
                    case ADD_UNIQUE_CONSTRAINT:
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

    /**
     * Called once as part of context initialization.
     *
     * @param td The table descriptor for the base table associated with the physical table this context writes to.
     */
    private void startDirect(DataDictionary dataDictionary, TableDescriptor td, ConglomerateDescriptor cd) throws StandardException, IOException {
        boolean isSysConglomerate = td.getSchemaDescriptor().getSchemaName().equals("SYS");
        if (isSysConglomerate) {
            SpliceLogUtils.trace(LOG, "Index management for SYS tables disabled, relying on external index management");
            state.set(State.NOT_MANAGED);
            return;
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        // PART 1: Context configuration for PK, FK, and unique constraints using constraint descriptors.
        //
        // Here we configure the context to the extent possible using the related table's constraint
        // descriptors.  The constraint descriptors (from sys.sysconstraints) provide limited
        // information however-- see notes in part 2.
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        int[] columnOrdering = null;
        int[] formatIds = DataDictionaryUtils.getFormatIds(td.getColumnDescriptorList());
        ConstraintDescriptorList constraintDescriptors = dataDictionary.getConstraintDescriptors(td);
        for (ConstraintDescriptor cDescriptor : constraintDescriptors) {
            UUID conglomerateId = cDescriptor.getConglomerateId();
            if (conglomerateId != null && td.getConglomerateDescriptor(conglomerateId).getConglomerateNumber() != conglomId) {
                continue;
            }

            switch (cDescriptor.getConstraintType()) {
                case DataDictionary.PRIMARYKEY_CONSTRAINT:
                    columnOrdering = DataDictionaryUtils.getColumnOrdering(cDescriptor);
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

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        // PART 2: Context configuration for unique/non-unique indexes using conglomerate descriptors.
        //
        // Configure indices and unique-indices for this context using the conglomerate descriptors of
        // table associated with this context.  We can only setup for non-unique indices this way,
        // because they are not classified as constraints, and thus never have an entry in the constraints
        // table. Why do we handle unique indices here then, instead of when iterating over constraints
        // above?  There seems to be a bug (or bad design?) we inherited from derby wherein unique
        // indices added by the "create index..." statement are not represented in the sys.sysconstraints
        // table.  Consequently we can only account for them by considering conglomerates, as we do here.
        // One last bit of derby weirdness is that multiple conglomerate descriptors can exist for the
        // same conglomerate number (multiple rows in sys.sysconglomerates for the same conglomerate
        // number.  This happens, for example, when a new FK constraint re-uses an existing unique or non-
        // unique index.
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        // This is how we tell if the current context is being configured for a base or index table.
        boolean isIndexTableContext = cd.isIndex();

        //
        // This is a context for an index.
        //
        if(isIndexTableContext) {
            // we are an index, so just map a constraint rather than an attached index
            addIndexConstraint(td, cd);
            // safe to clear here because we don't chain indices -- we don't send index writes to other index tables.
            indexFactories.clear();
        }
        //
        // This is a context for a base table.
        //
        else {
            Iterable<ConglomerateDescriptor> allCongloms = td.getConglomerateDescriptorList();
            // Group by conglomerate number so that we create at most one index config for each conglom number.
            Multimap<Long, ConglomerateDescriptor> numberToDescriptorMap = Multimaps.index(allCongloms, numberFunction());
            for (Long conglomerateNumber : numberToDescriptorMap.keySet()) {
                // The conglomerates just for the current conglomerate number.
                Collection<ConglomerateDescriptor> currentCongloms = numberToDescriptorMap.get(conglomerateNumber);
                // Is there an index?  How about a unique index?
                Optional<ConglomerateDescriptor> indexConglom = Iterables.tryFind(currentCongloms, isIndex());
                Optional<ConglomerateDescriptor> uniqueIndexConglom = Iterables.tryFind(currentCongloms, isUniqueIndex());
                if (indexConglom.isPresent()) {
                    // If this conglomerate number has a unique index conglomerate then the underlying storage (hbase
                    // table) is encoded as a unique index, so use the unique conglom.  Otherwise just use the first
                    // conglom descriptor for the current conglom number.
                    ConglomerateDescriptor srcConglomDesc = uniqueIndexConglom.isPresent() ? uniqueIndexConglom.get() : currentCongloms.iterator().next();
                    IndexDescriptor indexDescriptor = srcConglomDesc.getIndexDescriptor().getIndexDescriptor();
                    IndexFactory indexFactory = IndexFactory.create(srcConglomDesc.getConglomerateNumber(), indexDescriptor, columnOrdering, formatIds);
                    replace(indexFactory);
                }
            }
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        // PART 3: check tentative indexes
        //
        //
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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
                    case ADD_PRIMARY_KEY:
                    case ADD_UNIQUE_CONSTRAINT:
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

    private static ConstraintFactory buildPrimaryKey(ConstraintDescriptor cDescriptor) {
        ConstraintContext cc = ConstraintContext.primaryKey(cDescriptor);
        return new ConstraintFactory(new PrimaryKeyConstraint(cc));
    }

    @Override
    public boolean hasDependentWrite(TxnView txn) throws IOException, InterruptedException {
        isInitialized(txn);
        return !indexFactories.isEmpty();
    }

}