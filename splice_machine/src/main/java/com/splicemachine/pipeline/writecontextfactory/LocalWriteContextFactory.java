package com.splicemachine.pipeline.writecontextfactory;

import static com.splicemachine.pipeline.writecontextfactory.ConglomerateDescriptors.isIndex;
import static com.splicemachine.pipeline.writecontextfactory.ConglomerateDescriptors.isUniqueIndex;
import static com.splicemachine.pipeline.writecontextfactory.ConglomerateDescriptors.numberFunction;
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
import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.protobuf.ProtoUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.constraint.BatchConstraintChecker;
import com.splicemachine.pipeline.constraint.ChainConstraintChecker;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.PrimaryKeyConstraint;
import com.splicemachine.pipeline.constraint.UniqueConstraint;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.IndexCallBufferFactory;
import com.splicemachine.pipeline.writehandler.RegionWriteHandler;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

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

    /* Holds all of the WriteFactor instances related to foreign keys */
    private final FKWriteFactoryHolder fkWriteFactoryHolder = new FKWriteFactoryHolder();

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
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer, txn, rce, false, env);
        BatchConstraintChecker checker = buildConstraintChecker();
        context.addLast(new RegionWriteHandler(rce, tableWriteLatch, checker));
        addWriteHandlerFactories(1000, context);
        return context;
    }

    @Override
    public WriteContext create(IndexCallBufferFactory indexSharedCallBuffer,
                               TxnView txn, TransactionalRegion region, int expectedWrites, boolean skipIndexWrites,
                               RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer, txn, region, skipIndexWrites, env);
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
            for (ConstraintFactory constraintFactory : constraintFactories) {
                context.addLast(constraintFactory.create(expectedWrites));
            }

            //add index handlers
            for (LocalWriteFactory indexFactory : indexFactories) {
                indexFactory.addTo(context, true, expectedWrites);
            }

            for (AlterTableWriteFactory alterTableWriteFactory : alterTableWriteFactories) {
                alterTableWriteFactory.addTo(context, true, expectedWrites);
            }

            // FK - child intercept (of inserts/updates)
            for (ForeignKeyChildInterceptWriteFactory factory : fkWriteFactoryHolder.getChildInterceptWriteFactories()) {
                factory.addTo(context, false, expectedWrites);
            }

            // FK - child existence check (upon parent update/delete)
            if (fkWriteFactoryHolder.hasChildCheck()) {
                fkWriteFactoryHolder.getChildCheckWriteFactory().addTo(context, false, expectedWrites);
            }

            // FK - parent intercept (of deletes/updates)
            if (fkWriteFactoryHolder.hasParentIntercept()) {
                fkWriteFactoryHolder.getParentInterceptWriteFactory().addTo(context, false, expectedWrites);
            }

            // FK - parent existence check (upon child insert/update)
            if (fkWriteFactoryHolder.hasParentCheck()) {
                fkWriteFactoryHolder.getParentCheckWriteFactory().addTo(context, false, expectedWrites);
            }

        }
    }

    @Override
    public WriteContext createPassThrough(IndexCallBufferFactory indexSharedCallBuffer, TxnView txn, TransactionalRegion region, int expectedWrites, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(indexSharedCallBuffer, txn, region, false, env);
        addWriteHandlerFactories(expectedWrites, context);
        return context;
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
    public void addDDLChange(DDLChange ddlChange) {
        DDLChangeType ddlChangeType = ddlChange.getDdlChangeType();
        synchronized (tableWriteLatch) {
            tableWriteLatch.reset();
            try {
                switch(ddlChangeType) {
                    case DROP_COLUMN:
                    case ADD_COLUMN:
                    case ADD_PRIMARY_KEY:
                    case ADD_UNIQUE_CONSTRAINT:
                    case DROP_PRIMARY_KEY:
                        alterTableWriteFactories.add(AlterTableWriteFactory.create(ddlChange));
                        break;
                    case ADD_FOREIGN_KEY:
                        fkWriteFactoryHolder.handleForeignKeyAdd(ddlChange,conglomId);
                        break;
                    case DROP_FOREIGN_KEY:
                        fkWriteFactoryHolder.handleForeignKeyDrop(ddlChange,conglomId);
                        break;
                    case CREATE_INDEX:
                        IndexFactory index = IndexFactory.create(ddlChange);
                        replace(index);
                        break;
                    case DROP_INDEX_TRIGGER:
                        TentativeIndex ti = ddlChange.getTentativeIndex();
                        TxnView txn = DDLUtils.getLazyTransaction(ddlChange.getTxnId());
                        long indexConglomId = ti.getIndex().getConglomerate();
                        synchronized (indexFactories) {
                            for (int i = 0; i < indexFactories.size(); i++) {
                                LocalWriteFactory factory = indexFactories.get(i);
                                if (factory.getConglomerateId() == indexConglomId) {
                                    DropIndexFactory wrappedFactory = new DropIndexFactory(
                                            txn, factory,
                                            indexConglomId);
                                    indexFactories.set(i, wrappedFactory);
                                    return;
                                }
                            }
                            //it hasn't been added yet, so make sure that we add the index
                            indexFactories.add(new DropIndexFactory(txn, null, indexConglomId));
                        }
                        break;
                    // ignored
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
                        startDirect(transactionResource.getLcc(), dataDictionary, td, conglomerateDescriptor);
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
    private void startDirect(LanguageConnectionContext lcc, DataDictionary dataDictionary, TableDescriptor td, ConglomerateDescriptor cd) throws StandardException, IOException {
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
        ConstraintDescriptorList constraintDescriptors = dataDictionary.getConstraintDescriptors(td);
        for (ConstraintDescriptor cDescriptor : constraintDescriptors) {
            UUID conglomerateId = cDescriptor.getConglomerateId();
            if (conglomerateId != null && td.getConglomerateDescriptor(conglomerateId).getConglomerateNumber() != conglomId) {
                continue;
            }

            switch (cDescriptor.getConstraintType()) {
                case DataDictionary.PRIMARYKEY_CONSTRAINT:
                    constraintFactories.add(buildPrimaryKey(cDescriptor));
                    fkWriteFactoryHolder.buildForeignKeyCheckWriteFactory((ReferencedKeyConstraintDescriptor) cDescriptor);
                    break;
                case DataDictionary.UNIQUE_CONSTRAINT:
                    buildUniqueConstraint(cDescriptor);
                    fkWriteFactoryHolder.buildForeignKeyCheckWriteFactory((ReferencedKeyConstraintDescriptor) cDescriptor);
                    break;
                case DataDictionary.FOREIGNKEY_CONSTRAINT:
                    ForeignKeyConstraintDescriptor fkConstraintDescriptor = (ForeignKeyConstraintDescriptor) cDescriptor;
                    fkWriteFactoryHolder.buildForeignKeyInterceptWriteFactory(dataDictionary, fkConstraintDescriptor);
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

        //
        // This is how we tell if the current context is being configured for a base or index table.
        //
        if(cd.isIndex()) {
            // we are an index, so just map a constraint rather than an attached index
            addUniqueIndexConstraint(td, cd);
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

                    TentativeIndex ti = ProtoUtil.createTentativeIndex(lcc, td.getBaseConglomerateDescriptor().getConglomerateNumber(),
                            indexConglom.get().getConglomerateNumber(),td,indexDescriptor);
                    IndexFactory indexFactory = IndexFactory.create(ti);
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
            TxnView txn = DDLUtils.getLazyTransaction(ddlChange.getTxnId());
            if (txn.getEffectiveState().isFinal()) {
                DDLCoordinationFactory.getController().finishMetadataChange(ddlChange.getChangeId());
            } else {
                addDDLChange(ddlChange);
            }
        }
    }

    private void buildUniqueConstraint(ConstraintDescriptor cd) throws StandardException {
        ConstraintContext cc = ConstraintContext.unique(cd);
        constraintFactories.add(new ConstraintFactory(new UniqueConstraint(cc)));
    }

    private void addUniqueIndexConstraint(TableDescriptor td, ConglomerateDescriptor conglomDesc) {
        IndexDescriptor indexDescriptor = conglomDesc.getIndexDescriptor().getIndexDescriptor();
        if (indexDescriptor.isUnique()) {
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
