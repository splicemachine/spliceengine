package com.splicemachine.derby.impl.sql.execute;

import com.carrotsearch.hppc.BitSet;
import com.google.common.collect.Lists;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.derby.impl.sql.execute.AlterTable.DropColumnHandler;
import com.splicemachine.derby.impl.sql.execute.constraint.*;
import com.splicemachine.derby.impl.sql.execute.index.*;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.hbase.batch.*;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
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
 * @author Scott Fines
 * Created on: 4/30/13
 */
public class LocalWriteContextFactory implements WriteContextFactory<TransactionalRegion> {
    private static final Logger LOG = Logger.getLogger(LocalWriteContextFactory.class);

    private static final long STARTUP_LOCK_BACKOFF_PERIOD = SpliceConstants.startupLockWaitPeriod;

    //TODO -sf- hook this into JMX
    private static final int writeBatchSize = Integer.MAX_VALUE;

    private final long congomId;
    private final List<LocalWriteFactory> indexFactories = new CopyOnWriteArrayList<LocalWriteFactory>();
    private final Set<ConstraintFactory> constraintFactories = new CopyOnWriteArraySet<ConstraintFactory>();
    private final Set<DropColumnFactory> dropColumnFactories = new CopyOnWriteArraySet<DropColumnFactory>();

    private final ReentrantLock initializationLock = new ReentrantLock();
    /*
     * Latch for blocking writes during the updating of table metadata (adding constraints, indices,
     * etc).
     */
    private final ResettableCountDownLatch tableWriteLatch = new ResettableCountDownLatch(1);

    protected final AtomicReference<State> state = new AtomicReference<State>(State.WAITING_TO_START);

    public void prepare(){
        state.compareAndSet(State.WAITING_TO_START, State.READY_TO_START);
    }

    public LocalWriteContextFactory(long congomId) {
        this.congomId = congomId;
        tableWriteLatch.countDown();
    }

    @Override
    public WriteContext create(TxnView txn, TransactionalRegion region, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(txn,region, env);
        context.addLast(new RegionWriteHandler(region, tableWriteLatch, writeBatchSize, null));
        addIndexInformation(1000, context);
        return context;
    }

    @Override
    public WriteContext create(TxnView txn, TransactionalRegion region,
                               int expectedWrites, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(txn,region, env);
				BatchConstraintChecker checker = buildConstraintChecker();
        context.addLast(new RegionWriteHandler(region, tableWriteLatch, writeBatchSize,checker));
        addIndexInformation(expectedWrites, context);
        return context;
    }

		private BatchConstraintChecker buildConstraintChecker() {
				if(constraintFactories.size()<=0) return null;
				List<BatchConstraintChecker> checkers = Lists.newArrayListWithCapacity(constraintFactories.size());
				for(ConstraintFactory factory:constraintFactories){
						checkers.add(factory.getConstraintChecker());
				}
				return new ChainConstraintChecker(checkers);
		}

		private void addIndexInformation(int expectedWrites, PipelineWriteContext context) throws IOException, InterruptedException {
        TxnView txn = context.getTxn();
        switch (state.get()) {
            case READY_TO_START:
                SpliceLogUtils.trace(LOG, "Index management for conglomerate %d " +
                        "has not completed, attempting to start now", congomId);
                start(txn);
                break;
            case STARTING:
                SpliceLogUtils.trace(LOG,"Index management is starting up");
                start(txn);
                break;
            case FAILED_SETUP:
                //since we haven't done any writes yet, it's safe to just explore
                throw new DoNotRetryIOException("Failed write setup for congolomerate "+congomId);
            case SHUTDOWN:
                throw new IOException("management for conglomerate "+ congomId+" is shutdown");
        }
        //only add constraints and indices when we are in a RUNNING state
        if(state.get()== State.RUNNING){
            //add Constraint checks before anything else
						if(SpliceConstants.constraintsEnabled){
								for(ConstraintFactory constraintFactory:constraintFactories){
										context.addLast(constraintFactory.create());
								}
						}

            //add index handlers
            for(LocalWriteFactory indexFactory:indexFactories){
                indexFactory.addTo(context,true,expectedWrites);
            }

            for(DropColumnFactory dropColumnFactory:dropColumnFactories) {
                dropColumnFactory.addTo(context,true,expectedWrites);
            }
        }
    }

    @Override
    public WriteContext createPassThrough(TxnView txn, TransactionalRegion region, int expectedWrites, RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(txn,region, env);
        addIndexInformation(expectedWrites, context);

        return context;
    }

    @Override
    public void dropIndex(long indexConglomId,TxnView txn) {
        //ensure that all writes that need to be paused are paused
        synchronized (tableWriteLatch){
            tableWriteLatch.reset();

            /*
             * Drop the index. We cannot outright drop it, because
             * existing transactions may be still using it. Instead,
             * we replace it with a wrapped transaction
             */
            try{
                for(int i=0;i<indexFactories.size();i++){
                    LocalWriteFactory factory = indexFactories.get(i);
                    if(factory.getConglomerateId()==indexConglomId){
                        DropIndexFactory wrappedFactory = new DropIndexFactory(txn,factory);
                        indexFactories.set(i,wrappedFactory);
                        return;
                    }
                }
            }finally{
                tableWriteLatch.countDown();
            }
        }
    }

    private void replace(LocalWriteFactory newFactory) {
        for(int i=0;i<indexFactories.size();i++){
            LocalWriteFactory localWriteFactory = indexFactories.get(i);
            if(localWriteFactory.equals(newFactory)){
                indexFactories.set(i, newFactory);
                return;
            }
        }
        indexFactories.add(newFactory);
    }

    @Override
    public void addIndex(DDLChange ddlChange, int[] columnOrdring, int[] formatIds) {
        synchronized (tableWriteLatch){
            tableWriteLatch.reset();
            try{
                IndexFactory index = IndexFactory.create(ddlChange, columnOrdring, formatIds);
                replace(index);
            }finally{
                tableWriteLatch.countDown();
            }
        }
    }

    @Override
    public void addDDLChange(DDLChange ddlChange) {

        DDLChangeType ddlChangeType = ddlChange.getChangeType();
        synchronized (tableWriteLatch){
            tableWriteLatch.reset();
            try{
                switch (ddlChangeType) {
                    case DROP_COLUMN:
                        dropColumnFactories.add(DropColumnFactory.create(ddlChange));
                        break;
                    default:
                        break;
                }
            }finally{
                tableWriteLatch.countDown();
            }
        }
    }

    public static LocalWriteContextFactory unmanagedContextFactory(){
        return new LocalWriteContextFactory(-1){
            @Override
            public void prepare() {
                state.set(State.NOT_MANAGED);
            }
        };
    }

    /*
     * State management indicator
     */
    private enum State{
        /*
         * Mutations which see this state will emit a warning, but will still be allowed through.
         * Allowing mutations through during this phase prevents deadlocks when initialized the Database
         * for the first time. After the Derby DataDictionary is properly instantiated, no index set should
         * remain in this state for long.
         */
        WAITING_TO_START,
        /*
         * Indicates that the Index Set is able to start whenever it feels like it (typically before the
         * first mutation to the main table). IndexSets in this state believe that the Derby DataDictionary
         * has properly instantiated.
         */
        READY_TO_START,
        /*
         * Indicates that this set is currently in the process of intantiating all the indices, constraints, and
         * so forth, and that all mutations should block until set up is complete.
         */
        STARTING,
        /*
         * Indicates that initialization failed in some fatal way. Mutations encountering this state
         * will blow back on the caller with a DoNotRetryIOException
         */
        FAILED_SETUP,
        /*
         * The IndexSet is running, and index management is going smoothly
         */
        RUNNING,
        /*
         * the IndexSet is shutdown. Mutations will explode upon encountering this.
         */
        SHUTDOWN,
        /*
         * This IndexSet does not manage Mutations.
         */
        NOT_MANAGED
    }

    private void start(TxnView txn) throws IOException,InterruptedException{
        /*
         * Ready to Start => switch to STARTING
         * STARTING => continue through to block on the lock
         * Any other state => return, because we don't need to perform this method.
         */
        if(!state.compareAndSet(State.READY_TO_START,State.STARTING)){
            if(state.get()!=State.STARTING) return;
        }

        //someone else may have initialized this if so, we don't need to repeat, so return
        if(state.get()!=State.STARTING) return;

        SpliceLogUtils.debug(LOG,"Setting up index for conglomerate %d",congomId);

        if(!initializationLock.tryLock(STARTUP_LOCK_BACKOFF_PERIOD, TimeUnit.MILLISECONDS)){
            throw new IndexNotSetUpException("Unable to initialize index management for table "+ congomId
                    +" within a sufficient time frame. Please wait a bit and try again");
        }
        ContextManager currentCm = ContextService.getFactory().getCurrentContextManager();
        SpliceTransactionResourceImpl transactionResource;
        try {
            transactionResource = new SpliceTransactionResourceImpl();
            transactionResource.prepareContextManager();
            try{
                transactionResource.marshallTransaction(txn);

                DataDictionary dataDictionary= transactionResource.getLcc().getDataDictionary();
                ConglomerateDescriptor conglomerateDescriptor = dataDictionary.getConglomerateDescriptor(congomId);

                if (conglomerateDescriptor != null) {
                    dataDictionary.getExecutionFactory().newExecutionContext(ContextService.getFactory().getCurrentContextManager());
                    //Hbase scan
                    TableDescriptor td = dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());

                    if(td!=null){
                        startDirect(dataDictionary,td,conglomerateDescriptor);
                    }
                }
                state.set(State.RUNNING);
            } catch (SQLException e) {
                SpliceLogUtils.error(LOG,"Unable to acquire a database connection, aborting write, but backing" +
                        "off so that other writes can try again",e);
                state.set(State.READY_TO_START);
                throw new IndexNotSetUpException(e);
            } catch (StandardException e) {
                SpliceLogUtils.error(LOG,"Unable to set up index management for table "+ congomId+", aborting",e);
                state.set(State.FAILED_SETUP);
            }catch(IOException ioe){
                SpliceLogUtils.error(LOG,"Unable to set up index management for table "+ congomId+", aborting",ioe);
                state.set(State.FAILED_SETUP);
            }finally{
                initializationLock.unlock();

                transactionResource.resetContextManager();
            }
        } catch (SQLException e) {
            SpliceLogUtils.error(LOG,"Unable to acquire a database connection, aborting write, but backing" +
                    "off so that other writes can try again",e);
            state.set(State.READY_TO_START);
            throw new IndexNotSetUpException(e);
        } finally {
            if(currentCm!=null)
                ContextService.getFactory().setCurrentContextManager(currentCm);
        }
    }

    private void startDirect(DataDictionary dataDictionary,TableDescriptor td,ConglomerateDescriptor cd) throws StandardException,IOException{
//        indexFactories.clear();
        boolean isSysConglomerate = td.getSchemaDescriptor().getSchemaName().equals("SYS");
        if(isSysConglomerate){
            SpliceLogUtils.trace(LOG,"Index management for SYS tables disabled, relying on external index management");
            state.set(State.NOT_MANAGED);
            return;
        }

        //get primary key constraint
        //-sf- Hbase scan
        int[] columnOrdering = null;
        int[] formatIds;
        ColumnDescriptorList cdList = td.getColumnDescriptorList();
        int size = cdList.size();
        formatIds= new int[size];
        for (int j = 0; j < cdList.size(); ++j) {
            ColumnDescriptor columnDescriptor = cdList.elementAt(j);
            formatIds[j] = columnDescriptor.getType().getNull().getTypeFormatId();
        }
        ConstraintDescriptorList constraintDescriptors = dataDictionary.getConstraintDescriptors(td);
        for(int i=0;i<constraintDescriptors.size();i++){
            ConstraintDescriptor cDescriptor = constraintDescriptors.elementAt(i);
            org.apache.derby.catalog.UUID conglomerateId = cDescriptor.getConglomerateId();
            if(conglomerateId != null && td.getConglomerateDescriptor(conglomerateId).getConglomerateNumber()!=congomId)
                continue;

            switch(cDescriptor.getConstraintType()){
                case DataDictionary.PRIMARYKEY_CONSTRAINT:
                    int[] referencedColumns = cDescriptor.getReferencedColumns();
                    if (referencedColumns != null && referencedColumns.length > 0){
                        columnOrdering = new int[referencedColumns.length];

                        for (int j = 0; j < referencedColumns.length; ++j){
                            columnOrdering[j] = referencedColumns[j] - 1;
                        }
                    }

                    constraintFactories.add(buildPrimaryKey(cDescriptor));
                    break;
                case DataDictionary.UNIQUE_CONSTRAINT:
                    buildUniqueConstraint(cDescriptor);
                    break;
                default:
                    LOG.warn("Unknown Constraint on table "+ congomId+": type = "+ cDescriptor.getConstraintType());
            }
        }

        //get Constraints list
        ConglomerateDescriptorList congloms = td.getConglomerateDescriptorList();
        for (Object conglom : congloms) {
            ConglomerateDescriptor conglomDesc = (ConglomerateDescriptor) conglom;
            if (conglomDesc.isIndex()) {
                if (conglomDesc.getConglomerateNumber() == congomId) {
                    //we are an index, so just map a constraint rather than an attached index
                    addIndexConstraint(td, conglomDesc);
                    indexFactories.clear(); //safe to clear here because we don't chain indices
                    break;
                } else {
                    replace(buildIndex(conglomDesc,constraintDescriptors,columnOrdering,formatIds));
                }
            }
        }

        //check ourself for any additional constraints, but only if it's not present already
        if(!congloms.contains(cd)&&cd.isIndex()){
            //if we have a constraint, use it
            addIndexConstraint(td,cd);
            indexFactories.clear();
        }

        // check tentative indexes
        for (DDLChange ddlChange : DDLCoordinationFactory.getWatcher().getTentativeDDLs()) {
            TentativeDDLDesc ddlDesc = ddlChange.getTentativeDDLDesc();
            TxnView txn = ddlChange.getTxn();
            if (txn.getEffectiveState().isFinal()) {
                DDLCoordinationFactory.getController().finishMetadataChange(ddlChange.getChangeId());
            } else if (ddlDesc.getBaseConglomerateNumber() == congomId) {

                if(ddlChange.getChangeType() == DDLChangeType.CREATE_INDEX) {
                    replace(IndexFactory.create(ddlChange, columnOrdering, formatIds));
                }
                else if (ddlChange.getChangeType() == DDLChangeType.DROP_COLUMN) {
                    dropColumnFactories.add(DropColumnFactory.create(ddlChange));
                }
            }
        }
    }

    private void buildUniqueConstraint(ConstraintDescriptor cd) throws StandardException {
        constraintFactories.add(new ConstraintFactory(UniqueConstraint.create(new ConstraintContext(cd))));
    }

    private void addIndexConstraint(TableDescriptor td, ConglomerateDescriptor conglomDesc) {
        IndexDescriptor indexDescriptor = conglomDesc.getIndexDescriptor().getIndexDescriptor();
        if(indexDescriptor.isUnique()){
            //make sure it's not already in the constraintFactories
            for(ConstraintFactory constraintFactory:constraintFactories){
               if(constraintFactory.localConstraint.getType()== Constraint.Type.UNIQUE){
                   return; //we've found a local unique constraint, don't need to add it more than once
               }
            }
            constraintFactories.add(new ConstraintFactory(UniqueConstraint.create(new ConstraintContext(td, conglomDesc))));
        }
    }

    private IndexFactory buildIndex(ConglomerateDescriptor conglomDesc,ConstraintDescriptorList constraints,
                                    int[] columnOrdering, int[] typeIds) {
        IndexRowGenerator irg = conglomDesc.getIndexDescriptor();
        IndexDescriptor indexDescriptor = irg.getIndexDescriptor();
        if(indexDescriptor.isUnique())
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
        return new ConstraintFactory(new PrimaryKey(new ConstraintContext(cDescriptor)));
    }

    private static class ConstraintFactory{
        private final Constraint localConstraint;

        private ConstraintFactory(Constraint localConstraint) {
            this.localConstraint = localConstraint;
        }

        public WriteHandler create(){
            return new ConstraintHandler(localConstraint);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ConstraintFactory)) return false;

            ConstraintFactory that = (ConstraintFactory) o;

            return localConstraint.equals(that.localConstraint);
        }

        @Override
        public int hashCode() {
            return localConstraint.hashCode();
        }

				public BatchConstraintChecker getConstraintChecker() {
						return localConstraint.asChecker();
				}
		}

    private static class IndexFactory implements LocalWriteFactory{
        private final long indexConglomId;
        private final byte[] indexConglomBytes;
        private final boolean isUnique;
        private final boolean isUniqueWithDuplicateNulls;
        private BitSet indexedColumns;
        private int[] mainColToIndexPosMap;
        private BitSet descColumns;
        private DDLChange ddlChange;
        private int[] baseTableColumnOrdering;
        private int[] formatIds;

        private IndexFactory(long indexConglomId){
            this.indexConglomId = indexConglomId;
            this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
            this.isUnique=false;
            isUniqueWithDuplicateNulls = false;
        }

        private IndexFactory(long indexConglomId,BitSet indexedColumns,int[] mainColToIndexPosMap,boolean isUnique,
                             boolean isUniqueWithDuplicateNulls,BitSet descColumns, DDLChange ddlChange,
                             int[] columnOrdering, int[] formatIds) {
            this.indexConglomId = indexConglomId;
            this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
            this.isUnique=isUnique;
            this.isUniqueWithDuplicateNulls=isUniqueWithDuplicateNulls;
            this.indexedColumns=indexedColumns;
            this.mainColToIndexPosMap=mainColToIndexPosMap;
            this.descColumns = descColumns;
            this.ddlChange = ddlChange;
            this.baseTableColumnOrdering = columnOrdering;
            this.formatIds = formatIds;
        }

        public static IndexFactory create(long conglomerateNumber,int[] indexColsToMainColMap,boolean isUnique,boolean isUniqueWithDuplicateNulls,BitSet descColumns,
                                          int[] columnOrdering, int[] formatIds){
            BitSet indexedCols = getIndexedCols(indexColsToMainColMap);
            int[] mainColToIndexPosMap = getMainColToIndexPosMap(indexColsToMainColMap, indexedCols);

            return new IndexFactory(conglomerateNumber,indexedCols,mainColToIndexPosMap,isUnique, isUniqueWithDuplicateNulls,
                    descColumns, null, columnOrdering, formatIds);
        }

        public static IndexFactory create(DDLChange ddlChange, int[] columnOrdering, int[] formatIds){
            TentativeIndexDesc tentativeIndexDesc = (TentativeIndexDesc)ddlChange.getTentativeDDLDesc();
            int[] indexColsToMainColMap = tentativeIndexDesc.getIndexColsToMainColMap();
            BitSet indexedCols = getIndexedCols(indexColsToMainColMap);
            int[] mainColToIndexPosMap = getMainColToIndexPosMap(indexColsToMainColMap, indexedCols);

            return new IndexFactory(tentativeIndexDesc.getConglomerateNumber(),indexedCols,mainColToIndexPosMap,tentativeIndexDesc.isUnique(),
                                    tentativeIndexDesc.isUniqueWithDuplicateNulls(),tentativeIndexDesc.getDescColumns(), ddlChange,
                                    columnOrdering, formatIds);
        }

        private static int[] getMainColToIndexPosMap(int[] indexColsToMainColMap, BitSet indexedCols) {
            int[] mainColToIndexPosMap = new int[(int)indexedCols.length()];
            for(int indexCol=0;indexCol<indexColsToMainColMap.length;indexCol++){
                int mainCol = indexColsToMainColMap[indexCol];
                mainColToIndexPosMap[mainCol-1] = indexCol;
            }
            return mainColToIndexPosMap;
        }

        private static BitSet getIndexedCols(int[] indexColsToMainColMap) {
            BitSet indexedCols = new BitSet();
            for(int indexCol:indexColsToMainColMap){
                indexedCols.set(indexCol-1);
            }
            return indexedCols;
        }

        public static IndexFactory create(long conglomerateNumber, IndexDescriptor indexDescriptor,
                                          int[] columnOrdering, int[] formatIds) {
            return create(conglomerateNumber, indexDescriptor,indexDescriptor.isUnique(),
                          indexDescriptor.isUniqueWithDuplicateNulls(), columnOrdering, formatIds);
        }

        public static IndexFactory create(long conglomerateNumber, IndexDescriptor indexDescriptor,boolean isUnique,
                                          boolean isUniqueWithDuplicateNulls, int[] columnOrdering, int[] formatIds) {
            int[] indexColsToMainColMap = indexDescriptor.baseColumnPositions();

            //get the descending columns
            boolean[] ascending = indexDescriptor.isAscending();
            BitSet descColumns = new BitSet();
            for(int i=0;i<ascending.length;i++){
                if(!ascending[i])
                    descColumns.set(i);
            }
            return create(conglomerateNumber,indexColsToMainColMap,isUnique,isUniqueWithDuplicateNulls,descColumns,
                    columnOrdering,formatIds);
        }

        @Override
        public void addTo(PipelineWriteContext ctx,boolean keepState,int expectedWrites) throws IOException {
            IndexDeleteWriteHandler deleteHandler =
                    new IndexDeleteWriteHandler(indexedColumns, mainColToIndexPosMap, indexConglomBytes, descColumns,
                            keepState, isUnique, isUniqueWithDuplicateNulls, expectedWrites, baseTableColumnOrdering, formatIds);
            IndexUpsertWriteHandler writeHandler;
            if (isUnique) {
                writeHandler = new UniqueIndexUpsertWriteHandler(indexedColumns, mainColToIndexPosMap,
                        indexConglomBytes, descColumns, keepState, isUniqueWithDuplicateNulls, expectedWrites,
                        baseTableColumnOrdering, formatIds);
            } else {
                writeHandler = new IndexUpsertWriteHandler(indexedColumns, mainColToIndexPosMap, indexConglomBytes,
                        descColumns, keepState, false, false, expectedWrites, baseTableColumnOrdering, formatIds);
            }
            if (ddlChange == null) {
                ctx.addLast(deleteHandler);
                ctx.addLast(writeHandler);
            } else {
                DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController()
                        .newDDLFilter(ddlChange.getTxn());
                ctx.addLast(new SnapshotIsolatedWriteHandler(deleteHandler, ddlFilter));
                ctx.addLast(new SnapshotIsolatedWriteHandler(writeHandler, ddlFilter));
            }
        }

        @Override
        public long getConglomerateId() {
            return indexConglomId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if(o instanceof IndexFactory){
                IndexFactory that = (IndexFactory) o;

                return indexConglomId == that.indexConglomId;
            }else if(o instanceof DropIndexFactory){
                DropIndexFactory that = (DropIndexFactory) o;

                return indexConglomId == that.delegate.getConglomerateId();

            }else return false;
        }

        @Override
        public int hashCode() {
            return (int) (indexConglomId ^ (indexConglomId >>> 32));
        }

        public static IndexFactory wrap(long indexConglomId) {
            return new IndexFactory(indexConglomId);
        }
    }

    private static class DropIndexFactory implements LocalWriteFactory{
        private TxnView txn;
        private LocalWriteFactory delegate;

        private DropIndexFactory(TxnView txn, LocalWriteFactory delegate) {
            this.txn = txn;
            this.delegate = delegate;
        }

        @Override
        public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
            /*
             * We only want to add an entry if one of the following holds:
             *
             * 1. txn is not yet committed
             * 2. ctx.txn.beginTimestamp < txn.commitTimestamp
             */
            long commitTs = txn.getEffectiveCommitTimestamp();
            boolean shouldAdd = commitTs < 0 || ctx.getTxn().getBeginTimestamp() < commitTs;

            if(shouldAdd) delegate.addTo(ctx,keepState,expectedWrites);
        }

        @Override public long getConglomerateId() { return delegate.getConglomerateId(); }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if(o instanceof DropIndexFactory)
                return delegate.equals(((DropIndexFactory) o).delegate);
            else return o instanceof IndexFactory && delegate.equals(o);
        }

        @Override public int hashCode() { return delegate.hashCode(); }
    }

    private static class DropColumnFactory implements LocalWriteFactory{
        private UUID tableId;
        private TxnView txn;
        private long newConglomId;
        private ColumnInfo[] columnInfos;
        private int droppedColumnPosition;
        private DDLChange ddlChange;

        public DropColumnFactory(UUID tableId,
                                 TxnView txn,
                                 long newConglomId,
                                 ColumnInfo[] columnInfos,
                                 int droppedColumnPosition,
                                 DDLChange ddlChange) {
            this.tableId = tableId;
            this.txn = txn;
            this.newConglomId = newConglomId;
            this.columnInfos = columnInfos;
            this.droppedColumnPosition = droppedColumnPosition;
            this.ddlChange = ddlChange;
        }

        public static DropColumnFactory create(DDLChange ddlChange) {
            if (ddlChange.getChangeType() != DDLChangeType.DROP_COLUMN)
                return null;

            TentativeDropColumnDesc desc = (TentativeDropColumnDesc)ddlChange.getTentativeDDLDesc();

            UUID tableId = desc.getTableId();
            TxnView txn = ddlChange.getTxn();
            long newConglomId = desc.getConglomerateNumber();
            ColumnInfo[] columnInfos = desc.getColumnInfos();
            int droppedColumnPosition = desc.getDroppedColumnPosition();
            return new DropColumnFactory(tableId, txn, newConglomId, columnInfos, droppedColumnPosition, ddlChange);
        }

        @Override
        public void addTo(PipelineWriteContext ctx,boolean keepState, int expectedWrites) throws IOException{
            DropColumnHandler handler = new DropColumnHandler(tableId, newConglomId, txn, columnInfos, droppedColumnPosition);
            if (ddlChange == null) {
                ctx.addLast(handler);
            } else {
                    DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController().newDDLFilter(ddlChange.getTxn());
                ctx.addLast(new SnapshotIsolatedWriteHandler(handler, ddlFilter));
            }
        }

        @Override public long getConglomerateId() { return newConglomId; }
    }
}
