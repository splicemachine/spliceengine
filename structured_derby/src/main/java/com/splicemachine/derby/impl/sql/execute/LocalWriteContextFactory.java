package com.splicemachine.derby.impl.sql.execute;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.constraint.*;
import com.splicemachine.derby.impl.sql.execute.index.*;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.hbase.batch.*;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.tools.ResettableCountDownLatch;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public class LocalWriteContextFactory implements WriteContextFactory<RegionCoprocessorEnvironment> {
    private static final Logger LOG = Logger.getLogger(LocalWriteContextFactory.class);

    private static final long STARTUP_LOCK_BACKOFF_PERIOD = SpliceConstants.startupLockWaitPeriod;

    //TODO -sf- hook this into JMX
    private static final int writeBatchSize = Integer.MAX_VALUE;

    private final long congomId;
    private final Set<IndexFactory> indexFactories = new CopyOnWriteArraySet<IndexFactory>();
    private final Set<ConstraintFactory> constraintFactories = new CopyOnWriteArraySet<ConstraintFactory>();

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
    public WriteContext create(String txnId,RegionCoprocessorEnvironment rce) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(txnId,rce);
        context.addLast(new RegionWriteHandler(rce.getRegion(), tableWriteLatch, writeBatchSize, null));
        addIndexInformation(1000, context);
        return context;
    }

    @Override
    public WriteContext create(String txnId, RegionCoprocessorEnvironment rce,
                               RollForwardQueue<byte[], ByteBuffer> queue,int expectedWrites) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(txnId,rce);
        context.addLast(new RegionWriteHandler(rce.getRegion(), tableWriteLatch, writeBatchSize, queue));
        addIndexInformation(expectedWrites, context);
        return context;
    }

    private void addIndexInformation(int expectedWrites, PipelineWriteContext context) throws IOException, InterruptedException {
        switch (state.get()) {
            case READY_TO_START:
                SpliceLogUtils.trace(LOG, "Index management for conglomerate %d " +
                        "has not completed, attempting to start now", congomId);
                start();
                break;
            case STARTING:
                SpliceLogUtils.trace(LOG,"Index management is starting up");
                start();
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
            for(ConstraintFactory constraintFactory:constraintFactories){
                context.addLast(constraintFactory.create());
            }

            //add index handlers
            for(IndexFactory indexFactory:indexFactories){
                indexFactory.addTo(context,true,expectedWrites);
            }
        }
    }

    @Override
    public WriteContext createPassThrough(String txnId,RegionCoprocessorEnvironment key,int expectedWrites) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(txnId,key);
        addIndexInformation(expectedWrites, context);

        return context;
    }

    @Override
    public void dropIndex(long indexConglomId) { // XXX - TODO JLEACH - Cannot do this...
        //ensure that all writes that need to be paused are paused
        synchronized (tableWriteLatch){
            tableWriteLatch.reset();

            //drop the index
            try{
                indexFactories.remove(IndexFactory.wrap(indexConglomId));
            }finally{
                tableWriteLatch.countDown();
            }
        }
    }

    @Override
    public void addIndex(long indexConglomId, BitSet indexedColumns,int[] mainColToIndexPosMap, boolean unique,BitSet descColumns) {
        synchronized (tableWriteLatch){
            tableWriteLatch.reset();
            try{
                indexFactories.add(new IndexFactory(indexConglomId, indexedColumns, mainColToIndexPosMap, unique,descColumns));
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

    private void start() throws IOException,InterruptedException{
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
        TransactorControl transactor = null;
        TransactionId txnId = null;
        boolean success = false;
        ContextManager currentCm = ContextService.getFactory().getCurrentContextManager();
        SpliceTransactionResourceImpl transactionResource = null;
        try {
            transactionResource = new SpliceTransactionResourceImpl();
            transactionResource.prepareContextManager();
            try{
                transactor = HTransactorFactory.getTransactorControl();
                txnId = transactor.beginTransaction(false,true,true);
                transactionResource.marshallTransaction(txnId.getTransactionIdString());

                DataDictionary dataDictionary= transactionResource.getLcc().getDataDictionary();
                ConglomerateDescriptor conglomerateDescriptor = dataDictionary.getConglomerateDescriptor(congomId);

                dataDictionary.getExecutionFactory().newExecutionContext(ContextService.getFactory().getCurrentContextManager());
                //Hbase scan
                TableDescriptor td = dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());

                if(td!=null){
                    startDirect(dataDictionary,td,conglomerateDescriptor);
                }
                transactor.commit(txnId);
                state.set(State.RUNNING);
                success=true;
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
                if(!success&&(transactor!=null && txnId !=null))
                    transactor.rollback(txnId);
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
        indexFactories.clear();
        boolean isSysConglomerate = td.getSchemaDescriptor().getSchemaName().equals("SYS");
        if(isSysConglomerate){
            SpliceLogUtils.trace(LOG,"Index management for SYS tables disabled, relying on external index management");
            state.set(State.NOT_MANAGED);
            return;
        }

        //get primary key constraint
        //-sf- Hbase scan
        ConstraintDescriptorList constraintDescriptors = dataDictionary.getConstraintDescriptors(td);
        for(int i=0;i<constraintDescriptors.size();i++){
            ConstraintDescriptor cDescriptor = constraintDescriptors.elementAt(i);
            org.apache.derby.catalog.UUID conglomerateId = cDescriptor.getConglomerateId();
            if(conglomerateId != null && td.getConglomerateDescriptor(conglomerateId).getConglomerateNumber()!=congomId)
                continue;

            switch(cDescriptor.getConstraintType()){
                case DataDictionary.PRIMARYKEY_CONSTRAINT:
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
                    indexFactories.clear();
                    break;
                } else {
                    indexFactories.add(buildIndex(conglomDesc,constraintDescriptors));
                }
            }
        }

        //check ourself for any additional constraints, but only if it's not present already
        if(!congloms.contains(cd)&&cd.isIndex()){
            //if we have a constraint, use it
            addIndexConstraint(td,cd);
            indexFactories.clear();
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

    private IndexFactory buildIndex(ConglomerateDescriptor conglomDesc,ConstraintDescriptorList constraints) {
        IndexRowGenerator irg = conglomDesc.getIndexDescriptor();
        IndexDescriptor indexDescriptor = irg.getIndexDescriptor();
        if(indexDescriptor.isUnique())
            return IndexFactory.create(conglomDesc.getConglomerateNumber(), indexDescriptor);

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
                return IndexFactory.create(conglomDesc.getConglomerateNumber(), indexDescriptor, true);
            }
        }

        return IndexFactory.create(conglomDesc.getConglomerateNumber(), indexDescriptor);
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
    }

    private static class IndexFactory{
        private final long indexConglomId;
        private final byte[] indexConglomBytes;
        private final boolean isUnique;
        private BitSet indexedColumns;
        private int[] mainColToIndexPosMap;
        private BitSet descColumns;

        private IndexFactory(long indexConglomId){
            this.indexConglomId = indexConglomId;
            this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
            this.isUnique=false;
        }

        private IndexFactory(long indexConglomId,BitSet indexedColumns,int[] mainColToIndexPosMap,boolean isUnique,BitSet descColumns) {
            this.indexConglomId = indexConglomId;
            this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
            this.isUnique=isUnique;
            this.indexedColumns=indexedColumns;
            this.mainColToIndexPosMap=mainColToIndexPosMap;
            this.descColumns = descColumns;
        }

        public static IndexFactory create(long conglomerateNumber,int[] indexColsToMainColMap,boolean isUnique,BitSet descColumns){
            BitSet indexedCols = new BitSet();
            for(int indexCol:indexColsToMainColMap){
                indexedCols.set(indexCol-1);
            }
            int[] mainColToIndexPosMap = new int[(int)indexedCols.length()];
            for(int indexCol=0;indexCol<indexColsToMainColMap.length;indexCol++){
                int mainCol = indexColsToMainColMap[indexCol];
                mainColToIndexPosMap[mainCol-1] = indexCol;
            }

            return new IndexFactory(conglomerateNumber,indexedCols,mainColToIndexPosMap,isUnique, descColumns);
        }

        public static IndexFactory create(long conglomerateNumber, IndexDescriptor indexDescriptor) {
            return create(conglomerateNumber, indexDescriptor,indexDescriptor.isUnique());
        }

        public static IndexFactory create(long conglomerateNumber, IndexDescriptor indexDescriptor,boolean isUnique) {
            int[] indexColsToMainColMap = indexDescriptor.baseColumnPositions();

            //get the descending columns
            boolean[] ascending = indexDescriptor.isAscending();
            BitSet descColumns = new BitSet();
            for(int i=0;i<ascending.length;i++){
                if(!ascending[i])
                    descColumns.set(i);
            }
            return create(conglomerateNumber,indexColsToMainColMap,isUnique,descColumns);
        }

        public void addTo(PipelineWriteContext ctx,boolean keepState,int expectedWrites){

            ctx.addLast(new IndexDeleteWriteHandler(indexedColumns,mainColToIndexPosMap,indexConglomBytes,descColumns,keepState,isUnique,expectedWrites));
            if(isUnique){
                ctx.addLast(new UniqueIndexUpsertWriteHandler(indexedColumns,mainColToIndexPosMap,indexConglomBytes,descColumns,keepState,expectedWrites));
            }else{
                ctx.addLast(new IndexUpsertWriteHandler(indexedColumns,mainColToIndexPosMap,indexConglomBytes,descColumns,keepState,false,expectedWrites));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IndexFactory)) return false;

            IndexFactory that = (IndexFactory) o;

            return indexConglomId == that.indexConglomId;
        }

        @Override
        public int hashCode() {
            return (int) (indexConglomId ^ (indexConglomId >>> 32));
        }

        public static IndexFactory wrap(long indexConglomId) {
            return new IndexFactory(indexConglomId);
        }
    }
}
