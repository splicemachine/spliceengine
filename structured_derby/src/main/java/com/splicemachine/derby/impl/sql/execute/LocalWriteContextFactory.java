package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.constraint.*;
import com.splicemachine.derby.impl.sql.execute.index.*;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.hbase.batch.*;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.TransactorFactory;
import com.splicemachine.tools.ResettableCountDownLatch;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
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
public class LocalWriteContextFactory implements WriteContextFactory<RegionCoprocessorEnvironment> {
    private static final Logger LOG = Logger.getLogger(LocalWriteContextFactory.class);

    private static final long STARTUP_LOCK_BACKOFF_PERIOD = 200;

    private final long congomId;
    private final Set<IndexFactory> indexFactories = new CopyOnWriteArraySet<IndexFactory>();
    private final List<ConstraintFactory> constraintFactories = new CopyOnWriteArrayList<ConstraintFactory>();

    private final ReentrantLock initializationLock = new ReentrantLock();
    /*
     * Latch for blocking writes during the updating of table metadata (adding constraints, indices,
     * etc).
     */
    private final ResettableCountDownLatch tableWriteLatch = new ResettableCountDownLatch(1);

    protected final AtomicReference<State> state = new AtomicReference<State>(State.WAITING_TO_START);

    public void prepare(){
        state.compareAndSet(State.WAITING_TO_START,State.READY_TO_START);
    }

    public LocalWriteContextFactory(long congomId) {
        this.congomId = congomId;
        tableWriteLatch.countDown();
    }

    @Override
    public WriteContext create(RegionCoprocessorEnvironment rce) throws IOException, InterruptedException {
        PipelineWriteContext context = new PipelineWriteContext(rce);
        switch (state.get()) {
            case READY_TO_START:
                SpliceLogUtils.trace(LOG,"Index management for conglomerate %d " +
                        "has not completed, attempting to start now",congomId);
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
        if(state.get()==State.RUNNING){
            //add Constraint checks before anything else
            for(ConstraintFactory constraintFactory:constraintFactories){
                context.addLast(constraintFactory.create());
            }

            //add index handlers
            for(IndexFactory indexFactory:indexFactories){
                indexFactory.addTo(context);
            }
        }

        //add a region handler
        context.addLast(new RegionWriteHandler(rce.getRegion(),tableWriteLatch));
        return context;
    }

    @Override
    public void dropIndex(long indexConglomId) {
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
    public void addIndex(long indexConglomId, int[] indexColsToBaseColMap, boolean unique) {
        synchronized (tableWriteLatch){
            tableWriteLatch.reset();
            try{
                indexFactories.add(new IndexFactory(indexConglomId,indexColsToBaseColMap,unique));
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

    public long getMainTableConglomerateId() {
        return congomId;
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
        if(!state.compareAndSet(State.READY_TO_START,State.STARTING)){
            if(state.get()!=State.STARTING) return;
        }

        //someone else may have initialized this if so, we don't need to repeat, so return
        if(state.get()!=State.STARTING) return;

        SpliceLogUtils.debug(LOG,"Setting up index for conglomerate "+congomId);

        if(!initializationLock.tryLock(STARTUP_LOCK_BACKOFF_PERIOD, TimeUnit.MILLISECONDS)){
            throw new IndexNotSetUpException("Unable to initialize index management for table "+ congomId
                    +" within a sufficient time frame. Please wait a bit and try again");
        }
        Transactor transactor = null;
        TransactionId txnId = null;
        boolean success = false;
        try{
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            transactor = TransactorFactory.getDefaultTransactor();
            txnId = transactor.beginTransaction(false,true,false);
            transactionResource.marshallTransaction(txnId.getTransactionIdString());

            DataDictionary dataDictionary= transactionResource.getLcc().getDataDictionary();
            ConglomerateDescriptor conglomerateDescriptor = dataDictionary.getConglomerateDescriptor(congomId);

            dataDictionary.getExecutionFactory().newExecutionContext(ContextService.getFactory().getCurrentContextManager());
            TableDescriptor td = dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());

            if(td!=null){
                startDirect(dataDictionary,td);
            }
            transactor.commit(txnId);
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

            if(!success&&(transactor!=null && txnId !=null))
                transactor.rollback(txnId);
        }
    }

    private void startDirect(DataDictionary dataDictionary,TableDescriptor td) throws StandardException,IOException{
        boolean isSysConglomerate = td.getSchemaDescriptor().getSchemaName().equals("SYS");
        if(isSysConglomerate){
            SpliceLogUtils.trace(LOG,"Index management for SYS tables disabled, relying on external index management");
            state.set(State.NOT_MANAGED);
            return;
        }

        //get primary key constraint
        ConstraintDescriptorList constraintDescriptors = dataDictionary.getConstraintDescriptors(td);
        for(int i=0;i<constraintDescriptors.size();i++){
            ConstraintDescriptor cDescriptor = constraintDescriptors.elementAt(i);
            if(cDescriptor.getConstraintType()==DataDictionary.PRIMARYKEY_CONSTRAINT){
                constraintFactories.add(buildPrimaryKey(cDescriptor));
            }else{
                LOG.warn("Unknown Constraint on table "+ congomId+": type = "+ cDescriptor.getConstraintText());
            }
        }

        //get Constraints list
        ConglomerateDescriptorList congloms = td.getConglomerateDescriptorList();
        for(int i=0;i<congloms.size();i++){
            ConglomerateDescriptor conglomDesc = (ConglomerateDescriptor)congloms.get(i);
            if(conglomDesc.isIndex()){
                if(conglomDesc.getConglomerateNumber()==congomId){
                    //we are an index, so just map a constraint rather than an attached index
                    addIndexConstraint(conglomDesc);
                    indexFactories.clear();
                    break;
                }else{
                    indexFactories.add(buildIndex(conglomDesc));
                }
            }
        }
    }

    private void addIndexConstraint(ConglomerateDescriptor conglomDesc) {
        IndexDescriptor indexDescriptor = conglomDesc.getIndexDescriptor().getIndexDescriptor();
        if(indexDescriptor.isUnique())
            constraintFactories.add(new ConstraintFactory(UniqueConstraint.create()));
    }

    private IndexFactory buildIndex(ConglomerateDescriptor conglomDesc) {
        IndexRowGenerator irg = conglomDesc.getIndexDescriptor();
        IndexDescriptor indexDescriptor = irg.getIndexDescriptor();
        return new IndexFactory(conglomDesc.getConglomerateNumber(),indexDescriptor);
    }

    private ConstraintFactory buildPrimaryKey(ConstraintDescriptor cDescriptor) {
        return new ConstraintFactory(new PrimaryKey());
    }

    private static class ConstraintFactory{
        private final Constraint localConstraint;

        private ConstraintFactory(Constraint localConstraint) {
            this.localConstraint = localConstraint;
        }

        public WriteHandler create(){
            return new ConstraintHandler(localConstraint);
        }
    }

    private static class IndexFactory{
        private final long indexConglomId;
        private final byte[] indexConglomBytes;
        private final byte[][] mainColPos;
        private final int[] indexColsToMainColMap;
        private final boolean isUnique;

        private IndexFactory(long indexConglomId){
            this.indexConglomId = indexConglomId;
            this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
            this.indexColsToMainColMap=null;
            this.isUnique=false;
            this.mainColPos=null;
        }

        private IndexFactory(long indexConglomId,int[] baseIndexedColumns,boolean isUnique) {
            this.indexConglomId = indexConglomId;
            this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
            this.indexColsToMainColMap = translate(baseIndexedColumns);
            this.isUnique = isUnique;

            mainColPos = new byte[baseIndexedColumns.length][];
            for(int i=0;i<baseIndexedColumns.length;i++){
                mainColPos[i] = Integer.toString(baseIndexedColumns[i]-1).getBytes();
            }
        }

        public IndexFactory(long conglomerateNumber, IndexDescriptor indexDescriptor) {
            this(conglomerateNumber,indexDescriptor.baseColumnPositions(),indexDescriptor.isUnique());
        }

        /*
         * convert a one-based int[] into a zero-based. In essence, shift all values in the array down by one
         */
        private static int[] translate(int[] ints) {
            int[] zeroBased = new int[ints.length];
            for(int pos=0;pos<ints.length;pos++){
                zeroBased[pos] = ints[pos]-1;
            }
            return zeroBased;
        }

        public void addTo(PipelineWriteContext ctx){

            if(isUnique){
                ctx.addLast(new UniqueIndexDeleteWriteHandler(indexColsToMainColMap,mainColPos,indexConglomBytes));
                ctx.addLast(new UniqueIndexUpsertWriteHandler(indexColsToMainColMap,mainColPos,indexConglomBytes));
            }else{
                ctx.addLast(new IndexDeleteWriteHandler(indexColsToMainColMap,mainColPos,indexConglomBytes));
                ctx.addLast(new IndexUpsertWriteHandler(indexColsToMainColMap,mainColPos,indexConglomBytes));
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
