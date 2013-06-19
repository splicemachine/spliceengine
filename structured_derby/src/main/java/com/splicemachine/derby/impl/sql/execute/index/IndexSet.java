package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraints;
import com.splicemachine.derby.impl.sql.execute.constraint.ForeignKey;
import com.splicemachine.derby.impl.sql.execute.constraint.PrimaryKey;
import com.splicemachine.derby.impl.sql.execute.constraint.UniqueConstraint;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents all Indices and Constraints on a Table.
 *
 * While this instance <em>can</em> be created on its own, it is recommended that
 * instances be obtained through an IndexSetPool implementation.
 *
 * An IndexSet can be in one of several states(denoted by the State enum):
 *
 * WAITING_TO_START: all Mutations will issue a warning that IndexManagement hasn't yet started,
 * but will be allowed through. This is required to prevent deadlocks during Splice startup due to
 * derby's need to update indexed tables before the data dictionary can be fully loaded, but
 * we have to have a fully loaded data dictionary to populate index information.
 *
 * READY_TO_START: We have successfully loaded a data dictionary, and can load up index information at any
 * time. In practice, the most likely time is on the first mutation call to the table. Mutations which
 * encounter the READY_TO_START state will initialize the index set before proceeding.
 *
 * STARTING: The IndexSet is in the process of initializing state. Mutations which encounter the STARTING
 * state will block until the IndexSet has completed initialization
 *
 * RUNNING: Index Management is online, Mutations which encounter this state will update indices in a non-blocking
 * fashion
 *
 * SETUP_FAILED: Setting up Derby failed in some way. Mutations encountering this state will throw a
 * DoNotRetryIOException indicating that index management can't be performed until the problem is fixed.
 *
 * SHUTDOWN: Index Management has been terminated. Mutations will fail as they can't manage indices.
 *
 * NOT_MANAGED: Index management is disabled for this table. This is so that things which are managed through,
 * e.g. Derby, are not also indexed through an IndexSet. Typically, this applies to System tables, which
 * don't need external management and might break if they are.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class IndexSet {
    private static final long STARTUP_LOCK_BACKOFF_PERIOD = 100;
    private static final Logger LOG = Logger.getLogger(IndexSet.class);

    /*
     * Special tags to denote that a mutation was updated externally to the set,
     * and that it does not need index management internally. This is because it's possible
     * for indices to be managed externally, and thus we don't want to manage it here for
     * performance reasons.
     *
     * To bypass index updates, attach INDEX_UPDATED as an attribute key on a Mutation.
     */
    public static final String INDEX_UPDATED = SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;
    //it doesn't need to have any content, because we only key off of the Attribute key
    public static final byte[] INDEX_ALREADY_UPDATED = SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE;

    /*
     * Synchronizer object for use during initialization.
     */
    private final Lock initializationLock = new ReentrantLock(true);

    /*
     * The conglomerate id of the main table that we are indexing.
     */
    private final long conglomId;

    //The local constraint to apply (if any).
    private volatile Constraint localConstraint = Constraints.noConstraint();

    //any Foreign Key Constraints to apply.
    private volatile List<Constraint> fkConstraints = Collections.emptyList();

    //indices to manage
    private volatile Set<IndexManager> indices = Collections.emptySet();
    //the state of the IndexSet
    private AtomicReference<State> state = new AtomicReference<State>(State.WAITING_TO_START);

    //don't build me through this--this is to allow one to construct wrapper IndexSets for dropping tables etc.
    private IndexSet(State initialState){
        this.state.set(initialState);
        this.conglomId = -1l;
    }

    private IndexSet(long conglomId){
        this.conglomId = conglomId;
    }

    /**
     * Construct an IndexSet which does not manage indices. Useful for dropping indices and other
     * management stuff that doesn't need to maintain indices.
     *
     * @return an IndexSet permanently set to the NOT_MANAGED state.
     */
    public static IndexSet noIndex(){
        return new IndexSet(State.NOT_MANAGED);
    }

    /**
     * Construct an IndexSet for the specified main table.
     *
     * @param conglomId the conglomerate id for the main table
     * @return an IndexSet for the specified table.
     */
    public static IndexSet create(long conglomId) {
        return new IndexSet(conglomId);
    }

    /**
     * Inform the Index Set that it is safe to start index management at any time.
     */
    public void prepare() {
        state.compareAndSet(State.WAITING_TO_START,State.READY_TO_START);
    }

    /**
     * @return the conglomerate id of the main table
     */
    public long getMainTableConglomerateId(){
        return conglomId;
    }

    /**
     * Validate and update a single mutation.
     *
     * @param mutation the mutation to validate/manage
     * @param rce the RegionCoprocessorEnvironment of the region
     * @throws IOException if something goes wrong (Constraint violation, index issue, etc.)
     */
    public void update(Mutation mutation,RegionCoprocessorEnvironment rce) throws IOException{
        try {
            checkState();
        } catch (InterruptedException e) {
            //we were interrupted while either waiting for a lock or attempting to get a connection
            //bomb out this write in a retryable fashion so that we can prevent the interruption
            throw new IndexNotSetUpException();
        }
        //If the Index was updated already, then bypass this, since we don't need to check it
        if(mutation.getAttribute(INDEX_UPDATED)!=null) return;

        validate(mutation,rce);

        //update indices
        for(IndexManager index:indices){
            index.update(mutation, rce);
        }
    }

    /**
     * Update a block of Mutations in bulk.
     *
     * @param mutations the mutations to validate/manage
     * @param rce the region environment.
     * @throws IOException if something goes wrong with <em>any</em> of the mutations.
     */
    public void update(Collection<Mutation> mutations,
                       RegionCoprocessorEnvironment rce) throws IOException{
        try {
            checkState();
        } catch (InterruptedException e) {
            throw new IndexNotSetUpException();
        }

        validate(mutations,rce);

        //update indices
        for(IndexManager index:indices){
            index.update(mutations, rce);
        }
    }

    private void checkState() throws IOException, InterruptedException {
        switch (state.get()) {
            case WAITING_TO_START:
                SpliceLogUtils.trace(LOG, "Index management for conglomerate %d " +
                        "has not completed, indices may not be correctly updated, " +
                        "and constraints may not be correct!", conglomId);
                break;
            case READY_TO_START:
                SpliceLogUtils.trace(LOG, "Index management for conglomerate %d " +
                        "has not completed, indices may not be correctly updated, " +
                        "and constraints may not be correct!", conglomId);
                start();
                break;
            case STARTING:
                SpliceLogUtils.debug(LOG,"Index management is starting up");
                start();
                break;
            case RUNNING:
                break;
            case FAILED_SETUP:
                throw ConstraintViolation.failedSetup(Long.toString(conglomId));
            case SHUTDOWN:
                throw ConstraintViolation.shutdown(Long.toString(conglomId));
            case NOT_MANAGED:
                //we don't actively manage indices and constraints here--probably a sys table
                //or some other construct that has to be managed separately
                break;
        }
    }

    /**
     * Add an index to this table.
     *
     * @param index the index to add
     */
    public void addIndex(IndexManager index){
        if(state.get()!=State.RUNNING) return;
        indices.add(index);
    }

    /**
     * Drop an index from this table.
     *
     * @param index the index to drop
     */
    public void dropIndex(IndexManager index){
        if(state.get()!=State.RUNNING) return;
        indices.remove(index);
    }

    public void validate(Mutation mutation, RegionCoprocessorEnvironment environment) throws IOException {
        if(mutation.getAttribute(INDEX_UPDATED)!=null) return;
        //validate local constraints
        if(localConstraint!=null&&!localConstraint.validate(mutation,environment))
            throw ConstraintViolation.create(localConstraint.getType());

        //validate fkConstraints
        for(Constraint fkConstraint:fkConstraints){
            if(!fkConstraint.validate(mutation,environment))
                throw ConstraintViolation.create(fkConstraint.getType());
        }
    }

    public void validate(Collection<Mutation> mutations, RegionCoprocessorEnvironment environment) throws IOException {

        //validate local constraints
        if(localConstraint!=null&&!localConstraint.validate(mutations,environment))
            throw ConstraintViolation.create(localConstraint.getType());

        //validate fkConstraints
        for(Constraint fkConstraint:fkConstraints){
            if(!fkConstraint.validate(mutations,environment))
                throw ConstraintViolation.create(fkConstraint.getType());
        }
    }

    /*********************************************************************************************************************/
    /*private helper methods*/

    /*
     * initialize the table management.
     */
    private void start() throws IOException, InterruptedException {
        /*
         * The following code exercises a bit of fancy Double-checked locking, which goes as follows:
         *
         * First, attempt to change the state from READY_TO_START to STARTING atomically. If this succeeds,
         * then this thread is the first to attempt setting up, so it can proceed normally. Otherwise, either
         * someone else is already setting things up, or there is a bad state. If it's a bad state, return,
         * since this method can't do anything about bad state. If someone else is already starting up,
         * then enter into the synchronized block and wait for the initializing thread to complete. Once
         * the initializing thread is complete and you can enter into the synchronized block, check the
         * state again to ensure that it's not in some state that doesn't require startup. If it isn't, then
         * return. Otherwise, go ahead and initialize.
         */
        if(!state.compareAndSet(State.READY_TO_START,State.STARTING)){

            if(state.get()!=State.STARTING) return;
        }

        //someone else may have initialized this. If so, we don't need to repeat it, so return
        if(state.get()!=State.STARTING) return;

        SpliceLogUtils.debug(LOG,"Setting up index for conglomerate "+ conglomId);

        Connection connection = null;
            /*
             * RegionServers only have a limited number of IPC listeners to respond to puts, so we
             * want to make sure that they don't all end up blocking here lest we run out of threads. If
             * that happens, then likely we won't be able to leave this synchronization barrier, so we're in
             * a nice distributed deadlock, which is bad
             *
             * To avoid this, we have a backoff period on the synchronization barrier. If you can't
             * get through the barrier after a reasonably brief time period, then we just blow up on this
             * write and rely on the calling client to retry, which will (hopefully) generate enough
             * of a delay for the start up thread to complete its metadata scanning and finish
             * setting up for us
             */
        if(!initializationLock.tryLock(STARTUP_LOCK_BACKOFF_PERIOD, TimeUnit.MILLISECONDS)){
            throw new IndexNotSetUpException("Unable to initialize index management for table "+ conglomId+" within a sufficient period." +
                    " Please wait a bit and try again");
        }
        TransactorControl transactor = null;
        TransactionId txnID = null;
        SpliceTransactionResourceImpl impl = null;
        try {
            try{
                impl = new SpliceTransactionResourceImpl();
                transactor = HTransactorFactory.getTransactorControl(); // TODO Place Holder - Transaction Must Flow...
                txnID = transactor.beginTransaction(false, true, false);
                impl.marshallTransaction(txnID.getTransactionIdString());
                DataDictionary dataDictionary = impl.getLcc().getDataDictionary();
                ConglomerateDescriptor conglomerateDescriptor = dataDictionary.getConglomerateDescriptor(conglomId);
                dataDictionary.getExecutionFactory().newExecutionContext(ContextService.getFactory().getCurrentContextManager());
                TableDescriptor td = dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());
                    /*
                     * That's weird, there's no Table in the dictionary? Probably not good, but nothing we
                     * can do about it, so just bail.
                     */
                if(td!=null) {
                    startDirect(dataDictionary, td);
                }
                transactor.commit(txnID);
            }catch(StandardException se){
                SpliceLogUtils.error(LOG,"Unable to set up index management for table "+ conglomId+", aborting",se);
                state.set(State.FAILED_SETUP);
                return;
            } catch (IOException e) {
                SpliceLogUtils.error(LOG,"Unable to set up index management for table "+ conglomId+", aborting",e);
                state.set(State.FAILED_SETUP);
                return;
            } catch (SQLException e) {
                SpliceLogUtils.error(LOG,"Unable to acquire a Database Connection, " +
                        "aborting write, but backing off so that other writes can try again",e);
                state.set(State.READY_TO_START);
                throw new IndexNotSetUpException(e);
            } finally{
                //now unlock so that other threads can get access
                initializationLock.unlock();
                if (transactor != null && txnID != null)
                    transactor.rollback(txnID);
            }
        } finally {
            if (impl != null) {
                impl.cleanup();
            }
        }
    }

    private void startDirect(DataDictionary dataDictionary, TableDescriptor td) throws StandardException, IOException {
        boolean isSysConglomerate = td.getSchemaDescriptor().getSchemaName().equals("SYS");
        if(isSysConglomerate){
                /*
                 * The DataDictionary and Derby metadata code management will actually deal with
                 * constraints internally, so we don't have anything to do
                 */
            SpliceLogUtils.trace(LOG, "Index management for Sys table disabled, relying on external" +
                    "index management");
            state.set(State.NOT_MANAGED);
            return;
        }

        //get primary key constraint
        ConstraintDescriptorList constraintDescriptors = dataDictionary.getConstraintDescriptors(td);
        for(int i=0;i<constraintDescriptors.size();i++){
            ConstraintDescriptor cDescriptor = constraintDescriptors.elementAt(i);
            if(cDescriptor.getConstraintType()==DataDictionary.PRIMARYKEY_CONSTRAINT){
                localConstraint = buildPrimaryKey(cDescriptor);
            }else{
                LOG.warn("Unknown Constraint on table "+ conglomId+": type = "+ cDescriptor.getConstraintText());
            }
        }

        //get Constraints list
        ConglomerateDescriptorList congloms = td.getConglomerateDescriptorList();
        List<Constraint> foreignKeys = Lists.newArrayListWithExpectedSize(congloms.size());
        List<Constraint> checkConstraints = Lists.newArrayListWithExpectedSize(congloms.size());

        List<IndexManager> attachedIndices = Lists.newArrayListWithExpectedSize(congloms.size());
        for(int i=0;i<congloms.size();i++){
            ConglomerateDescriptor conglomDesc = (ConglomerateDescriptor) congloms.get(i);
            if(conglomDesc.isIndex()){
                if(conglomDesc.getConglomerateNumber()==conglomId){
                    //we are an index, so just map a constraint, rather than an IndexManager
                    localConstraint = buildIndexConstraint(conglomDesc);
                    attachedIndices.clear(); //there are no attached indices on the index htable itself
                    foreignKeys.clear(); //there are no foreign keys to deal with on the index htable itself
                    break;
                }else
                    attachedIndices.add(buildIndex(conglomDesc));
            }
        }

                /*
                 * Make sure that fkConstraints is thread safe so that we can drop the constraint whenever
                 * someone asks us to.
                 */
        fkConstraints = new CopyOnWriteArrayList<Constraint>(foreignKeys);
        indices = new CopyOnWriteArraySet<IndexManager>(attachedIndices);

        SpliceLogUtils.debug(LOG,"Index setup complete for table "+conglomId+", ready to run");
        state.set(State.RUNNING);
    }

    public void shutdown() throws IOException{
        state.set(State.SHUTDOWN);
        //do other stuff to clean up
    }

    /*
     * Build an index from a Conglomerate Descriptor
     */
    private IndexManager buildIndex(ConglomerateDescriptor conglomDesc) throws IOException {
        IndexRowGenerator irg = conglomDesc.getIndexDescriptor();
        IndexDescriptor indexDescriptor = irg.getIndexDescriptor();
        return IndexManager.create(conglomDesc.getConglomerateNumber(),indexDescriptor);
    }

    /*
     * Build a Foreign Key constraint
     */
    private ForeignKey buildForeignKey(ForeignKeyConstraintDescriptor fkcd) throws StandardException {
        int[] fkCols = fkcd.getReferencedColumns();
        BitSet fkColBits = new BitSet();
        for(int fkCol:fkCols){
            fkColBits.set(fkCol);
        }

        ReferencedKeyConstraintDescriptor rkcd = fkcd.getReferencedConstraint();
        long refTableId = rkcd.getTableDescriptor().getHeapConglomerateId();

        return new ForeignKey(Long.toString(refTableId),Long.toString(conglomId),fkColBits);
    }

    /*
     * Build a primary Key constraint
     */
    private PrimaryKey buildPrimaryKey(ConstraintDescriptor columnDescriptor) throws StandardException{
        return new PrimaryKey();
    }

    /*
     * Build an index constraint (Unique, Non-null, etc)
     */
    private Constraint buildIndexConstraint(ConglomerateDescriptor conglomerateDescriptor) throws StandardException{
        IndexDescriptor indexDescriptor = conglomerateDescriptor.getIndexDescriptor().getIndexDescriptor();
        if(indexDescriptor.isUnique()) return UniqueConstraint.create();
        //TODO -sf- get other types of indexing constraints, like NOT NULL etc. here
        return Constraints.noConstraint();
    }

    /*
     * Build a Check constraint
     */
    private Constraint buildCheckConstraint(ConstraintDescriptor descriptor) throws StandardException{
        //todo -sf- implement!
        return Constraints.noConstraint();
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

}
