package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.constraint.*;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents all Indices and Constraints on a Table.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class IndexSet {
    private static final Logger LOG = Logger.getLogger(IndexSet.class);

    @SuppressWarnings("UnnecessaryBoxing")
    private final Object initializer = new Integer(1); //lock object for synchronous initialization

    private final long conglomId;

    public void prepare() {
        state.compareAndSet(State.WAITING_TO_START,State.READY_TO_START);
    }

    private enum State{
        WAITING_TO_START,
        READY_TO_START,
        STARTING,
        RUNNING,
        FAILED_SETUP,
        SHUTDOWN,
        NOT_MANAGED
    }
    private volatile Constraint localConstraint = Constraints.noConstraint();
    private volatile List<Constraint> fkConstraints = Collections.emptyList();
    private volatile Set<IndexManager> indices = Collections.emptySet();
    private AtomicReference<State> state = new AtomicReference<State>(State.WAITING_TO_START);

    private IndexSet(State initialState){
        this.state.set(initialState);
        this.conglomId = -1l;
    }

    private IndexSet(long conglomId){
        this.conglomId = conglomId;
    }

    public static IndexSet noIndex(){
        return new IndexSet(State.NOT_MANAGED);
    }

    public static IndexSet create(long conglomId) {
        return new IndexSet(conglomId);
    }

    public long getMainTableConglomerateId(){
        return conglomId;
    }

    public void update(Mutation mutation,RegionCoprocessorEnvironment rce) throws IOException{
        checkState();

        //check local constraints
        if(!localConstraint.validate(mutation,rce))
            throw ConstraintViolation.create(localConstraint.getType());

        //check foreign key constraints
        for(Constraint fkConstraint: fkConstraints){
            if(!fkConstraint.validate(mutation,rce))
                throw ConstraintViolation.create(fkConstraint.getType());
        }

        //update indices
        for(IndexManager index:indices){
            index.update(mutation, rce);
        }
    }

    public void update(Collection<Mutation> mutations,
                              RegionCoprocessorEnvironment rce) throws IOException{
        checkState();

        //validate local constraints
        if(localConstraint!=null&&!localConstraint.validate(mutations,rce))
            throw ConstraintViolation.create(localConstraint.getType());

        //validate fkConstraints
        for(Constraint fkConstraint:fkConstraints){
            if(!fkConstraint.validate(mutations,rce))
                throw ConstraintViolation.create(fkConstraint.getType());
        }

        //update indices
        for(IndexManager index:indices){
            index.update(mutations, rce);
        }
    }

    private void checkState() throws ConstraintViolation {
        switch (state.get()) {
            case WAITING_TO_START:
                SpliceLogUtils.warn(LOG, "Index management for conglomerate %d " +
                        "has not completed, indices may not be correctly updated, " +
                        "and constraints may not be correct!", conglomId);
                break;
            case READY_TO_START:
                SpliceLogUtils.warn(LOG, "Index management for conglomerate %d " +
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

    public void addIndex(IndexManager index){
        if(state.get()!=State.RUNNING) return;
        indices.add(index);
    }

    public void dropIndex(IndexManager index){
        if(state.get()!=State.RUNNING) return;
        indices.remove(index);
    }

    public void shutdown() throws IOException{
        state.set(State.SHUTDOWN);
        //do other stuff to clean up
    }

    public void start(){
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

        synchronized (initializer){
            //someone else may have initialized this. If so, we don't need to repeat it, so return
            if(state.get()!=State.STARTING) return;

            SpliceLogUtils.debug(LOG,"Setting up index for conglomerate "+ conglomId);

            try{
                SpliceUtils.setThreadContext();
                DataDictionary dataDictionary = SpliceDriver.driver().getLanguageConnectionContext().getDataDictionary();
                ConglomerateDescriptor conglomerateDescriptor = dataDictionary.getConglomerateDescriptor(conglomId);

                dataDictionary.getExecutionFactory().newExecutionContext(ContextService.getFactory().getCurrentContextManager());
                TableDescriptor td = dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());
	            /*
	             * That's weird, there's no Table in the dictionary? Probably not good, but nothing we
	             * can do about it, so just bail.
	             */
                if(td==null) return;
                boolean isSysConglomerate = td.getSchemaDescriptor().getSchemaName().equals("SYS");
                if(isSysConglomerate){
                    /*
                     * The DataDictionary and Derby metadata code management will actually deal with
                     * constraints internally, so we don't have anything to do
                     */
                    SpliceLogUtils.trace(LOG,"Index management for Sys table disabled, relying on external" +
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


            }catch(StandardException se){
                SpliceLogUtils.error(LOG,"Unable to set up index management for table "+ conglomId+", aborting",se);
                state.set(State.FAILED_SETUP);
                return;
            } catch (IOException e) {
                SpliceLogUtils.error(LOG,"Unable to set up index management for table "+ conglomId+", aborting",e);
                state.set(State.FAILED_SETUP);
                return;
            }
            SpliceLogUtils.debug(LOG,"Index setup complete for table "+conglomId+", ready to run");
            state.set(State.RUNNING);
        }
    }

    private IndexManager buildIndex(ConglomerateDescriptor conglomDesc) throws IOException {
        IndexRowGenerator irg = conglomDesc.getIndexDescriptor();
        IndexDescriptor indexDescriptor = irg.getIndexDescriptor();
        return IndexManager.create(conglomDesc.getConglomerateNumber(),indexDescriptor);
    }

    private ForeignKey buildForeignKey(ForeignKeyConstraintDescriptor fkcd) throws StandardException {
        int[] fkCols = fkcd.getReferencedColumns();
        BitSet fkColBits = new BitSet();
        for(int fkCol:fkCols){
            fkColBits.set(fkCol);
        }

        ReferencedKeyConstraintDescriptor rkcd = fkcd.getReferencedConstraint();
        long refTableId = rkcd.getTableDescriptor().getHeapConglomerateId();

        return new ForeignKey(Long.toString(refTableId),Long.toString(conglomId),fkColBits,null);
    }

    private PrimaryKey buildPrimaryKey(ConstraintDescriptor columnDescriptor) throws StandardException{
        return new PrimaryKey();
    }

    private Constraint buildIndexConstraint(ConglomerateDescriptor conglomerateDescriptor) throws StandardException{
        IndexDescriptor indexDescriptor = conglomerateDescriptor.getIndexDescriptor().getIndexDescriptor();
        if(indexDescriptor.isUnique()) return UniqueConstraint.create();
        //TODO -sf- get other types of indexing constraints, like NOT NULL etc. here
        return Constraints.noConstraint();
    }

    private Constraint buildCheckConstraint(ConstraintDescriptor descriptor) throws StandardException{
        //todo -sf- implement!
        return Constraints.noConstraint();
    }

}
