package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.impl.sql.execute.constraint.*;
import com.splicemachine.derby.impl.sql.execute.index.CoprocessorEnvironmentTableSource;
import com.splicemachine.derby.impl.sql.execute.index.IndexManager;
import com.splicemachine.derby.impl.sql.execute.index.PrimaryKey;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class SpliceIndexObserver extends BaseRegionObserver {
    private static final Map<Long,SpliceIndexObserver> regionReferences = Maps.newConcurrentMap();
    private final Object initalizer = new Integer(2);

    private enum State{
        WAITING_TO_START,
        READY_TO_START,
        STARTING,
        RUNNING,
        FAILED_SETUP,
        SHUTDOWN
    }

    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);
    private volatile long conglomId = -1l;
    private volatile Constraint localConstraint;
    private volatile List<Constraint> fkConstraints = Collections.emptyList();
    private Set<IndexManager> indices = Collections.emptySet();
    private AtomicReference<State> state = new AtomicReference<State>(State.WAITING_TO_START);

    //todo -sf- replace this with a pool pattern in some way
    private CoprocessorEnvironmentTableSource tableSource = new CoprocessorEnvironmentTableSource();

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        //get the Conglomerate Id. If it's not a table that we can index (e.g. META, ROOT, SYS_TEMP,__TXN_LOG, etc)
        //then don't bother with setting things up
        String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
        try{
            conglomId = Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.debug(LOG, "Unable to parse Conglomerate Id for table %s, indexing is will not be set up", tableName);
            state.set(State.RUNNING);
            return;
        }
        regionReferences.put(conglomId,this);

        SpliceDriver.Service service = new SpliceDriver.Service() {
            @Override
            public boolean start() {
                state.set(State.READY_TO_START);
                //now that we know we can start, we don't care what else happens in the lifecycle
                SpliceDriver.driver().deregisterService(this);
                return true;
            }

            @Override
            public boolean shutdown() {
                //we don't care
                return true;
            }
        };

        //register for notifications--allow the registration to tell us if we can go ahead or not
        SpliceDriver.driver().registerService(service);

        super.postOpen(e);
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        SpliceDriver.driver().shutdown();
        super.stop(e);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
        /*
         * This is where we do a lot of things to validate correctness. Specifically, we
         *
         * 1. check PrimaryKey Constraints (if any)
         * 2. check ForeignKey Constraints( if any)
         * 3. check Index Constraints (if any)
         * 4. Update indices to keep in sync
         */
        switch (state.get()) {
            case WAITING_TO_START:
                SpliceLogUtils.warn(LOG,"Index management for table %s has not completed startup yet, indices may not be" +
                        "updated!",e.getEnvironment().getRegion().getTableDesc().getNameAsString());
                break;
            case STARTING:
                SpliceLogUtils.debug(LOG, "Index management is currently starting up,this put may block while startup completes!");
                break;
            case FAILED_SETUP:
                throw ConstraintViolation.failedSetup(e.getEnvironment().getRegion().getTableDesc().getNameAsString());
            case SHUTDOWN:
                throw ConstraintViolation.shutdown(e.getEnvironment().getRegion().getTableDesc().getNameAsString());
            default:
                validatePut(e.getEnvironment(),put);
        }
        super.prePut(e, put, edit, writeToWAL);
    }

    public static SpliceIndexObserver getObserver(long conglomId){
        return regionReferences.get(conglomId);
    }

    public void addIndex(IndexManager index){
        if(state.get()!=State.RUNNING) return;
        indices.add(index);
    }

    public void dropIndex(IndexManager manager){
        if(state.get()!=State.RUNNING) return;
        indices.remove(manager);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
        setUpObserver(e.getEnvironment());
//        if(!pkConstraints.validate(delete))throw new DoNotRetryIOException("Unable to delete, Foreign Key violation");
//        for(IndexManager indexManager:indices){
//            Delete indexDelete = indexManager.getIndexDelete(delete);
//            if(!indexManager.validate(delete))
//                throw new DoNotRetryIOException("Unable to delete, index constraint violation on index "+ indexManager);
//            indexManager.updateIndex(indexDelete,e.getEnvironment());
//        }

        super.preDelete(e, delete, edit, writeToWAL);
    }

/*******************************************************************************************************************/
    /*private helper methods*/

    private void validatePut(RegionCoprocessorEnvironment e, Put put) throws IOException{
        //we know that we're in a good state to do this, case we catch it in the calling method
        setUpObserver(e);
        // validate constraints, if there are any
        if(localConstraint!=null&&!localConstraint.validate(put, e)){
            SpliceLogUtils.trace(LOG,"Checking put for conglomerate %d",conglomId);
            throw ConstraintViolation.create(localConstraint.getType());
        }

        //update indices
        try{
            for(IndexManager manager:indices){
                manager.updateIndex(put,e);
            }
        }catch(IOException ioe){
            if(!(ioe instanceof RetriesExhaustedWithDetailsException))
                throw ioe;
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)ioe;
            SpliceLogUtils.error(LOG,rewde.getMessage(),rewde);
            /*
             * RetriesExhaustedWithDetailsException wraps out client puts that
             * fail because of an IOException on the other end of the RPC, including
             * Constraint Violations and other DoNotRetry exceptions. Thus,
             * if we find a DoNotRetryIOException somewhere, we unwrap and throw
             * that instead of throwing a normal RetriesExhausted error.
             */
            List<Throwable> errors = rewde.getCauses();
            for(Throwable t:errors){
                if(t instanceof DoNotRetryIOException)
                    throw (DoNotRetryIOException)t;
            }

            throw rewde;
        }
    }

    private void setUpObserver(RegionCoprocessorEnvironment e) throws IOException {
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
            /*
             * One of a few possible scenarios is going on:
             *
             * 1. State = Running => Good to go
             * 2. State = Waiting to startup => can't do anything, because we have to wait for startup to complete
             * 3. State = Failed setup || shutdown => can't do anything, because something very bad happened
             * 4. State = Starting => Someone else is initializing us. Go ahead and step forward into the
             * synchronized block so that you'll wait until it's completed.
             *
             */
            if(state.get()!=State.STARTING) return;
        }

        synchronized (initalizer){
            if(state.get()!=State.STARTING) return;

            //get the conglomerate id

            SpliceLogUtils.debug(LOG,"Setting up index observer for table %d", conglomId);

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
                    state.set(State.RUNNING);
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
            }
            SpliceLogUtils.debug(LOG,"Index setup complete for table "+conglomId+", ready to run");
            state.set(State.RUNNING);
        }
    }

    private IndexManager buildIndex(ConglomerateDescriptor conglomDesc) {
        IndexRowGenerator irg = conglomDesc.getIndexDescriptor();
        IndexDescriptor indexDescriptor = irg.getIndexDescriptor();
        if(indexDescriptor.isUnique()) return IndexManager.uniqueIndex(conglomDesc.getConglomerateNumber(),indexDescriptor);

        return null;   //TODO -sf- deal with other index types
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
        return new PrimaryKey(Long.toString(conglomId),null);
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
