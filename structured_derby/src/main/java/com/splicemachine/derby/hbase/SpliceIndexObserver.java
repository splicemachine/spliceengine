package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.index.*;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class SpliceIndexObserver extends BaseRegionObserver {
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
    private volatile PrimaryKey pkConstraint;
    private volatile CopyOnWriteArrayList<Constraint> fkConstraints;
    private List<IndexManager> indices;
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
        SpliceLogUtils.trace(LOG,"Checking put for conglomerate %d",conglomId);
        // validate Primary Key Constraints, if there are any
        if(pkConstraint!=null&&!pkConstraint.validate(put, e.getRegion()))
            throw ConstraintViolation.duplicatePrimaryKey();
        else if(pkConstraint!=null){
            SpliceLogUtils.trace(LOG,"Put validated, writing through");
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

                //get Constraints list
                ConstraintDescriptorList constraintDescriptors = dataDictionary.getConstraintDescriptors(td);
                List<Constraint> foreignKeys = Lists.newArrayListWithExpectedSize(constraintDescriptors.size());
                List<PrimaryKey> primaryKeys = Lists.newArrayListWithExpectedSize(constraintDescriptors.size());
                List<Constraint> uniqueConstraints = Lists.newArrayListWithExpectedSize(constraintDescriptors.size());
                List<Constraint> checkConstraints = Lists.newArrayListWithExpectedSize(constraintDescriptors.size());

                for(int i=0;i<constraintDescriptors.size();i++){
                    ConstraintDescriptor cDescriptor = constraintDescriptors.elementAt(i);
                    switch(cDescriptor.getConstraintType()){
                        case DataDictionary.FOREIGNKEY_CONSTRAINT:
                            foreignKeys.add(buildForeignKey((ForeignKeyConstraintDescriptor)cDescriptor));
                            break;
                        case DataDictionary.PRIMARYKEY_CONSTRAINT:
                            primaryKeys.add(buildPrimaryKey(cDescriptor));
                            break;
                        case DataDictionary.UNIQUE_CONSTRAINT:
                            uniqueConstraints.add(buildUniqueConstraint(cDescriptor,dataDictionary));
                            break;
                        case DataDictionary.CHECK_CONSTRAINT:
                            checkConstraints.add(buildCheckConstraint(cDescriptor));
                            break;
                        default:
                            throw new DoNotRetryIOException("Unable to determine constraint for type "+ cDescriptor.getConstraintType());
                    }
                }
            /*
             * Make sure that fkConstraints is thread safe so that we can drop the constraint whenever
             * someone asks us to.
             */
                fkConstraints = new CopyOnWriteArrayList<Constraint>(foreignKeys);

            /*
             * There is at most one primary key constraint on any table at any given point in time.
             */
                if(primaryKeys.size()>0)
                    pkConstraint = primaryKeys.get(0);
                //build indices
                IndexLister indexLister = td.getIndexLister();

            }catch(StandardException se){
                SpliceLogUtils.error(LOG,"Unable to set up index management for table "+ conglomId+", aborting",se);
                state.set(State.FAILED_SETUP);
            }
            SpliceLogUtils.debug(LOG,"Index setup complete for table "+conglomId+", ready to run");
            state.set(State.RUNNING);
        }
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

    private UniqueConstraint buildUniqueConstraint(ConstraintDescriptor descriptor,DataDictionary dictionary) throws StandardException{
        ReferencedKeyConstraintDescriptor rkcd = (ReferencedKeyConstraintDescriptor)descriptor;
        ConglomerateDescriptor indexDescriptor = rkcd.getIndexConglomerateDescriptor(dictionary);

        long indexConglomId = indexDescriptor.getConglomerateNumber();

        BitSet checkCols = new BitSet();
        for(int pos: rkcd.getReferencedColumns()){
            checkCols.set(pos);
        }

        return new UniqueConstraint(Long.toString(indexConglomId),checkCols,null);
    }

    private Constraint buildCheckConstraint(ConstraintDescriptor descriptor) throws StandardException{
        //todo -sf- implement!
        return Constraints.noConstraint();
    }
}
