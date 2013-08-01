package com.splicemachine.derby.hbase;

import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class SpliceIndexObserver extends BaseRegionObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);

    private long conglomId;
    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        //get the Conglomerate Id. If it's not a table that we can index (e.g. META, ROOT, SYS_TEMP,__TXN_LOG, etc)
        //then don't bother with setting things up
        String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
        try{
            conglomId = Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.debug(LOG, "Unable to parse Conglomerate Id for table %s, indexing is will not be set up", tableName);
            conglomId=-1;
            return;
        }

        super.postOpen(e);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
    	SpliceLogUtils.trace(LOG, "prePut %s",put);
        if(conglomId>0){
            //we can't update an index if the conglomerate id isn't positive--it's probably a temp table or something
            mutate(e.getEnvironment(), put);
        }
        super.prePut(e, put, edit, writeToWAL);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
    	SpliceLogUtils.trace(LOG, "preDelete %s",delete);
        mutate(e.getEnvironment(), delete);
        super.preDelete(e, delete, edit, writeToWAL);
    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Put put, boolean result) throws IOException {
    	SpliceLogUtils.trace(LOG, "preCheckAndPut %s",put);
    	return super.preCheckAndPut(e, row, family, qualifier, compareOp, comparator, put, result);    //To change body of overridden methods use File | Settings | File Templates.
    }

    /*******************************************************************************************************************/
    /*private helper methods*/


    private void mutate(RegionCoprocessorEnvironment rce, Mutation mutation) throws IOException {
    	SpliceLogUtils.trace(LOG, "mutate %s",mutation);
        //we've already done our write path, so just pass it through
        if(mutation.getAttribute(IndexSet.INDEX_UPDATED)!=null) return;
        WriteContext context;
        try{
            context = SpliceIndexEndpoint.factoryMap.get(conglomId).getFirst().createPassThrough(rce);
        }catch(InterruptedException e){
            throw new IOException(e);
        }
        context.sendUpstream(mutation);
        MutationResult mutationResult = context.finish().get(mutation);
        if(mutationResult==null) return; //we didn't actually do anything, so no worries
        switch (mutationResult.getCode()) {
            case FAILED:
                throw new IOException(mutationResult.getErrorMsg());
            case PRIMARY_KEY_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.PRIMARY_KEY, mutationResult.getConstraintContext());
            case UNIQUE_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.UNIQUE, mutationResult.getConstraintContext());
            case FOREIGN_KEY_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.FOREIGN_KEY, mutationResult.getConstraintContext());
            case CHECK_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.CHECK, mutationResult.getConstraintContext());
            case WRITE_CONFLICT:
                throw new WriteConflict(mutationResult.getErrorMsg());
		case NOT_RUN:
		case SUCCESS:
		default:
			break;
        }
    }
}
