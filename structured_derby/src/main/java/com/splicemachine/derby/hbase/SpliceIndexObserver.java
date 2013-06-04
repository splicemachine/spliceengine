package com.splicemachine.derby.hbase;

import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.derby.impl.sql.execute.index.WriteContextFactoryPool;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteContextFactory;
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

    private volatile WriteContextFactory<RegionCoprocessorEnvironment> writeContextFactory;
    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        //get the Conglomerate Id. If it's not a table that we can index (e.g. META, ROOT, SYS_TEMP,__TXN_LOG, etc)
        //then don't bother with setting things up
        String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
        final long conglomId;
        try{
            conglomId = Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.debug(LOG, "Unable to parse Conglomerate Id for table %s, indexing is will not be set up", tableName);
            writeContextFactory = WriteContextFactoryPool.getDefaultFactory();
            return;
        }

        try {
            writeContextFactory = WriteContextFactoryPool.getContextFactory(conglomId);
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }
        SpliceDriver.Service service = new SpliceDriver.Service() {
            @Override
            public boolean start() {
                //get the index set now that we know we can
                if(writeContextFactory instanceof LocalWriteContextFactory)
                    ((LocalWriteContextFactory) writeContextFactory).prepare();
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
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
    	SpliceLogUtils.trace(LOG, "postClose");
        try {
            WriteContextFactoryPool.releaseContextFactory((LocalWriteContextFactory) writeContextFactory);
        } catch (Exception e1) {
            SpliceLogUtils.error(LOG,"Unable to close Context factory--beware of memory leaks!");
        }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
    	SpliceLogUtils.trace(LOG, "prePut %s",put);
    	mutate(e.getEnvironment(), put);
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
            context = writeContextFactory.createPassThrough(rce);
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
                throw ConstraintViolation.create(Constraint.Type.PRIMARY_KEY);
            case UNIQUE_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.UNIQUE);
            case FOREIGN_KEY_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.FOREIGN_KEY);
            case CHECK_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.CHECK);
            case WRITE_CONFLICT:
                throw new WriteConflict(mutationResult.getErrorMsg());
		case NOT_RUN:
		case SUCCESS:
		default:
			break;
        }
    }
}
