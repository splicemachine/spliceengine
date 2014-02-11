package com.splicemachine.derby.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class SpliceIndexObserver extends BaseRegionObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);

    private long conglomId;
		private boolean isTemp;

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
						isTemp = SpliceConstants.TEMP_TABLE.equals(tableName);
            return;
        }

        super.postOpen(e);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
    	if (LOG.isTraceEnabled())
    			SpliceLogUtils.trace(LOG, "prePut %s",put);
        if(conglomId>0){
            if(put.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null) return;

            //we can't update an index if the conglomerate id isn't positive--it's probably a temp table or something
            byte[] row = put.getRow();
            List<KeyValue> data = put.get(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY);
            KVPair kv;
            if(data!=null&&data.size()>0){
                byte[] value = data.get(0).getValue();
                if(put.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null){
                    kv = new KVPair(row,value, KVPair.Type.UPDATE);
                }else
                    kv = new KVPair(row,value);
            }else{
                kv = new KVPair(row, HConstants.EMPTY_BYTE_ARRAY);
            }
            mutate(e.getEnvironment(), kv, SpliceUtils.getTransactionId(put));
        }
        super.prePut(e, put, edit, writeToWAL);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "preDelete %s",delete);
        if(conglomId>0){
            if(delete.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null){
                KVPair deletePair = KVPair.delete(delete.getRow());
                mutate(e.getEnvironment(), deletePair, Bytes.toString(delete.getAttribute("si_delete_put")));
            }
        }
        super.preDelete(e, delete, edit, writeToWAL);
    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Put put, boolean result) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "preCheckAndPut %s",put);
    	return super.preCheckAndPut(e, row, family, qualifier, compareOp, comparator, put, result);    //To change body of overridden methods use File | Settings | File Templates.
    }


		@Override
		public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s) throws IOException {
				if(!isTemp ||(s==null && scanners.size()<=0))
						return super.preCompactScannerOpen(c, store, scanners,scanType,earliestPutTs,s);

				c.complete();
				return SpliceDriver.driver().getTempTable().getTempCompactionScanner();
		}

		
		
		public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,List<StoreFile> candidates) throws IOException {
			if(!isTemp ||candidates.size()<=0){
				return;
			}
			try {
				SpliceDriver.driver().getTempTable().filterCompactionFiles(c.getEnvironment().getConfiguration(),candidates);
				c.bypass();
				c.complete();
			} catch (ExecutionException e) {
				throw new IOException(e.getCause());
			}
		}

		public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates, CompactionRequest request) throws IOException {
				if(!isTemp ||candidates.size()<=0){
						return;
				}
				try {
						SpliceDriver.driver().getTempTable().filterCompactionFiles(c.getEnvironment().getConfiguration(),candidates);
						c.bypass();
						c.complete();
				} catch (ExecutionException e) {
						throw new IOException(e.getCause());
				}
		}

		/*******************************************************************************************************************/
    /*private helper methods*/


    private void mutate(RegionCoprocessorEnvironment rce, KVPair mutation,String txnId) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "mutate %s",mutation);
        //we've already done our write path, so just pass it through
        WriteContext context;
        try{
            context = SpliceIndexEndpoint.factoryMap.get(conglomId).getFirst().createPassThrough(txnId,rce,1);
        }catch(InterruptedException e){
            throw new IOException(e);
        }
        context.sendUpstream(mutation);
        WriteResult mutationResult = context.finish().get(mutation);
        if(mutationResult==null) return; //we didn't actually do anything, so no worries
        switch (mutationResult.getCode()) {
            case FAILED:
                throw new IOException(mutationResult.getErrorMessage());
            case PRIMARY_KEY_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.PRIMARY_KEY, mutationResult.getConstraintContext());
            case UNIQUE_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.UNIQUE, mutationResult.getConstraintContext());
            case FOREIGN_KEY_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.FOREIGN_KEY, mutationResult.getConstraintContext());
            case CHECK_VIOLATION:
                throw ConstraintViolation.create(Constraint.Type.CHECK, mutationResult.getConstraintContext());
            case WRITE_CONFLICT:
                throw new WriteConflict(mutationResult.getErrorMessage());
		case NOT_RUN:
		case SUCCESS:
		default:
			break;
        }
    }
}
