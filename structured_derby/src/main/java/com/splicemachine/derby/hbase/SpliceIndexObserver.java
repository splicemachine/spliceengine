package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.batch.WriteContext;
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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

		private long blockingStoreFiles;

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
						blockingStoreFiles = SpliceConstants.config.getInt("hbase.hstore.blockingStoreFiles",10);
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
            List<KeyValue> data = put.get(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
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
		public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
																								 Store store,
																								 List<? extends KeyValueScanner> scanners,
																								 ScanType scanType,
																								 long earliestPutTs, InternalScanner s) throws IOException {
				if(!isTemp ||(s==null && scanners.size()<=0))
						return super.preCompactScannerOpen(c, store, scanners,scanType,earliestPutTs,s);

				if(blockingStoreFiles<=scanners.size()){
						LOG.info("Falling back to normal HBase compaction for TEMP--consider increasing the number of blocking store files");
						return super.preCompactScannerOpen(c,store,scanners,scanType,earliestPutTs,s);
				}else{
						c.complete();
						return SpliceDriver.driver().getTempTable().getTempCompactionScanner();
				}
		}

		public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,List<StoreFile> candidates) throws IOException {
				filterFiles(c, candidates);
		}

		public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates, CompactionRequest request) throws IOException {
				filterFiles(c, candidates);
		}

		/*******************************************************************************************************************/
    /*private helper methods*/

		private void filterFiles(ObserverContext<RegionCoprocessorEnvironment> c, List<StoreFile> candidates) throws IOException {
				/*
				 * We want to remove TEMP files that we aren't interested in anymore. However, there is always the possibility
				 * that these files can't be removed--that we're always interested in them. Normally, this is fine, we just
				 * don't perform any compaction.
				 *
				 * However, once we exceed the blockingStoreFiles limit of files, then we are in trouble--we need to fall
				 * back on SOME kind of compaction. Therefore, if we have at least as many candidate files as blockingStoreFiles,
				 * AND we are unable to remove any from TEMP exposure, then retain them ALL and fall back to normal compaction.
				 */
				if(!isTemp ||candidates.size()<=0){
						return;
				}
				if(LOG.isTraceEnabled())
						LOG.trace(String.format("Filtering %d StoreFiles",candidates.size()));
				List<StoreFile> copy = Lists.newArrayList(candidates);
				try {
						SpliceDriver.driver().getTempTable().filterCompactionFiles(c.getEnvironment().getConfiguration(), copy);
						if(copy.size()==0){
								/*
								 * We need to keep all the files around. This leaves two situations: when we have exceeded the
								 * blocking store files and when we have not.
								 *
								 * When we have not exceeded blockingStoreFiles, then we can just behave like normal--leave
								 * the files around. This means that we clear out the candidates list and return.
								 *
								 * When we exceed blockingStoreFiles, we keep candidates the same, and we will detect that the
								 * number has exceeded in the preCompactScannerOpen and fall back to the default.
								 */
								if(candidates.size()<blockingStoreFiles){
										//we are free to use the normal TEMP compaction procedure, which does nothing.
										candidates.clear();
								}
								//if the above isn't met, then we do nothing to candidates, because we are falling back to normal TEMP
								if(LOG.isDebugEnabled())
										LOG.debug("Blocking Store files exceeded, falling back to default HBase compaction");
						}else{
								//there are at least some files to remove using the TEMP structure, so just go with it
								candidates.retainAll(copy);
						}

						c.bypass();
						c.complete();
				} catch (ExecutionException e) {
						throw new IOException(e.getCause());
				}
		}


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
