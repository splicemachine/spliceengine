package com.splicemachine.derby.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.mrio.api.MemStoreFlushAwareScanner;
import com.splicemachine.si.api.*;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScan;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class SpliceIndexObserver extends AbstractSpliceIndexObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
    	if (LOG.isTraceEnabled())
    			SpliceLogUtils.trace(LOG, "prePut %s",put);
        if(conglomId>0){
            if(put.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null) return;

            //we can't update an index if the conglomerate id isn't positive--it's probably a temp table or something
            byte[] row = put.getRow();
            List<Cell> data = put.get(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
            KVPair kv;
            if(data!=null&&data.size()>0){
                byte[] value = CellUtil.cloneValue(data.get(0));
                if(put.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null){
                    kv = new KVPair(row,value, KVPair.Type.UPDATE);
                }else
                    kv = new KVPair(row,value);
            }else{
                kv = new KVPair(row, HConstants.EMPTY_BYTE_ARRAY);
            }
            mutate(e.getEnvironment(), kv, operationFactory.fromWrites(put));
        }
        super.prePut(e, put, edit, durability);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete, WALEdit edit, Durability durability) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "preDelete %s",delete);
        if(conglomId>0){
            if(delete.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null){
                KVPair deletePair = KVPair.delete(delete.getRow());
                TxnView txn = operationFactory.fromWrites(delete);
                mutate(e.getEnvironment(), deletePair,txn);
            }
        }
        super.preDelete(e, delete, edit, durability);
    }
    
    

	@Override
	public Reader preStoreFileReaderOpen(
			ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs,
			Path p, FSDataInputStreamWrapper in, long size,
			CacheConfig cacheConf, Reference r, Reader reader)
			throws IOException {
		// TODO Auto-generated method stub
		return super.preStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, reader);
	}
	
	
	@Override
	public KeyValueScanner preStoreScannerOpen(
			ObserverContext<RegionCoprocessorEnvironment> c, Store store,
			Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s)
			throws IOException {
		if (scan.getAttribute("MAP-REDUCE") != null) {
			LOG.debug("MemStoreFlushAwareScanner");
			InternalScan iscan = new InternalScan(scan);
			iscan.checkOnlyMemStore();
		      return new MemStoreFlushAwareScanner(store, store.getScanInfo(), iscan, targetCols,
		    	        ((HStore)store).getHRegion().getReadpoint(IsolationLevel.READ_UNCOMMITTED));		
		} 
		
		return super.preStoreScannerOpen(c, store, scan, targetCols, s);
	}
}
