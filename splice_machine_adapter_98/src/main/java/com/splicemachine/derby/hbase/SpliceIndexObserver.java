package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScan;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.mrio.api.MemStoreFlushAwareScanner;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.CounterWithLock;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Region Observer for managing indices and 
 * some other tasks
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class SpliceIndexObserver extends AbstractSpliceIndexObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);

    // Memory leak
    // TODO add clean up thread
    
    static ConcurrentHashMap<String, CounterWithLock> splitLockMap = 
    		new ConcurrentHashMap<String, CounterWithLock>();
    
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
	  	if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "preStoreScannerOpen %s : %s", store.toString(), scan.toString());
		if (scan.getAttribute("memstore-only") != null) {			
			// We can wait indefinitely  
			addReader(c.getEnvironment().getRegion());
			if(LOG.isDebugEnabled()){
				SpliceLogUtils.debug(LOG, "preStoreScannerOpen in MR mode %s", 
						c.getEnvironment().getRegion() );
			}
			InternalScan iscan = new InternalScan(scan);
			iscan.checkOnlyMemStore();
			
			MemStoreFlushAwareScanner scanner = new MemStoreFlushAwareScanner(store, store.getScanInfo(), iscan, targetCols,
		    	        ((HStore)store).getHRegion().getReadpoint(IsolationLevel.READ_UNCOMMITTED));
			// Set read lock counter (will be decremented on scanner's close)
			scanner.setReadLockCounter(getReadLockCounter(c.getEnvironment().getRegion()));
			// TODO: do we need bypass() & complete()?
			return scanner;
		} 
		
		return super.preStoreScannerOpen(c, store, scan, targetCols, s);
	}
	
    /**
     * Gets the counter with lock for a given region
     * @param region
     * @return  counter
     */
	private static CounterWithLock getReadLockCounter(HRegion region) {
		String regionName = region.getRegionNameAsString();
		return splitLockMap.get(regionName);
	}

	/**
	 * Adds reader (memory store scanner) to a given region.
	 * @param region
	 */
	private static void addReader(HRegion region) {

		String regionName = region.getRegionNameAsString();
		if( splitLockMap.containsKey(regionName) == false){
			splitLockMap.putIfAbsent(regionName, new CounterWithLock());
		}
		CounterWithLock counter = splitLockMap.get(regionName);
		counter.increment();
	}
	/**
	 * Tries locking a region for split
	 * @param region
	 * @return true if successful, false - otherwise
	 */
	private static boolean tryLock(HRegion region) {

		String regionName = region.getRegionNameAsString();
		if( splitLockMap.containsKey(regionName) == false){
			splitLockMap.putIfAbsent(regionName, new CounterWithLock());
		}
		CounterWithLock counter = splitLockMap.get(regionName);
		return counter.tryLock();
	}
	
	/**
	 * Unlocks locked region.
	 * @param region
	 */
	private static void unlock(HRegion region) {

		String regionName = region.getRegionNameAsString();
		CounterWithLock counter = splitLockMap.get(regionName);
		counter.unlock();
	}

	@Override
	public void preSplitBeforePONR(
			ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] splitKey,
			List<Mutation> metaEntries) throws IOException {
	  	if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "preSplitBeforePONR %s ", ctx.getEnvironment().getRegion() );		
		// Check if we have active MemStoreFlushAwareScanner instances on this Region
		// if - yes, then reject/bypass request
		if(tryLock((ctx.getEnvironment().getRegion())) == false) {
			// there are active memory store scanner
			// split is not allowed
			// TODO: add timeout
			ctx.bypass();
			ctx.complete();
			return;
		} else{
			// we got write lock
			// make sure we release it when split is done
			// in postSplit?
		}
		super.preSplitBeforePONR(ctx, splitKey, metaEntries);
	}

	@Override
	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e,
			HRegion l, HRegion r) throws IOException {
	  	if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "postSplit %s ", e.getEnvironment().getRegion() );		
		
		// Split finished, we can enable  memory store scanners
		unlock(e.getEnvironment().getRegion());
		super.postSplit(e, l, r);
	}



	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
			Get get, List<Cell> results) throws IOException {
		
		// get specific attribute if present
		// TODO: make this generic command framework
		if(get.getAttribute("CMD:STOREFILE:LIST") !=null){
		  	if (LOG.isDebugEnabled())
				SpliceLogUtils.debug(LOG, "preGetOp %s ", get );				
			storeFileList(e.getEnvironment(), results);
			e.bypass();
			e.complete();
			return;
		}		
		super.preGetOp(e, get, results);
	}

	/**
	 * Get the list of all store files for 
	 * a given region.
	 * @param env - region coprocessor environment
	 * @param results - result 
	 */
	private void storeFileList(RegionCoprocessorEnvironment env,
			 List<Cell> results ) {
		
		HRegion region = env.getRegion();
		Map<byte[], Store> storeMap = region.getStores();
		List<String> storeFiles = new ArrayList<String>();
		KeyValue retValue = toKeyValue(storeFiles);
		for(Store store: storeMap.values()){
			for(StoreFile sf: store.getStorefiles()){
				storeFiles.add(sf.getPath().toString());
			}
		}
		results.add(retValue);
	}
	
    /**
     * Converts list of String objects
     * into KeyValue instance
     * @param storeFiles
     * @return key value object
     */
	private KeyValue toKeyValue(List<String> list) {
		StringBuffer sb = new StringBuffer();
		for( int i=0; i < list.size(); i++){
			sb.append(list.get(i));
			if( i < list.size() -1){
				sb.append(";");
			}
		}
		// Pack all store file paths into dummy KV and return
		KeyValue kv = new KeyValue("r".getBytes(), 
				"f".getBytes(), "q".getBytes(), 0L, sb.toString().getBytes());
		return kv;
	}
	
	
	
	
}
