package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;

import com.splicemachine.utils.CounterWithLock;

/**
 *    This scanner works only on data in RS memory
 *    it intercepts MemStore flush events and throws IOException
 *    on subsequent next() call to notify client about this event. 
 *    All subsequent calls to this scanner will return 'false'.
 *    We need this behavior to :
 *    1. notify client-side that one of MemStore in a region was flushed
 *       and client must refresh all file - based scanners (one more HFile now
 *       in HBase file system)
 *    2. to preserve existing behavior in a RegionScanner. When scanner reaches end
 *       it return false on next().   
 *
 */
public class MemStoreFlushAwareScanner extends StoreScanner{
   
   public final static String FLUSH_EVENT = "FLUSH";   	
   protected boolean memstoreFlushed = false;   
   protected CounterWithLock lock ;

	public MemStoreFlushAwareScanner(Store store, ScanInfo scanInfo, Scan scan, 
			final NavigableSet<byte[]> columns, long readPt) throws IOException
	{
		super(store, scanInfo, scan, columns, readPt);
	}

	@Override
	public boolean next(List<Cell> outResult, int limit) throws IOException {
		if(memstoreFlushed){
			memstoreFlushed = false;
			close();
			throw new IOException(FLUSH_EVENT);
		}
		// next time we call after flush, scanner is closed
		// and next return false;
		return super.next(outResult, limit);
	}

	@Override
	public void updateReaders() throws IOException {
	  memstoreFlushed = true; 
	  // Do nothing actually
	  // We do not want the default StoreScanner.updateReaders
	}
	
	public void setReadLockCounter (CounterWithLock lock){
		this.lock = lock;
	}

	@Override
	public void close() {
		if(lock != null){
			// Release region read lock
			lock.decrement();
		}
		super.close();
	}
	
	

}
