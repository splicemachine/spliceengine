package com.splicemachine.pipeline.callbuffer;

import com.google.common.collect.Lists;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.MergingWriteStats;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;

/**
 * This is a data structure to buffer HBase RPC calls to a specific region server.
 * The call buffers are partitioned (organized) by region.  The region is identified by the starting row key for the region.
 * This means the entries that are buffered by this class are pairs consisting of the starting row key and the call buffer for the region.
 * <em>Please Note:</em> This data structure also contains a table name.  So there will be multiple instances of this class
 * for each region server, since each table and region server combination will have an instance of this class.
 */
class RegionServerCallBuffer implements CallBuffer<Pair<byte[],RegionCallBuffer>> {
		private static final Logger LOG = Logger.getLogger(RegionServerCallBuffer.class);
		private ServerName serverName;
		private final Writer writer;

		/**
		 * Map of all the call buffers for each region on this region server.
		 * The map is keyed by the starting row key for the regions.
		 * And the value (entry) stored in the map is the buffer of calls for the region.
		 * These calls are HBase RPC calls.
		 * Regions are basically defined by their table name and the starting row key for the region.
		 * If you know that information, you can get the call buffer to the region.
		 */
		private NavigableMap<byte[],RegionCallBuffer> buffers;
		private final List<Future<WriteStats>> outstandingRequests = Lists.newArrayList();
		private final MergingWriteStats writeStats;
		private WriteConfiguration writeConfiguration;
		private byte[] tableName;
		private final TxnView txn;

		public RegionServerCallBuffer(byte[] tableName,
																	TxnView txn,
																	WriteConfiguration writeConfiguration,
																	ServerName serverName,
																	Writer writer,
																	final MergingWriteStats writeStats) {
				this.txn = txn;
				this.writeConfiguration = writeConfiguration;
				this.tableName = tableName;
				this.writeStats = writeStats;
				this.serverName = serverName;
				this.writer = writer;
				this.buffers = new TreeMap<>(Bytes.BYTES_COMPARATOR);
		}

	    /**
	     * Add a buffer of region calls to this region server's call buffers.
	     * @param element a pair that consists of the starting row key and the call buffer for the region
	     */
		@Override
		public void add(Pair<byte[],RegionCallBuffer> element) throws Exception {
				SpliceLogUtils.trace(LOG, "add %s", element);
				buffers.put(element.getFirst(), element.getSecond());
		}

		@Override
		public void addAll(Pair<byte[],RegionCallBuffer>[] elements) throws Exception {
				for(Pair<byte[],RegionCallBuffer> element:elements)
						add(element);
		}

		public void remove(byte[] key) throws Exception {
				this.buffers.remove(key);
		}


		@Override
		public void addAll(Iterable<Pair<byte[],RegionCallBuffer>> elements) throws Exception {
				for(Pair<byte[],RegionCallBuffer> element:elements){
						add(element);
				}
		}

		private void flushBufferCheckPrevious() throws Exception {
				Iterator<Future<WriteStats>> futureIterator = outstandingRequests.iterator();
				while(futureIterator.hasNext()){
						Future<WriteStats> future = futureIterator.next();
						if(future.isDone()){
								WriteStats retStats = future.get();//check for errors
								//if it gets this far, it succeeded--strip the reference
								futureIterator.remove();
								writeStats.merge(retStats);
						}
				}
		}

		public BulkWrites getBulkWrites() throws Exception {
				Set<Entry<byte[], RegionCallBuffer>> entries = this.buffers.entrySet();
				Collection<BulkWrite> bws = new ArrayList<>(entries.size());
				for (Entry<byte[], RegionCallBuffer> regionEntry: this.buffers.entrySet()) {
						RegionCallBuffer value = regionEntry.getValue();
						if(value.isEmpty()) continue;
						bws.add(value.getBulkWrite());
						value.flushBuffer(); // zero out
				}
				if(LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "flushing %d entries", bws.size());
				return new BulkWrites(bws,this.txn,this.buffers.lastKey());
		}

		@Override
		public void flushBuffer() throws Exception {
				SpliceLogUtils.trace(LOG, "flushBuffer %s",this.serverName);
				if (writer == null) // No Op Buffer
						return;
				if(buffers.size()<=0)
						return;
				flushBufferCheckPrevious();
				BulkWrites bulkWrites = getBulkWrites();
				if (bulkWrites.numEntries()!=0)
						outstandingRequests.add(writer.write(tableName,bulkWrites, writeConfiguration));
		}

		@Override
		public void close() throws Exception {
				if (writer == null) // No Op Buffer
						return;
				flushBuffer();
				//make sure all outstanding buffers complete before returning
				for(Future<WriteStats> outstandingCall:outstandingRequests){
						WriteStats retStats = outstandingCall.get();//wait for errors and/or completion
						if (LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG, "close returning stats %s",retStats);
						writeStats.merge(retStats);
				}
		}

		public int getHeapSize() {
				int heapSize = 0;
				for(Entry<byte[], RegionCallBuffer> element:buffers.entrySet())
						heapSize+=element.getValue().getHeapSize();
				return heapSize;
		}

		public int getKVPairSize() {
				int size = 0;
				for(Entry<byte[], RegionCallBuffer> element:buffers.entrySet())
						size+=element.getValue().getBufferSize();
				return size;
		}

		public Writer getWriter() {
				return writer;
		}

		public ServerName getServerName() {
				return serverName;
		}

		@Override
		public PreFlushHook getPreFlushHook() {
				return null;
		}

		@Override
		public WriteConfiguration getWriteConfiguration() {
				return writeConfiguration;
		}
}
