package com.splicemachine.pipeline.callbuffer;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.MergingWriteStats;
import com.splicemachine.utils.SpliceLogUtils;

class RegionServerCallBuffer implements CallBuffer<Pair<byte[],RegionCallBuffer>> {
    private static final Logger LOG = Logger.getLogger(RegionServerCallBuffer.class);
	private ServerName serverName;
    private final Writer writer;
    private NavigableMap<byte[],RegionCallBuffer> buffers;
    private final List<Future<WriteStats>> outstandingRequests = Lists.newArrayList();
    private final MergingWriteStats writeStats;
    private WriteConfiguration writeConfiguration;
    private byte[] tableName;

    public RegionServerCallBuffer(byte[] tableName, WriteConfiguration writeConfiguration, ServerName serverName, Writer writer, final MergingWriteStats writeStats) {
    	this.writeConfiguration = writeConfiguration;
    	this.tableName = tableName;
    	this.writeStats = writeStats;
    	this.serverName = serverName;
    	this.writer = writer;
        this.buffers = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    }

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
    public void addAll(ObjectArrayList<Pair<byte[],RegionCallBuffer>> elements) throws Exception {
    	Object[] elementArray = elements.buffer;
    	int size = elements.size();
    	for (int i = 0; i< size; i++) {
            add((Pair<byte[],RegionCallBuffer>)elementArray[i]);        		
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
    	BulkWrites bulkWrites = new BulkWrites();
		for (Entry<byte[], RegionCallBuffer> regionEntry: this.buffers.entrySet()) {
			if (regionEntry.getValue().getBufferSize() > 0) {
				bulkWrites.addBulkWrite(regionEntry.getValue().getBulkWrite());
				regionEntry.getValue().flushBuffer(); // zero out
			}
		}
		if(LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "flushing %d entries",bulkWrites.numEntries());
		return bulkWrites;    	
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
		if (bulkWrites.getKVPairSize()!=0)
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

	public void setServerName(ServerName serverName) {
		this.serverName = serverName;
	}

	@Override
	public void incrementHeap(long heap) throws Exception {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public void incrementCount(int count) throws Exception {
		throw new RuntimeException("Not Implemented");
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

