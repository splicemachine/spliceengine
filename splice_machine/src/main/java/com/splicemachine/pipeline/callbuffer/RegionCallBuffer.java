package com.splicemachine.pipeline.callbuffer;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.si.api.TxnView;

public class RegionCallBuffer implements CallBuffer<KVPair> {
    private ObjectArrayList<KVPair> buffer;
    private int heapSize;
    private HRegionInfo hregionInfo;
    private RegionServerCallBuffer rsCallBuffer;
    private TxnView txn;
    private PreFlushHook preFlushHook;

    private RegionCallBuffer(HRegionInfo hregionInfo, TxnView txn, PreFlushHook preFlushHook) {
    	this.hregionInfo = hregionInfo;
        this.buffer = ObjectArrayList.newInstance();
        this.txn = txn;
        this.preFlushHook = preFlushHook;
    }
    
    public RegionCallBuffer(RegionServerCallBuffer rsCallBuffer, HRegionInfo hregionInfo, TxnView txn, PreFlushHook preFlushHook) {
    	this(hregionInfo, txn,preFlushHook);
    	this.rsCallBuffer = rsCallBuffer;
    }
    
    protected void swapRegionServerCallBuffer(byte[] key, RegionServerCallBuffer rsCallBuffer) throws Exception {
    	this.rsCallBuffer.incrementHeap(-heapSize);
    	this.rsCallBuffer.incrementCount(buffer.size());
    	rsCallBuffer.incrementHeap(heapSize);
    	rsCallBuffer.incrementCount(buffer.size());
    	this.rsCallBuffer.remove(key);
    	rsCallBuffer.add(Pair.newPair(key, this));
    	this.rsCallBuffer = rsCallBuffer;        	
    }

    @Override
    public void add(KVPair element) throws Exception {
        buffer.add(element);
        heapSize+=element.getSize();
    }

    @Override
    public void addAll(KVPair[] elements) throws Exception {
        for(KVPair element:elements)
            add(element);
    }

    @Override
    public void addAll(ObjectArrayList<KVPair> elements) throws Exception {
    	Object[] elementArray = elements.buffer;
    	int size = elements.size();
    	for (int i = 0; i< size; i++) {
            add((KVPair)elementArray[i]);        		
    	}
    }

    @Override
	public void flushBuffer() throws Exception {
    	heapSize = 0;
    	
    	
    	buffer.clear(); // Can we do it faster via note on this method and then torch reference later?
	}

    @Override
    public void close() throws Exception {
    	buffer.clear();
    	buffer.release();
    	buffer = null;
    }

    public ObjectArrayList<KVPair> removeAll() throws Exception {
    	this.rsCallBuffer.incrementHeap(-this.heapSize);
    	this.rsCallBuffer.remove(this.hregionInfo.getStartKey());
    	return this.buffer;
    }

    public int getHeapSize() {
        return heapSize;
    }

    public int getBufferSize() { return buffer.size(); }

	@Override
	public void incrementHeap(long heap) throws Exception {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public void incrementCount(int count) throws Exception {
		throw new RuntimeException("Not Implemented");			
	}
	
	public BulkWrite getBulkWrite() throws Exception {
		return new BulkWrite(preFlushHook.transform(buffer),hregionInfo.getEncodedName());
	}	
	
	public boolean keyOutsideBuffer(byte[] key) {
		return !this.hregionInfo.containsRow(key);
	}

	public boolean regionBoundsMatch(HRegionInfo hregionInfo) {
		return Bytes.equals(this.hregionInfo.getStartKey(),hregionInfo.getStartKey()) &&
				Bytes.equals(this.hregionInfo.getEndKey(),hregionInfo.getEndKey());
	}

	public boolean regionServerMatch(ServerName serverName) {
		return rsCallBuffer.getServerName().equals(serverName);
	}

	public boolean regionMatches(HRegionInfo hregionInfo, ServerName serverName) {
		return regionServerMatch(serverName) && regionBoundsMatch(hregionInfo);
	}

	public HRegionInfo getHregionInfo() {
		return hregionInfo;
	}

	public void setHregionInfo(HRegionInfo hregionInfo) {
		this.hregionInfo = hregionInfo;
	}

	@Override
	public PreFlushHook getPreFlushHook() {
		return preFlushHook;
	}

	@Override
	public WriteConfiguration getWriteConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public ObjectArrayList<KVPair> getBuffer() {
		return buffer;
	}

	public void setBuffer(ObjectArrayList<KVPair> buffer) {
		this.buffer = buffer;
	}
	
}

