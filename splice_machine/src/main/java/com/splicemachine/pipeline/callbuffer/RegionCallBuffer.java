package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.impl.BulkWrite;
import org.apache.hadoop.hbase.HRegionInfo;

import java.util.ArrayList;
import java.util.Collection;

public class RegionCallBuffer implements CallBuffer<KVPair> {
		private Collection<KVPair> buffer;
		private int heapSize;
		private HRegionInfo hregionInfo;
		private PreFlushHook preFlushHook;

		public RegionCallBuffer(HRegionInfo hregionInfo, PreFlushHook preFlushHook) {
				this.hregionInfo = hregionInfo;
				this.buffer = new ArrayList<>();
				this.preFlushHook = preFlushHook;
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
		public void addAll(Iterable<KVPair> elements) throws Exception {
				for (KVPair element:elements) {
						add(element);
				}
		}

		@Override
		public void flushBuffer() throws Exception {
				heapSize = 0;
				buffer.clear(); // Can we do it faster via note on this method and then torch reference later?
		}

		@Override
		public void close() throws Exception {
				buffer = null;
		}

		public boolean isEmpty() {
				return buffer.size()<=0;
		}

		public int getHeapSize() {
				return heapSize;
		}

		public int getBufferSize() { return buffer.size(); }

		public BulkWrite getBulkWrite() throws Exception {
				return new BulkWrite(heapSize,preFlushHook.transform(buffer),hregionInfo.getEncodedName());
		}

		public boolean keyOutsideBuffer(byte[] key) {
				return !this.hregionInfo.containsRow(key);
		}

		public HRegionInfo getHregionInfo() {
				return hregionInfo;
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

		public Collection<KVPair> getBuffer() {
				return buffer;
		}

}

