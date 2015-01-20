package com.splicemachine.pipeline.impl;

import com.splicemachine.si.api.TxnView;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * Extension of BulkWrites to wrap for a region server.
 *
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrites {
		private Collection<BulkWrite> bulkWrites;
		private TxnView txn;
		/*
		 *a key to indicate which region to send the write to. This is really just
		 * any region which is present on the destination region server
		 */
		private transient byte[] regionKey;

		public BulkWrites() {
				bulkWrites = new ArrayList<>(0);
		}

		public BulkWrites(Collection<BulkWrite> bulkWrites,TxnView txn) {
			this(bulkWrites,txn,null);
		}

		public BulkWrites(Collection<BulkWrite> bulkWrites,TxnView txn,byte[] regionKey) {
				this.bulkWrites = bulkWrites;
				this.txn = txn;
				this.regionKey = regionKey;
		}

		public byte[] getRegionKey() {
				return regionKey;
		}

		public Collection<BulkWrite> getBulkWrites() {
				return bulkWrites;
		}

		public TxnView getTxn() {
				return txn;
		}

		public int getBufferHeapSize() {
				int size = 0;
				for(BulkWrite bw:bulkWrites){
						size+=bw.getBufferSize();
				}
				return size;
		}

		/**
		 * @return the number of rows in the bulk write
		 */
		public int numEntries() {
				int size = 0;
				for(BulkWrite bw:bulkWrites){
						size+=bw.getSize();
				}
				return size;
		}

		/**
		 * @return the number of regions in this write
		 */
		public int numRegions(){
				return bulkWrites.size();
		}


		@Override
		public String toString() {
				StringBuilder sb = new StringBuilder();
				sb.append("BulkWrites{");
				boolean first = true;
				for(BulkWrite bw:bulkWrites){
						if(first) first=false;
						else sb.append(",");
						sb.append(bw);
				}
				sb.append("}");
				return sb.toString();
		}

		@Override
		public boolean equals(Object o) {
				if (this == o) return true;
				if (o == null || getClass() != o.getClass()) return false;

				BulkWrites that = (BulkWrites) o;

				return bulkWrites.equals(that.bulkWrites);

		}

		@Override
		public int hashCode() {
				return bulkWrites.hashCode();
		}

}
