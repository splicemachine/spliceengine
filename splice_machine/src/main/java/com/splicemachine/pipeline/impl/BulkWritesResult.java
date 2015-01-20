package com.splicemachine.pipeline.impl;

import java.util.Collection;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWritesResult {
		private Collection<BulkWriteResult> bulkWriteResults;

		public BulkWritesResult(Collection<BulkWriteResult> bulkWriteResults){
				this.bulkWriteResults = bulkWriteResults;
		}

		public Collection<BulkWriteResult> getBulkWriteResults() {
				return bulkWriteResults;
		}

		@Override
		public String toString() {
				StringBuilder sb = new StringBuilder("BulkWritesResult{");
				boolean first = true;
				for (BulkWriteResult result:bulkWriteResults) {
						if(first) first=false;
						else sb.append(",");
						sb.append(result);
				}
				return sb.toString();
		}
}
