/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.client;

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
