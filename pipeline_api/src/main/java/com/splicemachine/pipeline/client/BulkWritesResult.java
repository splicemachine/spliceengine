/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
