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

package com.splicemachine.stats.order;

import com.splicemachine.stats.IntUpdateable;

/**
 * @author Scott Fines
 * Date: 6/2/14
 */
public class IntMinMaxCollector implements MinMaxCollector<Integer>,IntUpdateable {
		private int currentMin;
		private long currentMinCount;
		private int currentMax;
		private long currentMaxCount;

		@Override
		public void update(int item, long count) {
				if(item==currentMin)
						currentMinCount+=count;
				else if(currentMin>item) {
						currentMin = item;
						currentMinCount = count;
				}
				if(item==currentMax)
						currentMaxCount+=count;
				else if(currentMax<item) {
						currentMax = item;
						currentMaxCount = count;
				}
		}

		@Override
		public void update(int item) {
				update(item,1l);

		}

		@Override
		public void update(Integer item) {
				assert item!=null: "Cannot order null elements";
				update(item.intValue());
		}

		@Override
		public void update(Integer item, long count) {
				assert item!=null: "Cannot order null elements!";
				update(item.intValue(),count);
		}

		@Override public Integer minimum() { return currentMin; }
		@Override public Integer maximum() { return currentMax; }
		@Override public long minCount() { return currentMinCount; }
		@Override public long maxCount() { return currentMaxCount; }

		public int max(){ return currentMax; }
		public int min(){ return currentMin; }

		public static IntMinMaxCollector newInstance() {
				IntMinMaxCollector collector = new IntMinMaxCollector();
				collector.currentMin = Integer.MAX_VALUE;
				collector.currentMax = Integer.MIN_VALUE;
				return collector;
		}
}
