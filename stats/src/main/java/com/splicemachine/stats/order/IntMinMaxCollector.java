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
